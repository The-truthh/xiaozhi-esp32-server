"""Role affect outbox/state on the existing managed-memory SQLite connection.

An event sequence is allocated before generation, not when a slow observer ends.
Role state never enters the factual ledger or PowerMem vector index.
"""
from contextlib import contextmanager
import hashlib
import json
import time


class CompanionStore:
    def __init__(self, ledger, initialize_affect=True):
        self.ledger = ledger
        if not initialize_affect:
            return
        with ledger.db() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS companion_affect_events(
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE NOT NULL,
                    scope TEXT NOT NULL, companion_id TEXT NOT NULL, mode TEXT NOT NULL,
                    policy TEXT NOT NULL, epoch INTEGER NOT NULL, session_id TEXT NOT NULL,
                    source_turn TEXT NOT NULL, observed_at REAL NOT NULL, deadline REAL NOT NULL,
                    status TEXT NOT NULL, payload TEXT NOT NULL DEFAULT '{}',
                    expected_revision INTEGER, error_class TEXT);
                CREATE INDEX IF NOT EXISTS affect_ready ON companion_affect_events(scope,companion_id,mode,status,seq);
                CREATE TABLE IF NOT EXISTS companion_affect(
                    scope TEXT NOT NULL, companion_id TEXT NOT NULL, mode TEXT NOT NULL,
                    epoch INTEGER NOT NULL, policy TEXT NOT NULL, revision INTEGER NOT NULL,
                    last_seq INTEGER NOT NULL, source_event TEXT NOT NULL, source_turn TEXT NOT NULL,
                    observed_at REAL NOT NULL, expires_at REAL NOT NULL, state TEXT NOT NULL,
                    PRIMARY KEY(scope,companion_id,mode));
            ''')

    @contextmanager
    def db(self):
        with self.ledger.db() as db:
            # This optional feature yields promptly to factual-ledger writers.
            db.execute('PRAGMA busy_timeout=50')
            yield db

    def reserve(self, scope, companion_id, mode, policy, session_id, turn, ttl, capacity, stamp=None):
        stamp = time.time() if stamp is None else stamp
        identifier = hashlib.sha256(json.dumps([scope, companion_id, mode, session_id, turn]).encode()).hexdigest()
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            epoch = self.ledger._epoch(db, scope)
            db.execute("UPDATE companion_affect_events SET status='expired',payload='{}' "
                       "WHERE deadline<? AND status IN ('reserved','queued','processing')", (stamp,))
            old = db.execute('SELECT * FROM companion_affect_events WHERE id=?', (identifier,)).fetchone()
            if old:
                return dict(old) if old['epoch'] == epoch and old['status'] == 'reserved' else None
            count = db.execute("SELECT COUNT(*) FROM companion_affect_events WHERE scope=? AND companion_id=? "
                               "AND status IN ('reserved','queued','processing')", (scope, companion_id)).fetchone()[0]
            if count >= capacity:
                return None
            db.execute("INSERT INTO companion_affect_events(id,scope,companion_id,mode,policy,epoch,session_id,"
                       "source_turn,observed_at,deadline,status) VALUES(?,?,?,?,?,?,?,?,?,?,'reserved')",
                       (identifier, scope, companion_id, mode, policy, epoch, session_id, turn, stamp, stamp + ttl))
            # Keep completed metadata bounded; payloads are cleared on completion.
            db.execute("DELETE FROM companion_affect_events WHERE scope=? AND status NOT IN ('reserved','queued','processing') "
                       "AND seq NOT IN (SELECT seq FROM companion_affect_events WHERE scope=? ORDER BY seq DESC LIMIT 512)",
                       (scope, scope))
            return dict(db.execute('SELECT * FROM companion_affect_events WHERE id=?', (identifier,)).fetchone())

    def enqueue(self, ticket, payload):
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            if self.ledger._epoch(db, ticket['scope']) != ticket['epoch']:
                return False
            cursor = db.execute("UPDATE companion_affect_events SET status='queued',payload=? "
                                "WHERE id=? AND status='reserved' AND deadline>=?",
                                (json.dumps(payload, ensure_ascii=False), ticket['id'], time.time()))
            return cursor.rowcount == 1

    def read(self, scope, companion_id, mode, policy):
        with self.db() as db:
            db.execute('BEGIN')
            epoch = db.execute('SELECT epoch FROM scopes WHERE scope=?', (scope,)).fetchone()
            row = db.execute('SELECT * FROM companion_affect WHERE scope=? AND companion_id=? AND mode=?',
                             (scope, companion_id, mode)).fetchone()
            if not row or not epoch or row['epoch'] != epoch[0] or row['policy'] != policy:
                return None
            if row['expires_at'] <= time.time():
                db.execute('DELETE FROM companion_affect WHERE scope=? AND companion_id=? AND mode=? AND expires_at<=?',
                           (scope, companion_id, mode, time.time()))
                return None
            value = dict(row)
            value['state'] = json.loads(value['state'])
            return value

    def claim(self, scope, companion_id, mode, policy):
        stamp = time.time()
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('DELETE FROM companion_affect WHERE scope=? AND companion_id=? AND mode=? AND expires_at<=?',
                       (scope, companion_id, mode, stamp))
            db.execute("UPDATE companion_affect_events SET status='expired',payload='{}' "
                       "WHERE deadline<? AND status IN ('reserved','queued','processing')", (stamp,))
            # A second worker cannot observe the same subject concurrently.
            if db.execute("SELECT 1 FROM companion_affect_events WHERE scope=? AND companion_id=? AND mode=? "
                          "AND status='processing' LIMIT 1", (scope, companion_id, mode)).fetchone():
                return None
            epoch = self.ledger._epoch(db, scope)
            row = db.execute("SELECT * FROM companion_affect_events WHERE scope=? AND companion_id=? AND mode=? "
                             "AND policy=? AND epoch=? AND status='queued' ORDER BY seq LIMIT 1",
                             (scope, companion_id, mode, policy, epoch)).fetchone()
            if not row:
                return None
            current = db.execute('SELECT revision FROM companion_affect WHERE scope=? AND companion_id=? AND mode=?',
                                 (scope, companion_id, mode)).fetchone()
            revision = current[0] if current else 0
            db.execute("UPDATE companion_affect_events SET status='processing',expected_revision=? WHERE id=?",
                       (revision, row['id']))
            result = dict(row)
            result['expected_revision'] = revision
            result['payload'] = json.loads(result['payload'])
            return result

    def apply(self, job, state, expires_at):
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            event = db.execute('SELECT status,deadline FROM companion_affect_events WHERE id=?', (job['id'],)).fetchone()
            if not event or event['status'] != 'processing':
                return False
            old = db.execute('SELECT * FROM companion_affect WHERE scope=? AND companion_id=? AND mode=?',
                             (job['scope'], job['companion_id'], job['mode'])).fetchone()
            valid = (event['deadline'] >= time.time() and self.ledger._epoch(db, job['scope']) == job['epoch']
                     and (old['revision'] if old else 0) == job['expected_revision']
                     and (not old or old['last_seq'] < job['seq']))
            if not valid:
                db.execute("UPDATE companion_affect_events SET status='stale',payload='{}' WHERE id=?", (job['id'],))
                return False
            db.execute('INSERT INTO companion_affect VALUES(?,?,?,?,?,?,?,?,?,?,?,?) '
                       'ON CONFLICT(scope,companion_id,mode) DO UPDATE SET epoch=excluded.epoch,policy=excluded.policy,'
                       'revision=excluded.revision,last_seq=excluded.last_seq,source_event=excluded.source_event,'
                       'source_turn=excluded.source_turn,observed_at=excluded.observed_at,expires_at=excluded.expires_at,state=excluded.state',
                       (job['scope'], job['companion_id'], job['mode'], job['epoch'], job['policy'], job['expected_revision'] + 1,
                        job['seq'], job['id'], state.get('source_turn', job['source_turn']), state['observed_at'], expires_at,
                        json.dumps(state, ensure_ascii=False)))
            db.execute("UPDATE companion_affect_events SET status='applied',payload='{}' WHERE id=?", (job['id'],))
            return True

    def fail(self, identifier, error_class):
        with self.db() as db:
            db.execute("UPDATE companion_affect_events SET status='failed',payload='{}',error_class=? "
                       "WHERE id=? AND status='processing'", (error_class, identifier))

    def cancel(self, identifier, withdraw=False):
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute("UPDATE companion_affect_events SET status='cancelled',payload='{}' "
                       "WHERE id=? AND status IN ('reserved','queued','processing')", (identifier,))
            if withdraw:
                db.execute('DELETE FROM companion_affect WHERE source_event=?', (identifier,))


class EpisodeStore(CompanionStore):
    """Durable sourced episodes and an explicitly separate extraction outbox."""

    def __init__(self, ledger):
        super().__init__(ledger, initialize_affect=False)
        with self.db() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS companion_episode_sources(
                    id TEXT PRIMARY KEY, scope TEXT NOT NULL, companion_id TEXT NOT NULL,
                    epoch INTEGER NOT NULL, session_id TEXT NOT NULL, source_turn TEXT NOT NULL,
                    observed_at REAL NOT NULL, user_text TEXT NOT NULL, assistant_text TEXT NOT NULL DEFAULT '',
                    delivery_status TEXT NOT NULL DEFAULT 'unanswered', semantic_revision INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS companion_episode_jobs(
                    seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE NOT NULL, source_id TEXT NOT NULL,
                    scope TEXT NOT NULL, companion_id TEXT NOT NULL, epoch INTEGER NOT NULL, policy TEXT NOT NULL,
                    status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, due REAL NOT NULL DEFAULT 0,
                    deadline REAL NOT NULL, error_class TEXT);
                CREATE INDEX IF NOT EXISTS episode_ready ON companion_episode_jobs(scope,companion_id,status,due,seq);
                CREATE TABLE IF NOT EXISTS companion_episodes(
                    id TEXT PRIMARY KEY, scope TEXT NOT NULL, companion_id TEXT NOT NULL, epoch INTEGER NOT NULL,
                    source_id TEXT NOT NULL UNIQUE, source_seq INTEGER NOT NULL, observed_at REAL NOT NULL,
                    expires_at REAL NOT NULL, status TEXT NOT NULL, revision INTEGER NOT NULL, payload TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS episode_scope ON companion_episodes(scope,companion_id,epoch,status,observed_at);
                CREATE TABLE IF NOT EXISTS companion_episode_audit(
                    id INTEGER PRIMARY KEY AUTOINCREMENT, scope TEXT NOT NULL, companion_id TEXT NOT NULL,
                    episode_id TEXT NOT NULL, action TEXT NOT NULL, revision INTEGER NOT NULL,
                    observed_at REAL NOT NULL, note TEXT NOT NULL DEFAULT '');
            ''')

    def reserve_episode(self, scope, companion, policy, session, turn, user_text, ttl=60, capacity=32):
        stamp = time.time()
        identifier = hashlib.sha256(json.dumps([scope, companion, session, turn]).encode()).hexdigest()
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            epoch = self.ledger._epoch(db, scope)
            identifier = hashlib.sha256((identifier + ':' + str(epoch)).encode()).hexdigest()
            self._expire(db, scope, companion, stamp)
            old = db.execute('SELECT * FROM companion_episode_jobs WHERE id=?', (identifier,)).fetchone()
            if old:
                return dict(old) if old['epoch'] == epoch and old['status'] == 'reserved' else None
            count = db.execute("SELECT COUNT(*) FROM companion_episode_jobs WHERE scope=? AND companion_id=? "
                               "AND status IN ('reserved','queued','processing')", (scope, companion)).fetchone()[0]
            if count >= capacity:
                return None
            db.execute('INSERT INTO companion_episode_sources(id,scope,companion_id,epoch,session_id,source_turn,observed_at,user_text) '
                       'VALUES(?,?,?,?,?,?,?,?)', (identifier, scope, companion, epoch, session, turn, stamp, user_text))
            db.execute("INSERT INTO companion_episode_jobs(id,source_id,scope,companion_id,epoch,policy,status,deadline) "
                       "VALUES(?,?,?,?,?,?,'reserved',?)", (identifier, identifier, scope, companion, epoch, policy, stamp + ttl))
            return dict(db.execute('SELECT * FROM companion_episode_jobs WHERE id=?', (identifier,)).fetchone())

    @staticmethod
    def _expire(db, scope, companion, stamp):
        db.execute("UPDATE companion_episode_jobs SET status='expired' WHERE scope=? AND companion_id=? "
                   "AND deadline<? AND status IN ('reserved','queued','processing')", (scope, companion, stamp))
        db.execute('DELETE FROM companion_episodes WHERE scope=? AND companion_id=? AND expires_at<=?', (scope, companion, stamp))
        db.execute("DELETE FROM companion_episode_sources WHERE scope=? AND companion_id=? "
                   "AND id NOT IN (SELECT source_id FROM companion_episodes) AND id NOT IN "
                   "(SELECT source_id FROM companion_episode_jobs WHERE status IN ('reserved','queued','processing'))",
                   (scope, companion))
        db.execute('DELETE FROM companion_episode_audit WHERE scope=? AND companion_id=? AND episode_id NOT IN '
                   '(SELECT id FROM companion_episodes)', (scope, companion))
        db.execute("DELETE FROM companion_episode_jobs WHERE scope=? AND companion_id=? AND status NOT IN ('reserved','queued','processing') "
                   "AND seq NOT IN (SELECT seq FROM companion_episode_jobs WHERE scope=? AND companion_id=? ORDER BY seq DESC LIMIT 512)",
                   (scope, companion, scope, companion))

    def finish_episode(self, ticket, assistant='', status='generated'):
        if status not in ('generated', 'failed', 'interrupted', 'unanswered'):
            raise ValueError('Invalid episode source status')
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            if self.ledger._epoch(db, ticket['scope']) != ticket['epoch']:
                return False
            row = db.execute('SELECT * FROM companion_episode_sources WHERE id=?', (ticket['source_id'],)).fetchone()
            if not row:
                return False
            if row['delivery_status'] == 'unanswered':
                db.execute('UPDATE companion_episode_sources SET assistant_text=?,delivery_status=?,semantic_revision=semantic_revision+1 WHERE id=?',
                           (assistant, status, ticket['source_id']))
            cursor = db.execute("UPDATE companion_episode_jobs SET status='queued' WHERE id=? AND status='reserved' AND deadline>=?",
                                (ticket['id'], time.time()))
            return cursor.rowcount == 1

    def episode_delivery(self, ticket, status):
        if status not in ('audio_sent', 'interrupted'):
            raise ValueError('Invalid episode delivery status')
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            if self.ledger._epoch(db, ticket['scope']) != ticket['epoch']:
                return False
            if status == 'interrupted':
                db.execute("UPDATE companion_episode_sources SET delivery_status='interrupted',semantic_revision=semantic_revision+1 "
                           "WHERE id=? AND delivery_status<>'interrupted'", (ticket['source_id'],))
                db.execute("UPDATE companion_episode_jobs SET status='queued' WHERE id=? AND status='reserved'", (ticket['id'],))
            else:
                db.execute("UPDATE companion_episode_sources SET delivery_status='audio_sent' WHERE id=? AND delivery_status='generated'",
                           (ticket['source_id'],))
            return True

    def claim_episode(self, scope, companion, policy):
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            self._expire(db, scope, companion, time.time())
            epoch = self.ledger._epoch(db, scope)
            if db.execute("SELECT 1 FROM companion_episode_jobs WHERE scope=? AND companion_id=? AND status='processing'", (scope, companion)).fetchone():
                return None
            row = db.execute("SELECT * FROM companion_episode_jobs WHERE scope=? AND companion_id=? AND epoch=? AND policy=? "
                             "AND status='queued' AND due<=? ORDER BY seq LIMIT 1", (scope, companion, epoch, policy, time.time())).fetchone()
            if not row:
                return None
            db.execute("UPDATE companion_episode_jobs SET status='processing',attempts=attempts+1 WHERE id=?", (row['id'],))
            return dict(row, attempts=row['attempts'] + 1)

    def episode_source(self, ticket):
        with self.db() as db:
            row = db.execute('SELECT s.* FROM companion_episode_sources s JOIN scopes p ON p.scope=s.scope AND p.epoch=s.epoch '
                             'WHERE s.id=? AND s.epoch=?', (ticket['source_id'], ticket['epoch'])).fetchone()
            return dict(row) if row else None

    def newer_episode_sources(self, job):
        with self.db() as db:
            return [dict(r) for r in db.execute('SELECT s.user_text FROM companion_episode_sources s '
                'JOIN companion_episode_jobs j ON j.source_id=s.id WHERE j.scope=? AND j.companion_id=? '
                'AND j.epoch=? AND j.seq>? ORDER BY j.seq DESC LIMIT 16',
                (job['scope'],job['companion_id'],job['epoch'],job['seq']))]

    def stale_episode(self, job):
        with self.db() as db:
            db.execute("UPDATE companion_episode_jobs SET status='stale' WHERE id=? AND epoch=? AND status='processing'",
                       (job['id'],job['epoch']))

    def cancel_episode(self, ticket):
        with self.db() as db:
            db.execute("UPDATE companion_episode_jobs SET status='cancelled' WHERE id=? AND epoch=? "
                       "AND status IN ('reserved','queued','processing')",(ticket['id'],ticket['epoch']))

    def episode_records(self, scope, companion, limit=256, include_withdrawn=False):
        with self.db() as db:
            db.execute('BEGIN')
            row = db.execute('SELECT epoch FROM scopes WHERE scope=?', (scope,)).fetchone()
            epoch = row[0] if row else 0
            rows = db.execute('SELECT e.*,s.user_text,s.assistant_text,s.delivery_status,s.semantic_revision,s.session_id,s.source_turn '
                              'FROM companion_episodes e LEFT JOIN companion_episode_sources s ON s.id=e.source_id '
                              'WHERE e.scope=? AND e.companion_id=? AND e.epoch=? AND e.expires_at>? '
                              + ('' if include_withdrawn else "AND e.status IN ('active','resolved') ")
                              + 'ORDER BY e.source_seq DESC LIMIT ?', (scope, companion, epoch, time.time(), max(1, min(512, limit)))).fetchall()
            result=[]
            for row in rows:
                value=dict(row);value['payload']=json.loads(value['payload']);result.append(value)
            return epoch, result

    def episodes_current(self, scope, companion, token):
        with self.db() as db:
            row = db.execute('SELECT epoch FROM scopes WHERE scope=?', (scope,)).fetchone()
            if (row[0] if row else 0) != token[0]:
                return False
            for identifier, revision, source_revision in token[1]:
                found = db.execute("SELECT e.revision,s.semantic_revision FROM companion_episodes e JOIN companion_episode_sources s ON s.id=e.source_id "
                                   "WHERE e.id=? AND e.scope=? AND e.companion_id=? AND e.status IN ('active','resolved') AND e.expires_at>?",
                                   (identifier, scope, companion, time.time())).fetchone()
                if not found or tuple(found) != (revision, source_revision):
                    return False
            return True

    def apply_episode(self, job, result, references, retention_days):
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            current = db.execute('SELECT status,deadline FROM companion_episode_jobs WHERE id=?', (job['id'],)).fetchone()
            source = db.execute('SELECT * FROM companion_episode_sources WHERE id=?', (job['source_id'],)).fetchone()
            valid = (current and current['status']=='processing' and current['deadline']>=time.time() and source
                     and self.ledger._epoch(db,job['scope'])==job['epoch'])
            targets={}
            for ref in references:
                row=db.execute('SELECT * FROM companion_episodes WHERE id=? AND scope=? AND companion_id=?',
                               (ref['id'],job['scope'],job['companion_id'])).fetchone()
                if not row or row['revision']!=ref['revision'] or row['status'] not in ('active','resolved'):
                    valid=False
                else:
                    original=db.execute('SELECT semantic_revision FROM companion_episode_sources WHERE id=?',(row['source_id'],)).fetchone()
                    if not original or original[0]!=ref['source_revision']:valid=False
                    targets[row['id']]=row
            if not valid:
                if current:db.execute("UPDATE companion_episode_jobs SET status='stale' WHERE id=?",(job['id'],))
                return False
            if not result['keep']:
                db.execute("UPDATE companion_episode_jobs SET status='skipped' WHERE id=?",(job['id'],))
                db.execute('DELETE FROM companion_episode_sources WHERE id=?',(job['source_id'],))
                return True
            for key,status in (('supersedes','superseded'),('resolves','resolved')):
                for identifier in result[key]:
                    target=targets.get(identifier)
                    if not target or target['source_seq']>=job['seq']:
                        raise ValueError('Invalid episode target sequence')
                    db.execute('UPDATE companion_episodes SET status=?,revision=revision+1 WHERE id=?',(status,identifier))
                    db.execute('INSERT INTO companion_episode_audit(scope,companion_id,episode_id,action,revision,observed_at) VALUES(?,?,?,?,?,?)',
                               (job['scope'],job['companion_id'],identifier,status,target['revision']+1,time.time()))
            identifier=hashlib.sha256((job['source_id']+':episode').encode()).hexdigest()
            payload=dict(result)
            if source['delivery_status'] in ('interrupted','failed','unanswered'):
                payload.update(role_reaction='',interpretation='')
            db.execute('INSERT OR IGNORE INTO companion_episodes VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                       (identifier,job['scope'],job['companion_id'],job['epoch'],job['source_id'],job['seq'],source['observed_at'],
                        source['observed_at']+retention_days*86400,'active',1,json.dumps(payload,ensure_ascii=False)))
            db.execute("UPDATE companion_episode_jobs SET status='done' WHERE id=?",(job['id'],))
            return True

    def fail_episode(self, job, error_class):
        with self.db() as db:
            db.execute("UPDATE companion_episode_jobs SET status=?,error_class=?,due=? WHERE id=? AND status='processing'",
                       ('queued' if job['attempts']<2 else 'failed',error_class,time.time()+1,job['id']))

    def maintain_episode(self, scope, companion, identifier, revision, changes=None, delete=False, note=''):
        allowed={'topic','summary','role_reaction','interpretation','unfinished'}
        if changes is not None and (not isinstance(changes,dict) or set(changes)-allowed):
            raise ValueError('Cannot alter episode provenance')
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT e.* FROM companion_episodes e JOIN scopes s ON s.scope=e.scope AND s.epoch=e.epoch '
                           'WHERE e.id=? AND e.scope=? AND e.companion_id=?',(identifier,scope,companion)).fetchone()
            if not row or row['revision']!=revision or row['status']=='deleted':
                return False
            payload=json.loads(row['payload']);payload.update(changes or {})
            if any(not isinstance(v,str) or len(v)>500 for k,v in (changes or {}).items() if k!='unfinished'):
                raise ValueError('Invalid episode edit')
            if 'unfinished' in (changes or {}) and type(changes['unfinished']) is not bool:
                raise ValueError('Invalid unfinished status')
            if delete:
                payload={};db.execute('DELETE FROM companion_episode_sources WHERE id=?',(row['source_id'],))
                db.execute('DELETE FROM companion_episode_audit WHERE episode_id=?',(identifier,))
            db.execute('UPDATE companion_episodes SET payload=?,status=?,revision=revision+1 WHERE id=?',
                       (json.dumps(payload,ensure_ascii=False),'deleted' if delete else row['status'],identifier))
            db.execute('INSERT INTO companion_episode_audit(scope,companion_id,episode_id,action,revision,observed_at,note) VALUES(?,?,?,?,?,?,?)',
                       (scope,companion,identifier,'delete' if delete else 'edit',revision+1,time.time(),str(note)[:160] if not delete else ''))
            return True


class TopicStore(CompanionStore):
    """Small scoped conversational state, never a second fact ledger."""
    def __init__(self,ledger):
        super().__init__(ledger,initialize_affect=False)
        with self.db() as db:
            db.executescript('''CREATE TABLE IF NOT EXISTS companion_topics(
                id TEXT PRIMARY KEY,scope TEXT NOT NULL,companion_id TEXT NOT NULL,
                epoch INTEGER NOT NULL,topic TEXT NOT NULL,pending_point TEXT NOT NULL,
                user_preference TEXT NOT NULL DEFAULT '{}',source_refs TEXT NOT NULL DEFAULT '[]',
                session_id TEXT NOT NULL,source_turn TEXT NOT NULL,last_mentioned REAL NOT NULL,
                status TEXT NOT NULL,expires_at REAL NOT NULL,revision INTEGER NOT NULL,
                is_current INTEGER NOT NULL DEFAULT 0,seen_turns TEXT NOT NULL DEFAULT '[]',
                source_episode TEXT,episode_revision INTEGER);
                CREATE INDEX IF NOT EXISTS topics_subject ON companion_topics(scope,companion_id,epoch,last_mentioned);''')

    def catalog(self,scope,companion,limit=5):
        with self.db() as db:
            epoch=self.ledger._epoch(db,scope)
            rows=db.execute('SELECT * FROM companion_topics WHERE scope=? AND companion_id=? AND epoch=? '
                'AND expires_at>? ORDER BY is_current DESC,last_mentioned DESC LIMIT ?',
                (scope,companion,epoch,time.time(),limit)).fetchall()
            return epoch,[dict(r) for r in rows]

    def apply_topic(self,scope,companion,epoch,session,turn,update,ttl):
        """Apply only current user evidence; same turn retry cannot reactivate a topic."""
        stamp=time.time();event=session+':'+turn
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            if self.ledger._epoch(db,scope)!=epoch:return False
            db.execute("UPDATE companion_topics SET status='expired',is_current=0,revision=revision+1 "
                "WHERE scope=? AND companion_id=? AND expires_at<=? AND status!='expired'",(scope,companion,stamp))
            rows=[dict(r) for r in db.execute('SELECT * FROM companion_topics WHERE scope=? AND companion_id=? AND epoch=? AND expires_at>?',
                (scope,companion,epoch,stamp))]
            if any(event in json.loads(r['seen_turns']) for r in rows):return True
            action=update['topic_action'];target=update['topic_id']
            row=next((r for r in rows if r['id']==target),None) if target else next((r for r in rows if r['is_current']),None)
            if target and row is None:return False
            if action=='none' and not update['preferences']:return True
            if action=='new' or row is None:
                identifier=hashlib.sha256(json.dumps([scope,companion,epoch,event]).encode()).hexdigest()
                row=dict(id=identifier,scope=scope,companion_id=companion,epoch=epoch,
                    topic=update['topic_label'] or '此刻的交流',pending_point=update['pending_point'],
                    user_preference='{}',source_refs='[]',session_id=session,source_turn=turn,
                    last_mentioned=stamp,status='pending',expires_at=stamp+ttl,revision=0,is_current=1,seen_turns='[]',
                    source_episode=None,episode_revision=None)
            if action in ('new','continue') or not rows:
                db.execute('UPDATE companion_topics SET is_current=0 WHERE scope=? AND companion_id=? AND is_current=1 AND id!=?',
                    (scope,companion,row['id']))
                row['is_current']=1
            if action=='continue':row['status']='pending'
            if action=='continue' and update['topic_label'] and update['evidence_quote']:
                row['topic']=update['topic_label']
            if action in ('decline','resolve'):
                row['status']='declined' if action=='decline' else 'resolved'
                row['pending_point']='';row['user_preference']='{}'
            if update['pending_point'] and action not in ('decline','resolve'):row['pending_point']=update['pending_point']
            preferences=json.loads(row['user_preference'])
            for p in update['preferences']:
                preferences[p['key']]=dict(value=p['value'],quote=p['quote'],source_ref=event+':user')
            row['user_preference']=json.dumps(preferences,ensure_ascii=False)
            refs=json.loads(row['source_refs'])
            if update['evidence_quote']:refs.append(dict(ref=event+':user',quote=update['evidence_quote']))
            refs.extend(dict(ref=event+':user',quote=p['quote']) for p in update['preferences'])
            row.update(source_refs=json.dumps(refs[-8:],ensure_ascii=False),source_turn=turn,session_id=session,
                last_mentioned=stamp,expires_at=stamp+ttl,revision=row['revision']+1,
                seen_turns=json.dumps((json.loads(row['seen_turns'])+[event])[-32:]))
            columns=list(row)
            db.execute('INSERT OR REPLACE INTO companion_topics('+','.join(columns)+') VALUES('+','.join('?' for _ in columns)+')',tuple(row[k] for k in columns))
            # Only small topical state survives. Prune old, non-current rows.
            db.execute('DELETE FROM companion_topics WHERE scope=? AND companion_id=? AND is_current=0 AND '
                '(expires_at<=? OR id NOT IN (SELECT id FROM companion_topics WHERE scope=? AND companion_id=? ORDER BY last_mentioned DESC LIMIT 64))',
                (scope,companion,stamp,scope,companion))
            return True

    def sync_episode_topics(self,scope,companion,epoch,records,ttl):
        """Only already retrieved unfinished Q3 episodes, with original revisions."""
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            if self.ledger._epoch(db,scope)!=epoch:return
            for r in records:
                if not r.get('unfinished') or r.get('episode_status')!='active':continue
                identifier='episode:'+r['id']
                old=db.execute('SELECT revision,status,episode_revision FROM companion_topics WHERE id=? AND scope=? AND companion_id=?',
                    (identifier,scope,companion)).fetchone()
                if old and (old['status']=='declined' or old['episode_revision']==r['revision']):continue
                # Q3 is optional. Never manufacture its table just to sync a topic.
                valid=db.execute('SELECT revision FROM companion_episodes WHERE id=? AND scope=? AND companion_id=? AND epoch=? AND status=?',
                    (r['id'],scope,companion,epoch,'active')).fetchone()
                if not valid or valid[0]!=r['revision']:continue
                db.execute('INSERT OR REPLACE INTO companion_topics VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (identifier,scope,companion,epoch,r['topic'],r['event_summary'],'{}',
                     json.dumps([r['user_quote']],ensure_ascii=False),r['source_session'],r['source_turn'],time.time(),
                     'pending',time.time()+ttl,(old['revision']+1) if old else 1,0,'[]',r['id'],r['revision']))

    def topic_context(self,scope,companion,epoch,related_ids,maximum):
        with self.db() as db:
            db.execute('BEGIN')
            if self.ledger._epoch(db,scope)!=epoch:return None,None
            rows=[dict(r) for r in db.execute('SELECT * FROM companion_topics WHERE scope=? AND companion_id=? AND epoch=? '
                'AND expires_at>? ORDER BY is_current DESC,last_mentioned DESC LIMIT 64',(scope,companion,epoch,time.time()))]
            selected=[]
            for r in rows:
                if r['source_episode']:
                    source=db.execute('SELECT revision,status FROM companion_episodes WHERE id=? AND epoch=?',(r['source_episode'],epoch)).fetchone()
                    if not source or source['revision']!=r['episode_revision'] or source['status']!='active':continue
                if r['is_current'] or (r['source_episode'] in related_ids and r['status']=='pending'):
                    selected.append(r)
                if len(selected)>=maximum:break
            current=next((r['id'] for r in rows if r['is_current']),'')
            token=(epoch,current,tuple((r['id'],r['revision'],r['source_episode'],r['episode_revision'],r['is_current']) for r in selected))
            return selected,token

    def topic_context_current(self,scope,companion,token):
        epoch,current,refs=token
        with self.db() as db:
            if self.ledger._epoch(db,scope)!=epoch:return False
            active=db.execute('SELECT id FROM companion_topics WHERE scope=? AND companion_id=? AND epoch=? AND is_current=1 AND expires_at>?',
                (scope,companion,epoch,time.time())).fetchone()
            if (active[0] if active else '')!=current:return False
            for identifier,revision,source,source_revision,is_current in refs:
                row=db.execute('SELECT revision,expires_at,is_current FROM companion_topics WHERE id=? AND scope=? AND companion_id=? AND epoch=?',
                    (identifier,scope,companion,epoch)).fetchone()
                if not row or row['revision']!=revision or row['expires_at']<=time.time() or row['is_current']!=is_current:return False
                if source:
                    old=db.execute('SELECT revision,status FROM companion_episodes WHERE id=? AND epoch=?',(source,epoch)).fetchone()
                    if not old or old['revision']!=source_revision or old['status']!='active':return False
            return True
