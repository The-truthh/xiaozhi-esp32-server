"""Role affect outbox/state on the existing managed-memory SQLite connection.

An event sequence is allocated before generation, not when a slow observer ends.
Role state never enters the factual ledger or PowerMem vector index.
"""
from contextlib import contextmanager
import hashlib
import json
import time


class CompanionStore:
    def __init__(self, ledger):
        self.ledger = ledger
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
