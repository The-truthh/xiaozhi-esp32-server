"""Durable sources, current facts and outbox; PowerMem is the derived search index.

Transactions fence late extraction/index writes by scope epoch and fact revision.
The ledger is local to this deployment, never a substitute for caller identity.
"""
from contextlib import contextmanager
import base64
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sqlite3
import time
import unicodedata
import uuid
import re


def now():
    return datetime.now(timezone.utc).isoformat()


def encode_item(label):
    """Reversible storage component, preserving meaningful spaces/symbols.

    Legacy word-only labels remain readable. '~' never occurred in legacy
    validated keys, so encoded labels cannot alias a literal legacy prefix.
    """
    normalized = ' '.join(unicodedata.normalize('NFKC', label).casefold().split())
    if not normalized or len(normalized) > 80:
        raise ValueError('Invalid item label')
    if re.fullmatch(r'[\w\-]+', normalized):
        return normalized
    return '~' + base64.urlsafe_b64encode(normalized.encode()).decode().rstrip('=')


def decode_item(component):
    if not component.startswith('~'):
        return component
    payload = component[1:]
    try:
        return base64.b64decode(payload + '=' * (-len(payload) % 4), altchars=b'-_', validate=True).decode()
    except (ValueError, UnicodeError) as exc:
        raise ValueError('Invalid encoded item') from exc


def canonical_key(key):
    parts = key.split(':', 2)
    return ':'.join(parts[:2] + [encode_item(decode_item(parts[2]))]) if len(parts) == 3 else key


class Store:
    def __init__(self, path):
        self.path = str(Path(path).resolve())
        Path(self.path).parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with self.db() as db:
            db.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS scopes(
                    scope TEXT PRIMARY KEY, epoch INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS sources(
                    id TEXT PRIMARY KEY, scope TEXT NOT NULL, epoch INTEGER NOT NULL,
                    text TEXT NOT NULL, observed_at TEXT NOT NULL, origin TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS facts(
                    id TEXT PRIMARY KEY, scope TEXT NOT NULL, fact_key TEXT NOT NULL,
                    kind TEXT NOT NULL, subject TEXT NOT NULL, content TEXT NOT NULL,
                    evidence TEXT NOT NULL, source_id TEXT NOT NULL,
                    observed_at TEXT NOT NULL, event_at TEXT, expires_at TEXT,
                    revision INTEGER NOT NULL, active INTEGER NOT NULL,
                    index_id TEXT, UNIQUE(scope,fact_key));
                CREATE TABLE IF NOT EXISTS jobs(
                    id TEXT PRIMARY KEY, scope TEXT NOT NULL, epoch INTEGER NOT NULL,
                    kind TEXT NOT NULL, payload TEXT NOT NULL, state TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0, due REAL NOT NULL DEFAULT 0,
                    error_class TEXT, updated_at TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS job_ready ON jobs(state,due);
                CREATE INDEX IF NOT EXISTS active_fact ON facts(scope,active);
            """)
        Path(self.path).chmod(0o600)

    @contextmanager
    def db(self):
        db = sqlite3.connect(self.path, timeout=2)
        db.row_factory = sqlite3.Row
        try:
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def _epoch(db, scope):
        db.execute("INSERT OR IGNORE INTO scopes(scope) VALUES(?)", (scope,))
        return db.execute("SELECT epoch FROM scopes WHERE scope=?", (scope,)).fetchone()[0]

    @staticmethod
    def _job(db, identifier, scope, epoch, kind, payload):
        db.execute("INSERT OR IGNORE INTO jobs VALUES(?,?,?,?,?,'pending',0,0,NULL,?)",
                   (identifier, scope, epoch, kind, json.dumps(payload, ensure_ascii=False), now()))

    def ingest(self, scope, event):
        with self.db() as db:
            db.execute("BEGIN IMMEDIATE")
            epoch = self._epoch(db, scope)
            identifier = hashlib.sha256((scope + ':' + event['id']).encode()).hexdigest()
            # Repeated delivery never reactivates a completed/deleted source.
            old = db.execute("SELECT state FROM jobs WHERE id=?", (identifier,)).fetchone()
            if old:
                return identifier
            db.execute("INSERT INTO sources VALUES(?,?,?,?,?,?)",
                       (identifier, scope, epoch, event['text'], event['observed_at'], event['origin']))
            self._job(db, identifier, scope, epoch, 'extract', {'source_id': identifier})
        return identifier

    def claim(self):
        with self.db() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM jobs WHERE state='pending' AND due<=? "
                             "ORDER BY CASE kind WHEN 'erase' THEN 0 ELSE 1 END,rowid LIMIT 1",
                             (time.time(),)).fetchone()
            if not row:
                return None
            db.execute("UPDATE jobs SET state='running',attempts=attempts+1,updated_at=? WHERE id=?",
                       (now(), row['id']))
            return dict(row)

    def recover(self):
        # Called once by the single process owner before its worker starts.
        with self.db() as db:
            db.execute("UPDATE jobs SET state='pending',due=0 WHERE state='running'")

    def source(self, job):
        with self.db() as db:
            row = db.execute("SELECT s.* FROM sources s JOIN scopes e ON s.scope=e.scope "
                             "WHERE s.id=? AND s.epoch=e.epoch AND s.epoch=?",
                             (job['payload']['source_id'], job['epoch'])).fetchone()
            return dict(row) if row else None

    def current(self, scope):
        with self.db() as db:
            return [dict(row) for row in db.execute(
                "SELECT * FROM facts WHERE scope=? AND active=1 "
                "AND (expires_at IS NULL OR expires_at>?) ORDER BY observed_at DESC,id",
                (scope, now())).fetchall()]

    def apply(self, job, source, candidates):
        with self.db() as db:
            db.execute("BEGIN IMMEDIATE")
            if self._epoch(db, job['scope']) != job['epoch']:
                return False
            for fact in candidates:
                old = db.execute("SELECT * FROM facts WHERE scope=? AND fact_key=?",
                                 (job['scope'], fact['key'])).fetchone()
                if old is None and ':' in fact['key']:
                    # Reuse an existing differently cased/width legacy label;
                    # keep its ID/key rather than creating a second memory.
                    matches = [r for r in db.execute('SELECT * FROM facts WHERE scope=?', (job['scope'],))
                               if canonical_key(r['fact_key']) == canonical_key(fact['key'])]
                    if len(matches) > 1:
                        raise ValueError('Ambiguous legacy item labels')
                    old = matches[0] if matches else None
                storage_key = old['fact_key'] if old else fact['key']
                identifier = old['id'] if old else uuid.uuid4().hex
                revision = old['revision'] + 1 if old else 1
                if old and old['observed_at'] > source['observed_at']:
                    continue  # A delayed retry cannot overwrite a newer correction.
                if old and old['index_id']:
                    self._job(db, uuid.uuid4().hex, job['scope'], job['epoch'], 'erase',
                              {'index_id': old['index_id']})
                expires = None
                if fact['kind'] != 'profile':
                    expires = (datetime.fromisoformat(source['observed_at']) + timedelta(days=30)).isoformat()
                db.execute("INSERT INTO facts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,1,NULL) "
                           "ON CONFLICT(scope,fact_key) DO UPDATE SET kind=excluded.kind,"
                           "subject=excluded.subject,content=excluded.content,evidence=excluded.evidence,"
                           "source_id=excluded.source_id,observed_at=excluded.observed_at,"
                           "event_at=excluded.event_at,expires_at=excluded.expires_at,"
                           "revision=excluded.revision,active=1,index_id=NULL",
                           (identifier, job['scope'], storage_key, fact['kind'], fact['subject'],
                            fact['content'], fact['evidence'], source['id'], source['observed_at'],
                            fact.get('event_at'), expires, revision))
                self._job(db, uuid.uuid4().hex, job['scope'], job['epoch'], 'project',
                          {'fact_id': identifier, 'revision': revision})
        return True

    def personal_snapshot(self, scope):
        """One consistent read of current scoped facts and unresolved sources.

        No caller/model-selected subject, vector query or mutation. The pending
        source text remains internal and is never part of diagnostic metadata.
        """
        with self.db() as db:
            db.execute('BEGIN')
            row = db.execute('SELECT epoch FROM scopes WHERE scope=?', (scope,)).fetchone()
            epoch = row['epoch'] if row else 0
            facts = [dict(r) for r in db.execute(
                "SELECT * FROM facts WHERE scope=? AND active=1 "
                "AND (expires_at IS NULL OR expires_at>?) ORDER BY observed_at DESC,id", (scope, now())).fetchall()]
            pending = [dict(r) for r in db.execute(
                "SELECT s.id,s.text,s.observed_at,j.state FROM sources s "
                "JOIN jobs j ON j.id=s.id WHERE s.scope=? AND s.epoch=? "
                "AND j.kind='extract' AND j.state IN ('pending','running','failed')",
                (scope, epoch)).fetchall()]
        return {'epoch': epoch, 'facts': facts, 'pending': pending}

    def identity_snapshot(self, scope):
        snapshot = self.personal_snapshot(scope)
        snapshot['facts'] = [r for r in snapshot['facts'] if r['subject'] == '本人'
                             and r['fact_key'] in ('name', 'preferred_address')]
        return snapshot

    def projection(self, job):
        with self.db() as db:
            if self._epoch(db, job['scope']) != job['epoch']:
                return None
            row = db.execute("SELECT * FROM facts WHERE id=? AND scope=? AND revision=? AND active=1",
                             (job['payload']['fact_id'], job['scope'], job['payload']['revision'])).fetchone()
            return dict(row) if row else None

    def link_index(self, job, index_id):
        with self.db() as db:
            db.execute("BEGIN IMMEDIATE")
            if self._epoch(db, job['scope']) != job['epoch']:
                return False
            cursor = db.execute("UPDATE facts SET index_id=? WHERE id=? AND scope=? AND revision=? AND active=1",
                                (str(index_id), job['payload']['fact_id'], job['scope'], job['payload']['revision']))
            return cursor.rowcount == 1

    def forget(self, scope, keys=None):
        with self.db() as db:
            db.execute("BEGIN IMMEDIATE")
            epoch = self._epoch(db, scope) + 1
            db.execute("UPDATE scopes SET epoch=? WHERE scope=?", (epoch, scope))
            # Every old extraction job is fenced; even old sources can no longer
            # be re-extracted after a deletion. This is deliberately conservative.
            db.execute("UPDATE jobs SET state='cancelled',payload='{}' WHERE scope=? "
                       "AND kind='extract' AND state IN ('pending','running','failed')", (scope,))
            rows = db.execute("SELECT * FROM facts WHERE scope=? AND active=1", (scope,)).fetchall()
            selected = [row for row in rows if keys is None or row['fact_key'] in keys]
            for row in selected:
                if row['index_id']:
                    self._job(db, uuid.uuid4().hex, scope, epoch, 'erase', {'index_id': row['index_id']})
                db.execute("UPDATE facts SET active=0,content='',evidence='',index_id=NULL WHERE id=?", (row['id'],))
            # Erase eligible raw text too; original application chat logs are a
            # separate retention surface and are not promised deleted here.
            if keys is None:
                db.execute("DELETE FROM sources WHERE scope=?", (scope,))
            else:
                for row in selected:
                    db.execute("DELETE FROM sources WHERE id=? AND scope=?", (row['source_id'], scope))
            # Surviving unindexed facts need fresh-epoch projection jobs.
            for row in rows:
                if row not in selected and not row['index_id']:
                    self._job(db, uuid.uuid4().hex, scope, epoch, 'project',
                              {'fact_id': row['id'], 'revision': row['revision']})
            self._job(db, uuid.uuid4().hex, scope, epoch, 'cleanup', {})
            return len(selected)

    def cleanup(self, scope):
        with self.db() as db:
            self._job(db, uuid.uuid4().hex, scope, self._epoch(db, scope), 'cleanup', {})

    def erase_later(self, scope, index_id):
        with self.db() as db:
            self._job(db, uuid.uuid4().hex, scope, self._epoch(db, scope), 'erase',
                      {'index_id': str(index_id)})

    def finish(self, job, error=None):
        with self.db() as db:
            if error is None:
                db.execute("UPDATE jobs SET state='done',error_class=NULL,updated_at=? "
                           "WHERE id=? AND state='running'", (now(), job['id']))
            else:
                row = db.execute("SELECT attempts FROM jobs WHERE id=?", (job['id'],)).fetchone()
                attempts = row[0] if row else 3
                state = 'pending' if attempts < 3 else 'failed'
                db.execute("UPDATE jobs SET state=?,error_class=?,due=?,updated_at=? "
                           "WHERE id=? AND state='running'",
                           (state, type(error).__name__, time.time()+min(30,2**attempts), now(), job['id']))

    def status(self, identifier):
        with self.db() as db:
            row = db.execute("SELECT state,error_class,attempts FROM jobs WHERE id=?", (identifier,)).fetchone()
            return dict(row) if row else {'state': 'unknown'}

    def pending(self, scope):
        with self.db() as db:
            return dict(db.execute("SELECT state,COUNT(*) FROM jobs WHERE scope=? "
                                   "AND state IN ('pending','running','failed') GROUP BY state", (scope,)).fetchall())
