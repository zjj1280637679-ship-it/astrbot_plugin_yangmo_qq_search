"""SQLite fact index. QQ remains the source of truth; this database is rebuildable."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
from collections.abc import Iterable


class QQSearchStore:
    def __init__(self, database_path: str) -> None:
        self.database_path = os.path.abspath(database_path)
        os.makedirs(os.path.dirname(self.database_path), exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            self.database_path,
            timeout=5,
            check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        self._configure()
        self._init_schema()

    def _configure(self) -> None:
        with self._conn:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.execute("PRAGMA foreign_keys=ON")

    def _init_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    message_seq TEXT NOT NULL DEFAULT '',
                    sent_at INTEGER NOT NULL DEFAULT 0,
                    sender_id TEXT NOT NULL DEFAULT '',
                    sender_name TEXT NOT NULL DEFAULT '',
                    sender_role TEXT NOT NULL DEFAULT '',
                    body TEXT NOT NULL DEFAULT '',
                    segment_types TEXT NOT NULL DEFAULT '||',
                    segments_json TEXT NOT NULL DEFAULT '[]',
                    reply_to TEXT NOT NULL DEFAULT '',
                    forward_id TEXT NOT NULL DEFAULT '',
                    recalled INTEGER NOT NULL DEFAULT 0 CHECK(recalled IN (0, 1)),
                    source TEXT NOT NULL DEFAULT 'history',
                    content_hash TEXT NOT NULL,
                    updated_at INTEGER NOT NULL,
                    UNIQUE(account_id, group_id, message_id)
                );

                CREATE INDEX IF NOT EXISTS messages_scope_time
                ON messages(account_id, group_id, sent_at, id);

                CREATE INDEX IF NOT EXISTS messages_sender_time
                ON messages(account_id, group_id, sender_id, sent_at);

                CREATE TABLE IF NOT EXISTS recall_tombstones (
                    account_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    message_seq TEXT NOT NULL DEFAULT '',
                    recalled_at INTEGER NOT NULL,
                    PRIMARY KEY(account_id, group_id, message_id)
                );

                CREATE TABLE IF NOT EXISTS sync_state (
                    account_id TEXT NOT NULL,
                    group_id TEXT NOT NULL,
                    backfill_cursor TEXT NOT NULL DEFAULT '',
                    backfill_complete INTEGER NOT NULL DEFAULT 0,
                    last_sync_at INTEGER NOT NULL DEFAULT 0,
                    last_success_at INTEGER NOT NULL DEFAULT 0,
                    last_seen_at INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY(account_id, group_id)
                );
                """
            )
            tombstone_columns = {
                str(row["name"])
                for row in self._conn.execute("PRAGMA table_info(recall_tombstones)")
            }
            if "message_seq" not in tombstone_columns:
                self._conn.execute(
                    "ALTER TABLE recall_tombstones ADD COLUMN message_seq TEXT NOT NULL DEFAULT ''"
                )
            self._conn.execute(
                "UPDATE recall_tombstones SET message_seq=message_id WHERE message_seq=''"
            )
            existing = self._conn.execute(
                "SELECT value FROM metadata WHERE key='fts_tokenizer'"
            ).fetchone()
            if existing is None:
                try:
                    self._conn.execute(
                        "CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts "
                        "USING fts5(sender_name, body, content='messages', "
                        "content_rowid='id', tokenize='trigram')"
                    )
                    tokenizer = "trigram"
                except sqlite3.OperationalError:
                    self._conn.execute(
                        "CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts "
                        "USING fts5(sender_name, body, content='messages', "
                        "content_rowid='id', tokenize='unicode61')"
                    )
                    tokenizer = "unicode61"
                self._conn.execute(
                    "INSERT OR REPLACE INTO metadata(key, value) VALUES('fts_tokenizer', ?)",
                    (tokenizer,),
                )
            self._conn.executescript(
                """
                CREATE TRIGGER IF NOT EXISTS messages_ai AFTER INSERT ON messages BEGIN
                    INSERT INTO messages_fts(rowid, sender_name, body)
                    VALUES (new.id, new.sender_name, new.body);
                END;
                CREATE TRIGGER IF NOT EXISTS messages_ad AFTER DELETE ON messages BEGIN
                    INSERT INTO messages_fts(messages_fts, rowid, sender_name, body)
                    VALUES ('delete', old.id, old.sender_name, old.body);
                END;
                CREATE TRIGGER IF NOT EXISTS messages_au AFTER UPDATE ON messages BEGIN
                    INSERT INTO messages_fts(messages_fts, rowid, sender_name, body)
                    VALUES ('delete', old.id, old.sender_name, old.body);
                    INSERT INTO messages_fts(rowid, sender_name, body)
                    VALUES (new.id, new.sender_name, new.body);
                END;
                """
            )

    @property
    def tokenizer(self) -> str:
        with self._lock:
            row = self._conn.execute(
                "SELECT value FROM metadata WHERE key='fts_tokenizer'"
            ).fetchone()
        return str(row[0]) if row else "unknown"

    @staticmethod
    def _types(value) -> str:
        items = sorted({str(item).strip() for item in (value or []) if str(item).strip()})
        return "|" + "|".join(items) + "|"

    @staticmethod
    def _canonical(account_id: str, record: dict) -> tuple:
        sender = record.get("sender") if isinstance(record.get("sender"), dict) else {}
        segments = record.get("segments") if isinstance(record.get("segments"), list) else []
        stable = {
            "message_id": str(record.get("message_id") or ""),
            "message_seq": str(record.get("message_seq") or ""),
            "time": int(record.get("time") or 0),
            "group_id": str(record.get("group_id") or ""),
            "sender_id": str(record.get("user_id") or sender.get("id") or ""),
            "sender_name": str(sender.get("name") or ""),
            "sender_role": str(sender.get("role") or ""),
            "body": str(record.get("text") or ""),
            "segment_types": QQSearchStore._types(record.get("segment_types")),
            "segments_json": json.dumps(segments, ensure_ascii=False, separators=(",", ":")),
            "reply_to": str(record.get("reply_to") or ""),
            "forward_id": str(record.get("forward_id") or ""),
            "recalled": 1 if record.get("recalled") else 0,
            "source": str(record.get("source") or "history"),
        }
        digest = hashlib.sha256(
            json.dumps(stable, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        return (
            str(account_id),
            stable["group_id"],
            stable["message_id"],
            stable["message_seq"],
            stable["time"],
            stable["sender_id"],
            stable["sender_name"],
            stable["sender_role"],
            stable["body"],
            stable["segment_types"],
            stable["segments_json"],
            stable["reply_to"],
            stable["forward_id"],
            stable["recalled"],
            stable["source"],
            digest,
            int(time.time()),
        )

    def upsert_many(self, account_id: str, records: Iterable[dict]) -> int:
        candidates = []
        for record in records:
            if not isinstance(record, dict):
                continue
            if not str(record.get("group_id") or "") or not str(record.get("message_id") or ""):
                continue
            candidates.append(dict(record))
        if not candidates:
            return 0
        sql = """
            INSERT INTO messages(
                account_id, group_id, message_id, message_seq, sent_at,
                sender_id, sender_name, sender_role, body, segment_types,
                segments_json, reply_to, forward_id, recalled, source,
                content_hash, updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(account_id, group_id, message_id) DO UPDATE SET
                message_seq=excluded.message_seq,
                sent_at=excluded.sent_at,
                sender_id=excluded.sender_id,
                sender_name=excluded.sender_name,
                sender_role=excluded.sender_role,
                body=excluded.body,
                segment_types=excluded.segment_types,
                segments_json=excluded.segments_json,
                reply_to=excluded.reply_to,
                forward_id=excluded.forward_id,
                recalled=MAX(messages.recalled, excluded.recalled),
                source=excluded.source,
                content_hash=excluded.content_hash,
                updated_at=excluded.updated_at
        """
        with self._lock, self._conn:
            # OneBot and QQ's native history API can assign different message IDs
            # to the same group message. QQ's per-group message sequence is the
            # stable identity shared by both paths, so retain the first stored ID.
            sequence_identity: dict[tuple[str, str], str] = {}
            rows_by_identity: dict[tuple[str, str, str], tuple] = {}
            for record in candidates:
                group_id = str(record.get("group_id") or "")
                message_seq = str(record.get("message_seq") or "")
                incoming_id = str(record.get("message_id") or "")
                canonical_id = None
                if message_seq:
                    sequence_key = (group_id, message_seq)
                    canonical_id = sequence_identity.get(sequence_key)
                    if canonical_id is None:
                        existing = self._conn.execute(
                            """
                            SELECT message_id FROM messages
                            WHERE account_id=? AND group_id=? AND message_seq=?
                            ORDER BY id ASC LIMIT 1
                            """,
                            (str(account_id), group_id, message_seq),
                        ).fetchone()
                        if existing is not None:
                            canonical_id = str(existing["message_id"])

                if canonical_id is None and message_seq and not message_seq.startswith("synthetic-"):
                    provisional = self._canonical(account_id, record)
                    legacy_matches = self._conn.execute(
                        """
                        SELECT message_id FROM messages
                        WHERE account_id=? AND group_id=? AND sender_id=?
                          AND sent_at=? AND body=? AND segment_types=?
                          AND segments_json=?
                          AND (message_seq='' OR message_seq LIKE 'synthetic-%')
                        ORDER BY id ASC LIMIT 2
                        """,
                        (
                            provisional[0],
                            provisional[1],
                            provisional[5],
                            provisional[4],
                            provisional[8],
                            provisional[9],
                            provisional[10],
                        ),
                    ).fetchall()
                    if len(legacy_matches) == 1:
                        canonical_id = str(legacy_matches[0]["message_id"])

                canonical_id = canonical_id or str(record.get("message_id") or "")
                record["message_id"] = canonical_id
                tombstone = self._conn.execute(
                    """
                    SELECT 1 FROM recall_tombstones
                    WHERE account_id=? AND group_id=?
                      AND (message_id IN (?,?) OR (message_seq<>'' AND message_seq=?))
                    LIMIT 1
                    """,
                    (
                        str(account_id),
                        group_id,
                        incoming_id,
                        canonical_id,
                        message_seq,
                    ),
                ).fetchone()
                if tombstone is not None:
                    record["recalled"] = True
                if message_seq:
                    sequence_identity[(group_id, message_seq)] = canonical_id

                row = self._canonical(account_id, record)
                rows_by_identity[(row[0], row[1], row[2])] = row

            rows = list(rows_by_identity.values())
            self._conn.executemany(sql, rows)
        return len(rows)

    def note_group(self, account_id: str, group_id: str, seen_at: int | None = None) -> None:
        now = int(seen_at or time.time())
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO sync_state(account_id, group_id, last_seen_at)
                VALUES(?,?,?)
                ON CONFLICT(account_id, group_id) DO UPDATE SET
                    last_seen_at=MAX(sync_state.last_seen_at, excluded.last_seen_at)
                """,
                (str(account_id), str(group_id), now),
            )

    def get_sync_state(self, account_id: str, group_id: str) -> dict:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM sync_state WHERE account_id=? AND group_id=?",
                (str(account_id), str(group_id)),
            ).fetchone()
        if row is None:
            self.note_group(account_id, group_id)
            return self.get_sync_state(account_id, group_id)
        return dict(row)

    def set_sync_state(
        self,
        account_id: str,
        group_id: str,
        *,
        cursor: str | None = None,
        complete: bool | None = None,
        success: bool = False,
        error: str = "",
    ) -> None:
        self.note_group(account_id, group_id)
        now = int(time.time())
        fields = ["last_sync_at=?", "last_error=?"]
        values: list = [now, str(error or "")[:300]]
        if cursor is not None:
            fields.append("backfill_cursor=?")
            values.append(str(cursor))
        if complete is not None:
            fields.append("backfill_complete=?")
            values.append(1 if complete else 0)
        if success:
            fields.append("last_success_at=?")
            values.append(now)
        values.extend([str(account_id), str(group_id)])
        with self._lock, self._conn:
            self._conn.execute(
                f"UPDATE sync_state SET {','.join(fields)} "
                "WHERE account_id=? AND group_id=?",
                values,
            )

    def mark_recalled(
        self,
        account_id: str,
        group_id: str,
        message_id: str,
        message_seq: str = "",
    ) -> bool:
        account_id = str(account_id or "")
        group_id = str(group_id or "")
        message_id = str(message_id or "")
        if not account_id or not group_id or not message_id:
            return False
        message_seq = str(message_seq or message_id)
        now = int(time.time())
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO recall_tombstones(
                    account_id, group_id, message_id, message_seq, recalled_at
                ) VALUES(?,?,?,?,?)
                ON CONFLICT(account_id, group_id, message_id) DO UPDATE SET
                    message_seq=CASE
                        WHEN excluded.message_seq<>'' THEN excluded.message_seq
                        ELSE recall_tombstones.message_seq
                    END,
                    recalled_at=MAX(recall_tombstones.recalled_at, excluded.recalled_at)
                """,
                (account_id, group_id, message_id, message_seq, now),
            )
            self._conn.execute(
                """
                UPDATE messages SET recalled=1, updated_at=?
                WHERE account_id=? AND group_id=? AND message_id=?
                """,
                (now, account_id, group_id, message_id),
            )
        return True

    @staticmethod
    def _filters(
        account_id: str,
        group_id: str,
        sender_id: str,
        since: int | None,
        until: int | None,
        types: list[str] | None,
    ) -> tuple[list[str], list]:
        clauses = ["m.account_id=?", "m.group_id=?", "m.recalled=0"]
        params: list = [str(account_id), str(group_id)]
        if sender_id:
            clauses.append("m.sender_id=?")
            params.append(str(sender_id))
        if since is not None:
            clauses.append("m.sent_at>=?")
            params.append(int(since))
        if until is not None:
            clauses.append("m.sent_at<=?")
            params.append(int(until))
        normalized_types = [str(kind).strip() for kind in types or [] if str(kind).strip()]
        if normalized_types:
            clauses.append("(" + " OR ".join("m.segment_types LIKE ?" for _ in normalized_types) + ")")
            params.extend(f"%|{kind}|%" for kind in normalized_types)
        return clauses, params

    def search(
        self,
        account_id: str,
        group_id: str,
        query: str,
        *,
        sender_id: str = "",
        since: int | None = None,
        until: int | None = None,
        types: list[str] | None = None,
        limit: int = 8,
    ) -> list[dict]:
        query = str(query or "").strip()
        limit = min(max(int(limit), 1), 50)
        clauses, params = self._filters(
            account_id, group_id, sender_id, since, until, types
        )
        compact_len = len("".join(query.split()))
        if not query:
            sql = (
                "SELECT m.*, 0.0 AS rank FROM messages m WHERE "
                + " AND ".join(clauses)
                + " ORDER BY m.sent_at DESC, m.id DESC LIMIT ?"
            )
        elif compact_len < 3:
            clauses.append("(m.body LIKE ? OR m.sender_name LIKE ?)")
            params.extend([f"%{query}%", f"%{query}%"])
            sql = (
                "SELECT m.*, 0.0 AS rank FROM messages m WHERE "
                + " AND ".join(clauses)
                + " ORDER BY m.sent_at DESC, m.id DESC LIMIT ?"
            )
        else:
            phrase = f'"{query.replace(chr(34), chr(34) * 2)}"'
            clauses.insert(0, "messages_fts MATCH ?")
            params.insert(0, phrase)
            sql = (
                "SELECT m.*, bm25(messages_fts) AS rank FROM messages_fts "
                "JOIN messages m ON m.id=messages_fts.rowid WHERE "
                + " AND ".join(clauses)
                + " ORDER BY rank, m.sent_at DESC LIMIT ?"
            )
        params.append(limit)
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def list_by_sender(
        self,
        account_id: str,
        group_id: str,
        sender_id: str,
        *,
        since: int | None = None,
        until: int | None = None,
        limit: int = 30,
    ) -> list[dict]:
        """List cached messages for one sender without requiring a keyword."""
        if not str(sender_id or "").strip():
            return []
        limit = min(max(int(limit), 1), 100)
        clauses, params = self._filters(
            account_id,
            group_id,
            str(sender_id),
            since,
            until,
            None,
        )
        params.append(limit)
        sql = (
            "SELECT m.*, 0.0 AS rank FROM messages m WHERE "
            + " AND ".join(clauses)
            + " ORDER BY m.sent_at DESC, m.id DESC LIMIT ?"
        )
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def open_message(
        self,
        account_id: str,
        group_id: str,
        message_id: str,
        *,
        before: int = 2,
        after: int = 2,
    ) -> list[dict]:
        before = min(max(int(before), 0), 20)
        after = min(max(int(after), 0), 20)
        scope = (str(account_id), str(group_id), str(message_id))
        with self._lock:
            target = self._conn.execute(
                """
                SELECT * FROM messages
                WHERE account_id=? AND group_id=? AND message_id=?
                """,
                scope,
            ).fetchone()
            if target is None:
                return []
            earlier = self._conn.execute(
                """
                SELECT * FROM messages
                WHERE account_id=? AND group_id=?
                  AND (sent_at < ? OR (sent_at=? AND id < ?))
                ORDER BY sent_at DESC, id DESC LIMIT ?
                """,
                (
                    str(account_id), str(group_id), target["sent_at"],
                    target["sent_at"], target["id"], before,
                ),
            ).fetchall()
            later = self._conn.execute(
                """
                SELECT * FROM messages
                WHERE account_id=? AND group_id=?
                  AND (sent_at > ? OR (sent_at=? AND id > ?))
                ORDER BY sent_at, id LIMIT ?
                """,
                (
                    str(account_id), str(group_id), target["sent_at"],
                    target["sent_at"], target["id"], after,
                ),
            ).fetchall()
        return [dict(row) for row in reversed(earlier)] + [dict(target)] + [dict(row) for row in later]

    def status(self, account_id: str, group_id: str) -> dict:
        state = self.get_sync_state(account_id, group_id)
        with self._lock:
            counts = self._conn.execute(
                """
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN recalled=1 THEN 1 ELSE 0 END) AS recalled,
                       MIN(sent_at) AS oldest,
                       MAX(sent_at) AS newest
                FROM messages WHERE account_id=? AND group_id=?
                """,
                (str(account_id), str(group_id)),
            ).fetchone()
        return {**state, **dict(counts), "fts_tokenizer": self.tokenizer}

    def active_groups(self, account_id: str, *, since: int, limit: int) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT group_id FROM sync_state
                WHERE account_id=? AND last_seen_at>=?
                ORDER BY last_seen_at DESC LIMIT ?
                """,
                (str(account_id), int(since), min(max(int(limit), 1), 100)),
            ).fetchall()
        return [str(row[0]) for row in rows]

    def latest_account(self) -> str:
        with self._lock:
            row = self._conn.execute(
                "SELECT account_id FROM sync_state ORDER BY last_seen_at DESC LIMIT 1"
            ).fetchone()
        return str(row[0]) if row else ""

    def rebuild_fts(self) -> None:
        with self._lock, self._conn:
            self._conn.execute("INSERT INTO messages_fts(messages_fts) VALUES('rebuild')")

    def close(self) -> None:
        with self._lock:
            self._conn.close()
