from __future__ import annotations

from datetime import datetime, timezone

from app.db import connect

STREAM_SEEDS = [
    {
        "code": "member_profile",
        "title": "会员档案变更",
        "domain": "member",
        "entity_key_name": "member_id",
        "payload_fields": ["display_name", "tier", "region"],
        "fixture": "member_profile.json",
        "enabled": 1,
    },
    {
        "code": "store_archive",
        "title": "门店档案变更",
        "domain": "store",
        "entity_key_name": "store_id",
        "payload_fields": ["name", "status", "city"],
        "fixture": "store_archive.json",
        "enabled": 1,
    },
    {
        "code": "channel_attr",
        "title": "渠道属性变更",
        "domain": "channel",
        "entity_key_name": "channel_code",
        "payload_fields": ["label", "active"],
        "fixture": "channel_attr.json",
        "enabled": 1,
    },
]

SETTING_DEFAULTS = [
    ("apply_order", "by_seq"),
    ("duplicate_policy", "skip"),
    ("tombstone_mode", "soft_delete"),
    # 互斥部分失败结局：rollback / keep_prefix
    ("partial_failure_policy", "rollback"),
]

# 批次结局列（旧库通过 ALTER TABLE 补齐）
BATCH_EXTRA_COLUMNS = [
    ("outcome_policy", "TEXT"),
    ("success_count", "INTEGER NOT NULL DEFAULT 0"),
    ("failure_count", "INTEGER NOT NULL DEFAULT 0"),
    ("blocked_count", "INTEGER NOT NULL DEFAULT 0"),
    ("attempted_count", "INTEGER NOT NULL DEFAULT 0"),
    ("discarded_count", "INTEGER NOT NULL DEFAULT 0"),
    ("rolled_back", "INTEGER NOT NULL DEFAULT 0"),
    ("fail_seq", "INTEGER"),
    ("fail_event_id", "TEXT"),
    ("watermark_before", "INTEGER NOT NULL DEFAULT 0"),
    ("watermark_after", "INTEGER NOT NULL DEFAULT 0"),
    ("error_message", "TEXT"),
    ("projection_before_digest", "TEXT"),
    ("projection_after_digest", "TEXT"),
]


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _ensure_batch_columns(conn) -> set[str]:
    """Return the set of columns that existed before migration (for backfill)."""
    existing = {r["name"] for r in conn.execute("PRAGMA table_info(project_batches)").fetchall()}
    for name, decl in BATCH_EXTRA_COLUMNS:
        if name not in existing:
            conn.execute(f"ALTER TABLE project_batches ADD COLUMN {name} {decl}")
    return existing


def _backfill_old_batches(conn, existing: set[str]) -> None:
    """Old completed batches predate the new columns; derive a coherent view."""
    if "success_count" in existing:
        return
    conn.execute(
        """
        UPDATE project_batches
        SET success_count = applied_upsert + applied_delete,
            attempted_count = source_events,
            watermark_after = max_seq,
            outcome_policy = 'by_seq_full'
        WHERE success_count = 0 AND status = 'completed'
        """
    )


def init_db() -> None:
    import json

    conn = connect()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS project_streams(
                id INTEGER PRIMARY KEY,
                code TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL,
                domain TEXT NOT NULL,
                entity_key_name TEXT NOT NULL,
                payload_fields_json TEXT NOT NULL,
                fixture TEXT NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                watermark_seq INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS project_batches(
                id INTEGER PRIMARY KEY,
                batch_no TEXT NOT NULL UNIQUE,
                stream_id INTEGER NOT NULL,
                status TEXT NOT NULL,
                source_events INTEGER NOT NULL,
                applied_upsert INTEGER NOT NULL,
                applied_delete INTEGER NOT NULL,
                skipped_duplicate INTEGER NOT NULL,
                projected_alive INTEGER NOT NULL,
                projected_deleted INTEGER NOT NULL,
                max_seq INTEGER NOT NULL,
                note TEXT,
                created_at TEXT NOT NULL,
                outcome_policy TEXT,
                success_count INTEGER NOT NULL DEFAULT 0,
                failure_count INTEGER NOT NULL DEFAULT 0,
                blocked_count INTEGER NOT NULL DEFAULT 0,
                attempted_count INTEGER NOT NULL DEFAULT 0,
                rolled_back INTEGER NOT NULL DEFAULT 0,
                fail_seq INTEGER,
                fail_event_id TEXT,
                watermark_before INTEGER NOT NULL DEFAULT 0,
                watermark_after INTEGER NOT NULL DEFAULT 0,
                error_message TEXT,
                discarded_count INTEGER NOT NULL DEFAULT 0,
                projection_before_digest TEXT,
                projection_after_digest TEXT,
                FOREIGN KEY(stream_id) REFERENCES project_streams(id)
            );
            CREATE TABLE IF NOT EXISTS projection_rows(
                id INTEGER PRIMARY KEY,
                stream_id INTEGER NOT NULL,
                entity_key TEXT NOT NULL,
                payload_json TEXT,
                applied_seq INTEGER NOT NULL,
                applied_event_id TEXT NOT NULL,
                deleted INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL,
                UNIQUE(stream_id, entity_key),
                FOREIGN KEY(stream_id) REFERENCES project_streams(id)
            );
            CREATE TABLE IF NOT EXISTS applied_events(
                id INTEGER PRIMARY KEY,
                batch_id INTEGER NOT NULL,
                stream_id INTEGER NOT NULL,
                event_id TEXT NOT NULL,
                entity_key TEXT NOT NULL,
                op TEXT NOT NULL,
                seq INTEGER NOT NULL,
                event_time TEXT NOT NULL,
                result TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(batch_id) REFERENCES project_batches(id),
                FOREIGN KEY(stream_id) REFERENCES project_streams(id)
            );
            CREATE TABLE IF NOT EXISTS ooo_buffer(
                id INTEGER PRIMARY KEY,
                stream_id INTEGER NOT NULL,
                event_id TEXT NOT NULL,
                entity_key TEXT NOT NULL,
                op TEXT NOT NULL,
                seq INTEGER NOT NULL,
                event_time TEXT NOT NULL,
                payload_json TEXT,
                buffered_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                FOREIGN KEY(stream_id) REFERENCES project_streams(id)
            );
            CREATE TABLE IF NOT EXISTS settings(
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        existing_batch_columns = _ensure_batch_columns(conn)
        _backfill_old_batches(conn, existing_batch_columns)

        if conn.execute("SELECT COUNT(*) c FROM project_streams").fetchone()["c"] == 0:
            now = _now()
            for s in STREAM_SEEDS:
                conn.execute(
                    """
                    INSERT INTO project_streams(
                        code, title, domain, entity_key_name, payload_fields_json,
                        fixture, enabled, watermark_seq, created_at
                    ) VALUES (?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        s["code"],
                        s["title"],
                        s["domain"],
                        s["entity_key_name"],
                        json.dumps(s["payload_fields"], ensure_ascii=False),
                        s["fixture"],
                        s["enabled"],
                        0,
                        now,
                    ),
                )
        for key, value in SETTING_DEFAULTS:
            conn.execute(
                "INSERT OR IGNORE INTO settings(key, value) VALUES (?, ?)",
                (key, value),
            )
        conn.commit()
    finally:
        conn.close()
