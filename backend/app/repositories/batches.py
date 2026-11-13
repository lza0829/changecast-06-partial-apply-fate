from __future__ import annotations

from typing import Any

import sqlite3

BATCH_FIELDS = (
    "batch_no",
    "stream_id",
    "status",
    "source_events",
    "applied_upsert",
    "applied_delete",
    "skipped_duplicate",
    "projected_alive",
    "projected_deleted",
    "max_seq",
    "note",
    "created_at",
    "outcome_policy",
    "success_count",
    "failure_count",
    "blocked_count",
    "attempted_count",
    "discarded_count",
    "rolled_back",
    "fail_seq",
    "fail_event_id",
    "watermark_before",
    "watermark_after",
    "error_message",
    "projection_before_digest",
    "projection_after_digest",
)


def insert_batch(
    conn: sqlite3.Connection,
    *,
    metrics: dict[str, Any],
) -> int:
    values = [metrics.get(f) for f in BATCH_FIELDS]
    placeholders = ",".join("?" for _ in BATCH_FIELDS)
    cur = conn.execute(
        f"INSERT INTO project_batches({','.join(BATCH_FIELDS)}) VALUES ({placeholders})",
        values,
    )
    return int(cur.lastrowid)


def update_batch(conn: sqlite3.Connection, batch_id: int, metrics: dict[str, Any]) -> None:
    fields = [f for f in BATCH_FIELDS if f in metrics]
    assignments = ",".join(f"{f}=?" for f in fields)
    conn.execute(
        f"UPDATE project_batches SET {assignments} WHERE id=?",
        [metrics[f] for f in fields] + [batch_id],
    )


def list_batches(conn: sqlite3.Connection, limit: int = 50) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM project_batches ORDER BY id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_batch(conn: sqlite3.Connection, batch_id: int) -> dict[str, Any] | None:
    r = conn.execute("SELECT * FROM project_batches WHERE id=?", (batch_id,)).fetchone()
    return dict(r) if r else None


def insert_applied_event(
    conn: sqlite3.Connection,
    *,
    batch_id: int,
    stream_id: int,
    event: dict[str, Any],
    created_at: str,
) -> None:
    conn.execute(
        """
        INSERT INTO applied_events(
            batch_id, stream_id, event_id, entity_key, op, seq, event_time, result, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            batch_id,
            stream_id,
            event["event_id"],
            event["entity_key"],
            event["op"],
            event["seq"],
            event["event_time"],
            event["result"],
            created_at,
        ),
    )


def list_applied_events(
    conn: sqlite3.Connection,
    *,
    batch_id: int | None = None,
    stream_id: int | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM applied_events WHERE 1=1"
    params: list[Any] = []
    if batch_id is not None:
        sql += " AND batch_id=?"
        params.append(batch_id)
    if stream_id is not None:
        sql += " AND stream_id=?"
        params.append(stream_id)
    sql += " ORDER BY seq ASC, id ASC LIMIT ?"
    params.append(limit)
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def count_results(conn: sqlite3.Connection, batch_id: int) -> dict[str, int]:
    rows = conn.execute(
        "SELECT result, COUNT(*) c FROM applied_events WHERE batch_id=? GROUP BY result",
        (batch_id,),
    ).fetchall()
    return {r["result"]: r["c"] for r in rows}
