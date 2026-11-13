from __future__ import annotations

import hashlib
import json
from typing import Any

import sqlite3


def replace_stream_projection(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    rows: list[dict[str, Any]],
    updated_at: str,
) -> None:
    conn.execute("DELETE FROM projection_rows WHERE stream_id=?", (stream_id,))
    for row in rows:
        conn.execute(
            """
            INSERT INTO projection_rows(
                stream_id, entity_key, payload_json, applied_seq, applied_event_id, deleted, updated_at
            ) VALUES (?,?,?,?,?,?,?)
            """,
            (
                stream_id,
                row["entity_key"],
                json.dumps(row.get("payload"), ensure_ascii=False) if row.get("payload") is not None else None,
                row["applied_seq"],
                row["applied_event_id"],
                1 if row.get("deleted") else 0,
                updated_at,
            ),
        )


def apply_event_to_projection(
    conn: sqlite3.Connection,
    *,
    stream_id: int,
    event: dict[str, Any],
    updated_at: str,
) -> None:
    """Idempotent per-event write used by the keep_prefix fate."""
    deleted = 1 if event["op"] == "delete" else 0
    payload = None if deleted else json.dumps(event.get("payload") or {}, ensure_ascii=False)
    conn.execute(
        """
        INSERT INTO projection_rows(
            stream_id, entity_key, payload_json, applied_seq, applied_event_id, deleted, updated_at
        ) VALUES (?,?,?,?,?,?,?)
        ON CONFLICT(stream_id, entity_key) DO UPDATE SET
            payload_json=excluded.payload_json,
            applied_seq=excluded.applied_seq,
            applied_event_id=excluded.applied_event_id,
            deleted=excluded.deleted,
            updated_at=excluded.updated_at
        """,
        (
            stream_id,
            event["entity_key"],
            payload,
            event["seq"],
            event["event_id"],
            deleted,
            updated_at,
        ),
    )


def list_projections(
    conn: sqlite3.Connection,
    *,
    stream_id: int | None = None,
    include_deleted: bool = True,
) -> list[dict[str, Any]]:
    sql = "SELECT * FROM projection_rows WHERE 1=1"
    params: list[Any] = []
    if stream_id is not None:
        sql += " AND stream_id=?"
        params.append(stream_id)
    if not include_deleted:
        sql += " AND deleted=0"
    sql += " ORDER BY stream_id, entity_key"
    out: list[dict[str, Any]] = []
    for r in conn.execute(sql, params).fetchall():
        d = dict(r)
        d["payload"] = json.loads(d.pop("payload_json")) if d["payload_json"] else None
        d.pop("payload_json", None)
        d["deleted"] = bool(d["deleted"])
        out.append(d)
    return out


def projection_digest(conn: sqlite3.Connection, stream_id: int) -> str:
    """Stable fingerprint of the persisted projection for a stream.

    Used to prove that a rollback restored the projection to its pre-run state
    byte-for-byte at the row level.
    """
    rows = conn.execute(
        """
        SELECT entity_key, payload_json, applied_seq, applied_event_id, deleted
        FROM projection_rows WHERE stream_id=?
        ORDER BY entity_key
        """,
        (stream_id,),
    ).fetchall()
    canonical = [
        [r["entity_key"], r["payload_json"], r["applied_seq"], r["applied_event_id"], r["deleted"]]
        for r in rows
    ]
    blob = json.dumps(canonical, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def projection_counts(conn: sqlite3.Connection, stream_id: int) -> dict[str, int]:
    alive = conn.execute(
        "SELECT COUNT(*) c FROM projection_rows WHERE stream_id=? AND deleted=0",
        (stream_id,),
    ).fetchone()["c"]
    deleted = conn.execute(
        "SELECT COUNT(*) c FROM projection_rows WHERE stream_id=? AND deleted=1",
        (stream_id,),
    ).fetchone()["c"]
    return {"projected_alive": alive, "projected_deleted": deleted}


def max_projected_seq(conn: sqlite3.Connection, stream_id: int) -> int:
    r = conn.execute(
        "SELECT MAX(applied_seq) m FROM projection_rows WHERE stream_id=?",
        (stream_id,),
    ).fetchone()
    return int(r["m"] or 0)


def list_known_event_ids(conn: sqlite3.Connection, stream_id: int) -> set[str]:
    rows = conn.execute(
        "SELECT DISTINCT event_id FROM applied_events WHERE stream_id=?",
        (stream_id,),
    ).fetchall()
    return {r["event_id"] for r in rows}


def list_ooo_buffer(conn: sqlite3.Connection, stream_id: int | None = None) -> list[dict[str, Any]]:
    if stream_id is None:
        rows = conn.execute("SELECT * FROM ooo_buffer ORDER BY id DESC").fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM ooo_buffer WHERE stream_id=? ORDER BY id DESC",
            (stream_id,),
        ).fetchall()
    out: list[dict[str, Any]] = []
    for r in rows:
        d = dict(r)
        d["payload"] = json.loads(d["payload_json"]) if d["payload_json"] else None
        d.pop("payload_json", None)
        out.append(d)
    return out
