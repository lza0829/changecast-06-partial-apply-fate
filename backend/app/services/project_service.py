from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import sqlite3

from app.config import FIXTURES_DIR
from app.db import connect
from app.engines.project import plan_apply
from app.repositories import batches as batch_repo
from app.repositories import projections as proj_repo
from app.repositories import streams as stream_repo

POLICIES = ("rollback", "keep_prefix")


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load_fixture(name: str) -> list[dict[str, Any]]:
    path = FIXTURES_DIR / name
    with path.open(encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"fixture {name} must be a JSON array")
    return data


def _setting(conn: sqlite3.Connection, key: str) -> str:
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else ""


def dashboard_stats() -> dict[str, Any]:
    conn = connect()
    try:
        return {
            "stream_count": conn.execute("SELECT COUNT(*) c FROM project_streams").fetchone()["c"],
            "enabled_streams": conn.execute(
                "SELECT COUNT(*) c FROM project_streams WHERE enabled=1"
            ).fetchone()["c"],
            "batch_count": conn.execute("SELECT COUNT(*) c FROM project_batches").fetchone()["c"],
            "failed_batches": conn.execute(
                "SELECT COUNT(*) c FROM project_batches WHERE status != 'completed'"
            ).fetchone()["c"],
            "projection_alive": conn.execute(
                "SELECT COUNT(*) c FROM projection_rows WHERE deleted=0"
            ).fetchone()["c"],
            "projection_deleted": conn.execute(
                "SELECT COUNT(*) c FROM projection_rows WHERE deleted=1"
            ).fetchone()["c"],
            "ooo_buffered": conn.execute("SELECT COUNT(*) c FROM ooo_buffer").fetchone()["c"],
            "apply_order": _setting(conn, "apply_order"),
            "partial_failure_policy": _setting(conn, "partial_failure_policy"),
        }
    finally:
        conn.close()


def list_streams() -> list[dict[str, Any]]:
    conn = connect()
    try:
        return stream_repo.list_streams(conn)
    finally:
        conn.close()


def _verify_batch(
    conn: sqlite3.Connection, batch: dict[str, Any], *, is_latest: bool
) -> list[dict[str, Any]]:
    """Mutual checks: counters vs ledger vs watermark vs projection.

    Live-state checks (watermark "now", current projection) only make sense
    for the stream's latest batch; older batches are verified against the
    values recorded at the moment they ran.
    """
    trace = batch_repo.count_results(conn, batch["id"])
    ledger_rows = sum(trace.values())
    max_projected = proj_repo.max_projected_seq(conn, batch["stream_id"])
    checks: list[dict[str, Any]] = []

    def check(name: str, ok: bool, expected: Any, actual: Any, note: str) -> None:
        checks.append(
            {"name": name, "ok": bool(ok), "expected": expected, "actual": actual, "note": note}
        )

    check(
        "成功数 = 留痕行数",
        batch["success_count"] == ledger_rows,
        batch["success_count"],
        ledger_rows,
        "成功数以事件留痕实际写入为准",
    )
    check(
        "写入+删除+跳过 = 留痕",
        batch["applied_upsert"] + batch["applied_delete"] + batch["skipped_duplicate"] == ledger_rows,
        batch["applied_upsert"] + batch["applied_delete"] + batch["skipped_duplicate"],
        ledger_rows,
        "三类结果之和必须等于留痕总数",
    )
    if batch["rolled_back"]:
        check(
            "回滚后投影与开跑前一致",
            batch["projection_after_digest"] == batch["projection_before_digest"],
            batch["projection_before_digest"],
            batch["projection_after_digest"],
            "投影摘要（实体键+载荷+seq+删除态）逐行一致",
        )
        check(
            "回滚后水位不前进",
            batch["watermark_after"] == batch["watermark_before"],
            batch["watermark_before"],
            batch["watermark_after"],
            "整批失败，水位保持开跑前",
        )
        check(
            "回滚后本趟留痕为空",
            ledger_rows == 0,
            batch["success_count"],
            ledger_rows,
            "本趟成功写入的留痕一并撤销",
        )
        if is_latest:
            check(
                "水位现状未越过失败点",
                batch["watermark_now"] == batch["watermark_before"],
                batch["watermark_before"],
                batch["watermark_now"],
                "库内当前水位",
            )
    else:
        check(
            "水位停在连续成功前缀末",
            batch["watermark_after"] == batch["watermark_before"] + batch["success_count"],
            batch["watermark_before"] + batch["success_count"],
            batch["watermark_after"],
            "按 seq 连续，每个成功事件推进一格",
        )
        if batch["fail_seq"] is not None:
            check(
                "水位不跨失败 seq",
                batch["watermark_after"] < batch["fail_seq"],
                f"< {batch['fail_seq']}",
                batch["watermark_after"],
                "失败事件及其后不得标成功，水位不得越过失败点",
            )
        if is_latest:
            check(
                "水位现状与前缀一致",
                batch["watermark_now"] == batch["watermark_after"],
                batch["watermark_after"],
                batch["watermark_now"],
                "库内当前水位",
            )
            check(
                "投影最大 seq ≤ 水位",
                max_projected <= batch["watermark_now"],
                batch["watermark_now"],
                max_projected,
                "投影中任何行都不得带水位之后的 seq",
            )
        if batch["fail_seq"] is not None:
            changed = batch["projection_after_digest"] != batch["projection_before_digest"]
            prefix_written = (
                changed if batch["success_count"] > 0
                else batch["projection_after_digest"] == batch["projection_before_digest"]
            )
            check(
                "失败趟投影只含成功前缀",
                prefix_written,
                "有成功前缀→投影变化；无成功前缀→与开跑前一致",
                f"success={batch['success_count']} changed={changed}",
                "前缀之外不得留下任何投影写入",
            )
    expected_failures = 1 if batch["fail_event_id"] else 0
    check(
        "失败数口径",
        batch["failure_count"] == expected_failures,
        expected_failures,
        batch["failure_count"],
        "每趟至多一个首个失败事件；seq 缺口截停不算事件失败",
    )
    check(
        "成功+失败+回滚丢弃 = 尝试处理",
        batch["success_count"]
        + batch["discarded_count"]
        + batch["failure_count"]
        == batch["attempted_count"],
        batch["attempted_count"],
        batch["success_count"] + batch["discarded_count"] + batch["failure_count"],
        "尝试处理数只含成功、回滚撤销与首个失败，被截停的不尝试",
    )
    return checks


def _is_latest_batch(conn: sqlite3.Connection, batch: dict[str, Any]) -> bool:
    r = conn.execute(
        "SELECT MAX(id) m FROM project_batches WHERE stream_id=?", (batch["stream_id"],)
    ).fetchone()
    return int(r["m"] or 0) == int(batch["id"])


def _decorate(conn: sqlite3.Connection, batch: dict[str, Any], streams: dict[int, dict]) -> dict[str, Any]:
    s = streams.get(batch["stream_id"])
    batch["stream_code"] = s["code"] if s else None
    batch["stream_title"] = s["title"] if s else None
    batch["watermark_now"] = s["watermark_seq"] if s else None
    batch["is_latest"] = _is_latest_batch(conn, batch)
    batch["trace"] = batch_repo.count_results(conn, batch["id"])
    batch["checks"] = _verify_batch(conn, batch, is_latest=batch["is_latest"])
    batch["checks_ok"] = all(c["ok"] for c in batch["checks"])
    return batch


def list_batches(limit: int = 50) -> list[dict[str, Any]]:
    conn = connect()
    try:
        streams = {s["id"]: s for s in stream_repo.list_streams(conn)}
        rows = batch_repo.list_batches(conn, limit=limit)
        return [_decorate(conn, r, streams) for r in rows]
    finally:
        conn.close()


def get_batch_detail(batch_id: int) -> dict[str, Any] | None:
    conn = connect()
    try:
        batch = batch_repo.get_batch(conn, batch_id)
        if not batch:
            return None
        streams = {s["id"]: s for s in stream_repo.list_streams(conn)}
        _decorate(conn, batch, streams)
        batch["events"] = batch_repo.list_applied_events(conn, batch_id=batch_id, limit=500)
        batch["projection_now_digest"] = proj_repo.projection_digest(conn, batch["stream_id"])
        batch["counts"] = {
            "source_events": batch["source_events"],
            "applied_upsert": batch["applied_upsert"],
            "applied_delete": batch["applied_delete"],
            "skipped_duplicate": batch["skipped_duplicate"],
            "projected_alive": batch["projected_alive"],
            "projected_deleted": batch["projected_deleted"],
            "max_seq": batch["max_seq"],
            "success_count": batch["success_count"],
            "failure_count": batch["failure_count"],
            "blocked_count": batch["blocked_count"],
            "attempted_count": batch["attempted_count"],
        }
        return batch
    finally:
        conn.close()


def list_projections(stream_id: int | None = None, include_deleted: bool = True) -> list[dict[str, Any]]:
    conn = connect()
    try:
        rows = proj_repo.list_projections(
            conn, stream_id=stream_id, include_deleted=include_deleted
        )
        streams = {s["id"]: s for s in stream_repo.list_streams(conn)}
        for r in rows:
            s = streams.get(r["stream_id"])
            r["stream_code"] = s["code"] if s else None
            r["stream_title"] = s["title"] if s else None
        return rows
    finally:
        conn.close()


def list_events(batch_id: int | None = None, stream_id: int | None = None) -> list[dict[str, Any]]:
    conn = connect()
    try:
        return batch_repo.list_applied_events(conn, batch_id=batch_id, stream_id=stream_id)
    finally:
        conn.close()


def list_ooo_buffer(stream_id: int | None = None) -> list[dict[str, Any]]:
    conn = connect()
    try:
        return proj_repo.list_ooo_buffer(conn, stream_id=stream_id)
    finally:
        conn.close()


def list_settings() -> dict[str, str]:
    conn = connect()
    try:
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
        return {r["key"]: r["value"] for r in rows}
    finally:
        conn.close()


def update_settings(patch: dict[str, str]) -> dict[str, str]:
    allowed = {"partial_failure_policy", "tombstone_mode", "duplicate_policy", "apply_order"}
    unknown = set(patch) - allowed
    if unknown:
        raise ValueError(f"unknown setting(s): {', '.join(sorted(unknown))}")
    if "partial_failure_policy" in patch and patch["partial_failure_policy"] not in POLICIES:
        raise ValueError("partial_failure_policy must be rollback or keep_prefix")
    conn = connect()
    try:
        for key, value in patch.items():
            conn.execute(
                "INSERT INTO settings(key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
        conn.commit()
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
        return {r["key"]: r["value"] for r in rows}
    finally:
        conn.close()


def reset_stream(stream_id: int) -> dict[str, Any]:
    """Demo helper: return a stream to its pre-run state.

    Clears the stream's projection, event ledger and batch history and zeroes
    the watermark so the success and failure scenarios can be replayed from
    the same starting point.
    """
    conn = connect()
    try:
        stream = stream_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        batch_ids = [
            r["id"]
            for r in conn.execute(
                "SELECT id FROM project_batches WHERE stream_id=?", (stream_id,)
            ).fetchall()
        ]
        conn.execute("DELETE FROM applied_events WHERE stream_id=?", (stream_id,))
        for bid in batch_ids:
            conn.execute("DELETE FROM project_batches WHERE id=?", (bid,))
        conn.execute("DELETE FROM projection_rows WHERE stream_id=?", (stream_id,))
        conn.execute("DELETE FROM ooo_buffer WHERE stream_id=?", (stream_id,))
        stream_repo.set_watermark(conn, stream_id, 0)
        conn.commit()
        return {
            "stream_id": stream_id,
            "watermark_seq": 0,
            "cleared_batches": len(batch_ids),
        }
    finally:
        conn.close()


def run_project(
    stream_id: int,
    note: str | None = None,
    *,
    policy: str | None = None,
    fail_at_seq: int | None = None,
    fail_event_id: str | None = None,
    skip_seq: int | None = None,
) -> dict[str, Any]:
    """Apply the fixture incrementally under one of two exclusive fates.

    Every run is a single SQLite transaction. ``rollback`` unwinds the
    SAVEPOINT covering projection rows and ledger rows, so the stream is
    exactly as it was before the run and the watermark does not move.
    ``keep_prefix`` writes only the contiguous seq prefix that succeeded and
    advances the watermark no further than its last seq.

    ``fail_at_seq`` / ``fail_event_id`` inject a mid-run failure at that
    event. ``skip_seq`` removes that seq from the candidate stream to
    simulate an upstream hole, so the run must stop before the gap even
    though the fixture carries higher seqs.
    """
    conn = connect()
    try:
        stream = stream_repo.get_stream(conn, stream_id)
        if not stream:
            raise ValueError("stream not found")
        if not stream["enabled"]:
            raise ValueError("stream disabled")
        chosen = policy or _setting(conn, "partial_failure_policy") or "rollback"
        if chosen not in POLICIES:
            raise ValueError(f"policy must be one of {POLICIES}")

        watermark_before = int(stream["watermark_seq"])
        fixture = _load_fixture(stream["fixture"])
        if fail_at_seq is not None and int(fail_at_seq) <= watermark_before:
            raise ValueError(
                f"注入点 seq {fail_at_seq} 不在本趟处理范围（水位已到 {watermark_before}）"
            )
        if fail_event_id is not None and not any(
            str(e.get("event_id")) == fail_event_id and int(e.get("seq", 0)) > watermark_before
            for e in fixture
        ):
            raise ValueError(
                f"注入事件 {fail_event_id} 不在本趟处理范围（水位已到 {watermark_before}）"
            )
        if skip_seq is not None and int(skip_seq) <= watermark_before:
            raise ValueError(
                f"缺口 seq {skip_seq} 不在本趟处理范围（水位已到 {watermark_before}）"
            )
        if skip_seq is not None:
            fixture = [e for e in fixture if int(e.get("seq", 0)) != int(skip_seq)]
        plan = plan_apply(
            fixture,
            watermark=watermark_before,
            fail_at_seq=fail_at_seq,
            fail_event_id=fail_event_id,
            known_event_ids=proj_repo.list_known_event_ids(conn, stream_id),
        )

        now = _now()
        batch_seq = conn.execute("SELECT MAX(id) m FROM project_batches").fetchone()["m"] or 0
        while True:
            batch_seq += 1
            batch_no = f"PB-{now[:10].replace('-', '')}-{batch_seq:04d}"
            if not conn.execute(
                "SELECT 1 FROM project_batches WHERE batch_no=?", (batch_no,)
            ).fetchone():
                break
        digest_before = proj_repo.projection_digest(conn, stream_id)
        counts_before = proj_repo.projection_counts(conn, stream_id)

        failure = plan["failure"]
        gap = plan["terminal_reason"] == "gap"
        fatal = failure is not None or gap
        status = "failed" if fatal else "completed"
        error_message = failure["message"] if failure else ("seq 缺口，本趟不得越过" if gap else None)

        # Batch row lives in the outer transaction (it is the durable record
        # of the attempt even if the write set unwinds). Projection + ledger
        # writes happen inside one SAVEPOINT whose fate the policy decides.
        batch_id = batch_repo.insert_batch(
            conn,
            metrics={
                "batch_no": batch_no,
                "stream_id": stream_id,
                "status": status,
                "source_events": plan["source_events"],
                "applied_upsert": plan["applied_upsert"],
                "applied_delete": plan["applied_delete"],
                "skipped_duplicate": plan["skipped_duplicate"],
                "projected_alive": counts_before["projected_alive"],
                "projected_deleted": counts_before["projected_deleted"],
                "max_seq": plan["max_success_seq"],
                "note": note,
                "created_at": now,
                "outcome_policy": chosen,
                "success_count": plan["success_count"],
                "failure_count": plan["failure_count"],
                "blocked_count": plan["blocked_count"],
                "attempted_count": plan["attempted_count"],
                "discarded_count": 0,
                "rolled_back": 0,
                "fail_seq": plan["fail_seq"] if fatal else None,
                "fail_event_id": failure["event_id"] if failure else None,
                "watermark_before": watermark_before,
                "watermark_after": plan["surviving_watermark"],
                "error_message": error_message,
                "projection_before_digest": digest_before,
            },
        )

        conn.execute("SAVEPOINT apply_run")
        try:
            for ev in plan["successes"]:
                if ev["result"] != "skipped_duplicate":
                    proj_repo.apply_event_to_projection(
                        conn, stream_id=stream_id, event=ev, updated_at=now
                    )
                batch_repo.insert_applied_event(
                    conn,
                    batch_id=batch_id,
                    stream_id=stream_id,
                    event=ev,
                    created_at=now,
                )

            if chosen == "rollback" and fatal:
                # The prefix really was written; the whole write set is then
                # unwound so the stream is byte-for-byte as it was pre-run.
                conn.execute("ROLLBACK TO SAVEPOINT apply_run")
                batch_repo.update_batch(
                    conn,
                    batch_id,
                    {
                        "applied_upsert": 0,
                        "applied_delete": 0,
                        "skipped_duplicate": 0,
                        "projected_alive": counts_before["projected_alive"],
                        "projected_deleted": counts_before["projected_deleted"],
                        "max_seq": watermark_before,
                        "success_count": 0,
                        "discarded_count": plan["success_count"],
                        "rolled_back": 1,
                        "watermark_after": watermark_before,
                        "projection_after_digest": digest_before,
                    },
                )
            else:
                stream_repo.set_watermark(conn, stream_id, plan["surviving_watermark"])
                counts_after = proj_repo.projection_counts(conn, stream_id)
                batch_repo.update_batch(
                    conn,
                    batch_id,
                    {
                        "projected_alive": counts_after["projected_alive"],
                        "projected_deleted": counts_after["projected_deleted"],
                        "projection_after_digest": proj_repo.projection_digest(
                            conn, stream_id
                        ),
                    },
                )
            conn.execute("RELEASE SAVEPOINT apply_run")
            conn.commit()
        except Exception:
            conn.execute("ROLLBACK TO SAVEPOINT apply_run")
            conn.execute("RELEASE SAVEPOINT apply_run")
            conn.rollback()
            raise

        detail = get_batch_detail(batch_id)
        assert detail is not None
        return detail
    finally:
        conn.close()
