"""Plan an incremental projection run over a contiguous seq prefix.

Two mutually exclusive partial-failure fates consume this plan:

* ``rollback``   - all writes made by this run are undone with a SQLite
  savepoint; the batch fails as a whole and the watermark stays put.
* ``keep_prefix`` - only the contiguous run of successful seqs starting at
  ``watermark + 1`` survives; the watermark advances to the end of that
  prefix and the failed seq and everything after it are never marked
  successful.

The planner itself never touches storage: it walks candidate events in seq
order, stops at the first failure or seq hole, and reports what happened.
A seq hole is also terminal for the run: events beyond a gap cannot be
committed because the stream's ordering contract is not satisfied.
"""

from __future__ import annotations

from typing import Any, Iterable


def _failure(ev: dict[str, Any], reason: str, message: str) -> dict[str, Any]:
    return {
        "seq": int(ev.get("seq", 0)),
        "event_id": str(ev.get("event_id", "")),
        "entity_key": str(ev.get("entity_key", "")),
        "reason": reason,
        "message": message,
    }


def plan_apply(
    events: Iterable[dict[str, Any]],
    *,
    watermark: int,
    fail_at_seq: int | None = None,
    fail_event_id: str | None = None,
    known_event_ids: set[str] | None = None,
) -> dict[str, Any]:
    """Return the fate of every candidate event without writing anything.

    Candidates must be events with ``seq > watermark``; any lower-seq event is
    treated as stale and ignored before planning.
    """
    watermark = int(watermark)
    known = set(known_event_ids or ())
    candidates = sorted(
        (dict(e) for e in events if int(e.get("seq", 0)) > watermark),
        key=lambda e: (int(e.get("seq", 0)), str(e.get("event_id", ""))),
    )

    expected_seq = watermark + 1
    successes: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    failure: dict[str, Any] | None = None
    terminal_reason: str | None = None

    for idx, ev in enumerate(candidates):
        seq = int(ev.get("seq", 0))
        event_id = str(ev.get("event_id", ""))
        entity_key = str(ev.get("entity_key", ""))
        op = str(ev.get("op", "upsert"))

        if seq != expected_seq:
            # A hole in seq ordering. Never skip over it: everything from
            # here on stays unapplied, even if the fixture carries higher seqs.
            terminal_reason = "gap"
            blocked = candidates[idx:]
            break

        if fail_at_seq is not None and seq == fail_at_seq:
            failure = _failure(ev, "injected_failure", f"注入失败：seq {seq} 按请求失败")
            terminal_reason = "injected_failure"
            blocked = candidates[idx + 1 :]
            break
        if fail_event_id is not None and event_id == fail_event_id:
            failure = _failure(ev, "injected_failure", f"注入失败：事件 {event_id} 按请求失败")
            terminal_reason = "injected_failure"
            blocked = candidates[idx + 1 :]
            break

        if op not in ("upsert", "delete"):
            failure = _failure(ev, "invalid_op", f"不支持的操作 {op!r}")
            terminal_reason = "invalid_op"
            blocked = candidates[idx + 1 :]
            break
        if not entity_key:
            failure = _failure(ev, "invalid_entity_key", "事件缺少实体键")
            terminal_reason = "invalid_entity_key"
            blocked = candidates[idx + 1 :]
            break
        if op == "upsert" and ev.get("payload") is not None and not isinstance(ev["payload"], dict):
            failure = _failure(ev, "invalid_payload", "upsert 载荷必须是对象")
            terminal_reason = "invalid_payload"
            blocked = candidates[idx + 1 :]
            break

        duplicate = event_id in known
        if not duplicate:
            known.add(event_id)
        successes.append(
            {
                "seq": seq,
                "event_id": event_id,
                "entity_key": entity_key,
                "op": op,
                "event_time": str(ev.get("event_time", "")),
                "payload": ev.get("payload"),
                "duplicate": duplicate,
                "result": (
                    "skipped_duplicate"
                    if duplicate
                    else ("applied_delete" if op == "delete" else "applied_upsert")
                ),
            }
        )
        expected_seq = seq + 1

    resulting_watermark = expected_seq - 1
    applied_upsert = sum(1 for s in successes if s["result"] == "applied_upsert")
    applied_delete = sum(1 for s in successes if s["result"] == "applied_delete")
    skipped_duplicate = sum(1 for s in successes if s["result"] == "skipped_duplicate")
    attempted = len(successes) + (1 if failure else 0)

    return {
        "successes": successes,
        "failure": failure,
        "blocked": [
            {
                "seq": int(e.get("seq", 0)),
                "event_id": str(e.get("event_id", "")),
                "entity_key": str(e.get("entity_key", "")),
                "op": str(e.get("op", "upsert")),
                "event_time": str(e.get("event_time", "")),
            }
            for e in blocked
        ],
        "terminal_reason": terminal_reason,
        "source_events": len(candidates),
        "success_count": len(successes),
        "failure_count": 1 if failure else 0,
        "blocked_count": len(blocked),
        "attempted_count": attempted,
        "applied_upsert": applied_upsert,
        "applied_delete": applied_delete,
        "skipped_duplicate": skipped_duplicate,
        "surviving_watermark": resulting_watermark,
        "max_seq": max((int(e.get("seq", 0)) for e in candidates), default=watermark),
        "max_success_seq": successes[-1]["seq"] if successes else watermark,
        "fail_seq": failure["seq"] if failure else (blocked[0]["seq"] if blocked else None),
    }
