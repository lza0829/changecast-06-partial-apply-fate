from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

import pytest


@pytest.fixture()
def svc(tmp_path, monkeypatch):
    """Fresh module state with an isolated SQLite file per test."""
    monkeypatch.setenv("DATA_DIR", str(tmp_path))
    # Drop cached modules so config/db re-read DATA_DIR.
    for name in [
        "app.config",
        "app.db",
        "app.seed",
        "app.repositories",
        "app.repositories.batches",
        "app.repositories.projections",
        "app.repositories.streams",
        "app.engines",
        "app.engines.project",
        "app.services",
        "app.services.project_service",
    ]:
        sys.modules.pop(name, None)
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    seed = importlib.import_module("app.seed")
    seed.init_db()
    return importlib.import_module("app.services.project_service")


def _counts(svc, stream_id):
    return svc.list_projections(stream_id=stream_id)


def _bad(checks):
    return [c for c in checks if not c["ok"]]


def test_all_success_under_both_policies(svc):
    for policy in ("rollback", "keep_prefix"):
        svc.reset_stream(1)
        batch = svc.run_project(1, policy=policy)
        assert batch["status"] == "completed"
        assert batch["success_count"] == 6
        assert batch["failure_count"] == 0
        assert batch["watermark_before"] == 0
        assert batch["watermark_after"] == 6
        assert batch["watermark_now"] == 6
        assert _bad(batch["checks"]) == []
        # counters and ledger agree
        assert batch["applied_upsert"] + batch["applied_delete"] == 6
        assert len(batch["events"]) == 6


def test_rollback_failure_leaves_projection_identical(svc):
    svc.reset_stream(2)
    before = json.dumps(_counts(svc, 2), sort_keys=True, ensure_ascii=False)

    batch = svc.run_project(2, policy="rollback", fail_at_seq=3)
    after = json.dumps(_counts(svc, 2), sort_keys=True, ensure_ascii=False)

    assert batch["status"] == "failed"
    assert batch["rolled_back"] == 1
    assert batch["success_count"] == 0
    assert batch["discarded_count"] == 2  # seq 1,2 planned then undone
    assert batch["events"] == []  # no half-written ledger survives
    assert before == after  # projection identical to pre-run
    assert batch["projection_before_digest"] == batch["projection_after_digest"]
    assert batch["watermark_after"] == batch["watermark_before"] == 0
    assert _bad(batch["checks"]) == []


def test_rollback_failure_preserves_prior_projection(svc):
    svc.reset_stream(2)
    first = svc.run_project(2, policy="rollback", fail_at_seq=3)  # wm stays 0
    svc.run_project(2, policy="keep_prefix", fail_at_seq=4)  # commit seq 1..3? fail@4 -> keep 1..3
    before = _counts(svc, 2)
    assert before  # there is committed state now

    batch = svc.run_project(2, policy="rollback", fail_at_seq=5)  # fail on the next event
    after = _counts(svc, 2)
    assert batch["rolled_back"] == 1
    assert before == after
    assert batch["watermark_after"] == batch["watermark_before"]
    assert _bad(batch["checks"]) == []


def test_keep_prefix_stops_at_failure(svc):
    svc.reset_stream(3)
    batch = svc.run_project(3, policy="keep_prefix", fail_at_seq=4)

    assert batch["status"] == "failed"
    assert batch["rolled_back"] == 0
    assert batch["success_count"] == 3  # ch-001..003
    assert batch["failure_count"] == 1
    assert batch["fail_seq"] == 4
    assert batch["watermark_after"] == 3
    assert batch["watermark_after"] < batch["fail_seq"]
    assert batch["watermark_now"] == 3
    assert [e["seq"] for e in batch["events"]] == [1, 2, 3]
    assert all(e["result"] != "skipped_duplicate" for e in batch["events"])
    assert _bad(batch["checks"]) == []


def test_keep_prefix_never_crosses_gap(svc):
    svc.reset_stream(1)
    batch = svc.run_project(1, policy="keep_prefix", skip_seq=4)

    assert batch["status"] == "failed"
    assert batch["success_count"] == 3  # only 1..3 contiguous
    # fixture has seq 5 and 6 but the run must not apply them past the hole
    assert [e["seq"] for e in batch["events"]] == [1, 2, 3]
    assert batch["watermark_after"] == 3
    assert batch["blocked_count"] >= 1
    assert _bad(batch["checks"]) == []


def test_rollback_on_gap_does_not_advance(svc):
    svc.reset_stream(1)
    batch = svc.run_project(1, policy="rollback", skip_seq=4)
    assert batch["status"] == "failed"
    assert batch["rolled_back"] == 1
    assert batch["success_count"] == 0
    assert batch["watermark_after"] == 0
    assert _counts(svc, 1) == []
    assert _bad(batch["checks"]) == []


def test_incremental_runs_do_not_double_apply(svc):
    svc.reset_stream(1)
    svc.run_project(1, policy="keep_prefix", fail_at_seq=4)  # wm 3
    second = svc.run_project(1, policy="keep_prefix")  # resumes at 4
    assert second["status"] == "completed"
    assert second["watermark_before"] == 3
    assert second["watermark_after"] == 6
    assert [e["seq"] for e in second["events"]] == [4, 5, 6]
    assert _bad(second["checks"]) == []
    # a third run has nothing left to do
    third = svc.run_project(1, policy="rollback")
    assert third["status"] == "completed"
    assert third["success_count"] == 0
    assert third["watermark_after"] == 6
    assert _bad(third["checks"]) == []


def test_fail_event_id_injection(svc):
    svc.reset_stream(1)
    batch = svc.run_project(1, policy="keep_prefix", fail_event_id="mp-003")
    assert batch["fail_seq"] == 3
    assert batch["fail_event_id"] == "mp-003"
    assert batch["success_count"] == 2
    assert batch["watermark_after"] == 2
    assert _bad(batch["checks"]) == []


@pytest.mark.parametrize("policy", ["rollback", "keep_prefix"])
def test_settings_roundtrip(svc, policy):
    settings = svc.update_settings({"partial_failure_policy": policy})
    assert settings["partial_failure_policy"] == policy
    with pytest.raises(ValueError):
        svc.update_settings({"partial_failure_policy": "nope"})
    with pytest.raises(ValueError):
        svc.update_settings({"unknown_key": "x"})


def test_batch_no_unique_across_resets(svc):
    svc.reset_stream(1)
    for _ in range(3):
        svc.run_project(1, policy="rollback")
        svc.reset_stream(1)
    # reset_stream drops the stream's batches, so this mainly proves the
    # allocator does not collide on COUNT(*) after deletions elsewhere.
    svc.reset_stream(2)
    svc.run_project(1, policy="rollback")
    svc.run_project(2, policy="rollback")
    ids = [b["batch_no"] for b in svc.list_batches(limit=100)]
    assert len(ids) == len(set(ids))
