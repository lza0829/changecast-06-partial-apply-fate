from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services import project_service as svc

router = APIRouter(tags=["batches"])


class RunBody(BaseModel):
    stream_id: int
    note: str | None = None
    policy: str | None = None
    fail_at_seq: int | None = None
    fail_event_id: str | None = None
    skip_seq: int | None = None


class ResetBody(BaseModel):
    stream_id: int


@router.get("/batches")
def list_batches(limit: int = 50):
    return svc.list_batches(limit=limit)


@router.get("/batches/{batch_id}")
def get_batch(batch_id: int):
    batch = svc.get_batch_detail(batch_id)
    if batch is None:
        raise HTTPException(404, "batch not found")
    return batch


@router.post("/project")
def run_project(body: RunBody):
    try:
        return svc.run_project(
            body.stream_id,
            body.note,
            policy=body.policy,
            fail_at_seq=body.fail_at_seq,
            fail_event_id=body.fail_event_id,
            skip_seq=body.skip_seq,
        )
    except ValueError as e:
        raise HTTPException(400, str(e)) from e


@router.post("/streams/{stream_id}/reset")
def reset_stream(stream_id: int):
    """Demo helper: return a stream to the pre-run state (watermark 0, no rows)."""
    try:
        return svc.reset_stream(stream_id)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
