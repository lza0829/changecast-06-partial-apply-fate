from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services import project_service as svc

router = APIRouter(tags=["settings"])


class SettingsPatch(BaseModel):
    partial_failure_policy: str | None = None


@router.get("/settings")
def list_settings():
    return svc.list_settings()


@router.put("/settings")
def update_settings(patch: SettingsPatch):
    values = {k: v for k, v in patch.model_dump().items() if v is not None}
    try:
        return svc.update_settings(values)
    except ValueError as e:
        raise HTTPException(400, str(e)) from e
