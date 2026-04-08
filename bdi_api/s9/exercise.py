from datetime import datetime

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

s9 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s9",
    tags=["s9"],
)


class PipelineRun(BaseModel):
    id: str
    repository: str
    branch: str
    status: str
    triggered_by: str
    started_at: datetime
    finished_at: datetime | None
    stages: list[str]


class PipelineStage(BaseModel):
    name: str
    status: str
    started_at: datetime
    finished_at: datetime | None
    logs_url: str


PIPELINES = [
    {
        "id": "run-001",
        "repository": "bts-bdp-assignment",
        "branch": "main",
        "status": "success",
        "triggered_by": "push",
        "started_at": "2026-04-08T10:00:00Z",
        "finished_at": "2026-04-08T10:05:30Z",
        "stages": ["lint", "test", "build"],
    },
    {
        "id": "run-002",
        "repository": "bts-bdp-assignment",
        "branch": "feat/add-s7",
        "status": "success",
        "triggered_by": "pull_request",
        "started_at": "2026-04-07T14:00:00Z",
        "finished_at": "2026-04-07T14:03:20Z",
        "stages": ["lint", "test", "build"],
    },
    {
        "id": "run-003",
        "repository": "bts-bdp-assignment",
        "branch": "main",
        "status": "failure",
        "triggered_by": "push",
        "started_at": "2026-04-06T09:00:00Z",
        "finished_at": "2026-04-06T09:02:10Z",
        "stages": ["lint", "test"],
    },
    {
        "id": "run-004",
        "repository": "bts-bdp-exercises",
        "branch": "main",
        "status": "success",
        "triggered_by": "push",
        "started_at": "2026-04-05T16:00:00Z",
        "finished_at": "2026-04-05T16:04:00Z",
        "stages": ["lint", "test", "build", "deploy"],
    },
    {
        "id": "run-005",
        "repository": "bts-bdp-assignment",
        "branch": "main",
        "status": "success",
        "triggered_by": "schedule",
        "started_at": "2026-04-04T02:00:00Z",
        "finished_at": "2026-04-04T02:06:00Z",
        "stages": ["lint", "test", "build"],
    },
    {
        "id": "run-006",
        "repository": "bts-bdp-assignment",
        "branch": "feat/add-s8",
        "status": "success",
        "triggered_by": "pull_request",
        "started_at": "2026-04-03T11:00:00Z",
        "finished_at": "2026-04-03T11:04:50Z",
        "stages": ["lint", "test", "build"],
    },
    {
        "id": "run-007",
        "repository": "bts-bdp-assignment",
        "branch": "main",
        "status": "running",
        "triggered_by": "manual",
        "started_at": "2026-04-02T08:00:00Z",
        "finished_at": None,
        "stages": ["lint", "test", "build"],
    },
    {
        "id": "run-008",
        "repository": "bts-bdp-exercises",
        "branch": "main",
        "status": "success",
        "triggered_by": "push",
        "started_at": "2026-04-01T20:00:00Z",
        "finished_at": "2026-04-01T20:03:30Z",
        "stages": ["lint", "test"],
    },
    {
        "id": "run-009",
        "repository": "bts-bdp-assignment",
        "branch": "main",
        "status": "pending",
        "triggered_by": "push",
        "started_at": "2026-03-31T12:00:00Z",
        "finished_at": None,
        "stages": ["lint", "test", "build"],
    },
    {
        "id": "run-010",
        "repository": "bts-bdp-assignment",
        "branch": "feat/add-s6",
        "status": "success",
        "triggered_by": "pull_request",
        "started_at": "2026-03-30T15:00:00Z",
        "finished_at": "2026-03-30T15:05:00Z",
        "stages": ["lint", "test", "build"],
    },
]

STAGES_DB = {}
for p in PIPELINES:
    pid = p["id"]
    base_time = datetime.fromisoformat(p["started_at"].replace("Z", "+00:00"))
    stages = []
    for i, stage_name in enumerate(p["stages"]):
        from datetime import timedelta
        s_start = base_time + timedelta(seconds=i * 45)
        s_end = base_time + timedelta(seconds=(i + 1) * 45) if p["finished_at"] else None
        if p["status"] == "failure" and stage_name == p["stages"][-1]:
            s_status = "failure"
        elif p["status"] == "running" and stage_name == p["stages"][-1]:
            s_status = "running"
            s_end = None
        elif p["status"] == "pending":
            s_status = "pending"
            s_end = None
        else:
            s_status = "success"
        stages.append({
            "name": stage_name,
            "status": s_status,
            "started_at": s_start.isoformat(),
            "finished_at": s_end.isoformat() if s_end else None,
            "logs_url": f"/api/s9/pipelines/{pid}/stages/{stage_name}/logs",
        })
    STAGES_DB[pid] = stages


@s9.get("/pipelines")
def list_pipelines(
    repository: str | None = None,
    status_filter: str | None = None,
    num_results: int = 100,
    page: int = 0,
) -> list[PipelineRun]:
    """List CI/CD pipeline runs."""
    results = PIPELINES[:]

    if repository:
        results = [p for p in results if p["repository"] == repository]
    if status_filter:
        results = [p for p in results if p["status"] == status_filter]

    results.sort(key=lambda p: p["started_at"], reverse=True)

    offset = page * num_results
    return results[offset : offset + num_results]


@s9.get("/pipelines/{pipeline_id}/stages")
def get_pipeline_stages(pipeline_id: str) -> list[PipelineStage]:
    """Get stages of a pipeline run."""
    if pipeline_id not in STAGES_DB:
        raise HTTPException(status_code=404, detail=f"Pipeline '{pipeline_id}' not found")
    return STAGES_DB[pipeline_id]
