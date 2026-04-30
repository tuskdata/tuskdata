"""Background-job listing endpoints. Used by the topnav activity
indicator and the global toast poller in `tusk-jobs.js`.

Owner scoping: in multi-user mode, the user only sees jobs they own
plus jobs with NULL owner (system-wide, e.g. scheduled backups). In
single-user mode, _current_user_id returns ''; we treat that as
"return everything" so the activity drawer is useful.
"""

from litestar import Controller, get, Request

from tusk.core.jobs import get_registry
from tusk.studio.routes.base import _current_user_id


def _scope_owner_id(request: Request) -> str | None:
    """Map the empty-string single-user owner to None so the registry
    skips the owner filter and returns the full job list."""
    uid = _current_user_id(request)
    return uid if uid else None


class JobsController(Controller):
    path = "/api/jobs"

    @get("/")
    async def list_jobs(self, request: Request, limit: int = 50) -> dict:
        """Return up to `limit` recent jobs visible to the caller."""
        owner = _scope_owner_id(request)
        # Cap limit defensively — the drawer only renders ~25 anyway.
        limit = max(1, min(int(limit), 200))
        jobs = get_registry().list(owner, limit=limit)
        running = [j for j in jobs if j.status == "running"]
        return {
            "jobs": [
                {
                    "id": j.id,
                    "label": j.label,
                    "kind": j.kind,
                    "status": j.status,
                    "started_at": j.started_at,
                    "ended_at": j.ended_at,
                    "result": j.result,
                    "error": j.error,
                    "href": j.href,
                }
                for j in jobs
            ],
            "running_count": len(running),
        }

    @get("/{job_id:str}")
    async def get_job(self, request: Request, job_id: str) -> dict:
        owner = _scope_owner_id(request)
        job = get_registry().get(job_id)
        if job is None:
            return {"error": "Job not found", "id": job_id}
        # Defense in depth: in multi-user mode, refuse cross-owner reads.
        if owner is not None and job.owner_id and job.owner_id != owner:
            return {"error": "Job not found", "id": job_id}
        return {
            "id": job.id,
            "label": job.label,
            "kind": job.kind,
            "status": job.status,
            "started_at": job.started_at,
            "ended_at": job.ended_at,
            "result": job.result,
            "error": job.error,
            "href": job.href,
        }
