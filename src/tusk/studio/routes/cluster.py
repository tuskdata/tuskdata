"""API routes for Cluster management"""

import asyncio
import subprocess
from datetime import datetime
from litestar import Controller, get, post, delete
from litestar.params import Body
import msgspec

from tusk.core.logging import get_logger

log = get_logger("cluster_api")

# In-memory cluster state (shared between requests)
# In a real deployment, this would connect to the actual scheduler
_cluster_state = {
    "scheduler": None,
    "workers": {},
    "jobs": {},
}

# Cluster connection config (persisted separately from active scheduler)
_cluster_config = {
    "scheduler_host": "localhost",
    "scheduler_port": 8814,
    "connected": False,
}

# Local cluster process (for single-node mode)
_local_cluster = {
    "process": None,
    "running": False,
}


class ClusterController(Controller):
    """API for Cluster management"""

    path = "/api/cluster"

    @get("/status")
    async def get_status(self) -> dict:
        """Get overall cluster status"""
        workers = list(_cluster_state["workers"].values())
        jobs = list(_cluster_state["jobs"].values())

        online_workers = sum(1 for w in workers if w.get("status") != "offline")
        active_jobs = sum(1 for j in jobs if j.get("status") == "running")
        completed_jobs = sum(1 for j in jobs if j.get("status") == "completed")

        return {
            "scheduler_online": _cluster_state["scheduler"] is not None,
            "scheduler_address": _cluster_state["scheduler"]["address"] if _cluster_state["scheduler"] else None,
            "workers_online": online_workers,
            "workers_total": len(workers),
            "active_jobs": active_jobs,
            "completed_jobs": completed_jobs,
            "total_bytes_processed": sum(w.get("bytes_processed", 0) for w in workers),
        }

    @get("/workers")
    async def list_workers(self) -> dict:
        """List all workers with metrics"""
        workers = []
        for worker_id, worker in _cluster_state["workers"].items():
            workers.append({
                "id": worker_id,
                "address": worker.get("address", "unknown"),
                "port": worker.get("port", 0),
                "status": worker.get("status", "offline"),
                "cpu_percent": worker.get("cpu_percent", 0),
                "memory_mb": worker.get("memory_mb", 0),
                "memory_percent": worker.get("memory_percent", 0),
                "last_heartbeat": worker.get("last_heartbeat"),
                "jobs_completed": worker.get("jobs_completed", 0),
                "bytes_processed": worker.get("bytes_processed", 0),
            })
        return {"workers": workers}

    @post("/workers/register")
    async def register_worker(self, data: dict = Body()) -> dict:
        """Register a worker (called by workers)"""
        worker_id = data.get("id")
        if not worker_id:
            return {"error": "Worker ID required"}

        _cluster_state["workers"][worker_id] = {
            "id": worker_id,
            "address": data.get("address", "localhost"),
            "port": data.get("port", 8815),
            "status": "idle",
            "cpu_percent": 0,
            "memory_mb": 0,
            "memory_percent": 0,
            "last_heartbeat": datetime.now().isoformat(),
            "jobs_completed": 0,
            "bytes_processed": 0,
        }

        log.info("Worker registered via API", worker_id=worker_id)
        return {"registered": True, "worker_id": worker_id}

    @post("/workers/{worker_id:str}/heartbeat")
    async def worker_heartbeat(self, worker_id: str, data: dict = Body()) -> dict:
        """Update worker metrics (called by workers)"""
        if worker_id not in _cluster_state["workers"]:
            return {"error": "Worker not found"}

        worker = _cluster_state["workers"][worker_id]
        worker["cpu_percent"] = data.get("cpu", 0)
        worker["memory_mb"] = data.get("memory", 0)
        worker["memory_percent"] = data.get("memory_percent", 0)
        worker["last_heartbeat"] = datetime.now().isoformat()
        worker["status"] = data.get("status", "idle")

        return {"ok": True}

    @post("/workers/{worker_id:str}/unregister")
    async def unregister_worker(self, worker_id: str) -> dict:
        """Unregister a worker"""
        if worker_id in _cluster_state["workers"]:
            del _cluster_state["workers"][worker_id]
            log.info("Worker unregistered via API", worker_id=worker_id)
            return {"unregistered": True}
        return {"error": "Worker not found"}

    @get("/jobs")
    async def list_jobs(self) -> dict:
        """List all jobs"""
        jobs = []
        for job_id, job in _cluster_state["jobs"].items():
            jobs.append({
                "id": job_id,
                "sql": job.get("sql", "")[:100],
                "status": job.get("status", "unknown"),
                "progress": job.get("progress", 0),
                "stages_total": job.get("stages_total", 1),
                "stages_completed": job.get("stages_completed", 0),
                "created_at": job.get("created_at"),
                "started_at": job.get("started_at"),
                "completed_at": job.get("completed_at"),
                "worker_id": job.get("worker_id"),
                "rows_processed": job.get("rows_processed", 0),
                "error": job.get("error"),
            })

        # Sort by created_at descending
        jobs.sort(key=lambda j: j.get("created_at", ""), reverse=True)
        return {"jobs": jobs}

    @get("/jobs/{job_id:str}")
    async def get_job(self, job_id: str) -> dict:
        """Get job details"""
        job = _cluster_state["jobs"].get(job_id)
        if not job:
            return {"error": "Job not found"}
        return job

    @post("/jobs")
    async def submit_job(self, data: dict = Body()) -> dict:
        """Submit a new job"""
        sql = data.get("sql")
        if not sql:
            return {"error": "SQL query required"}

        from uuid import uuid4
        job_id = str(uuid4())[:8]

        job = {
            "id": job_id,
            "sql": sql,
            "status": "pending",
            "progress": 0,
            "stages_total": 1,
            "stages_completed": 0,
            "created_at": datetime.now().isoformat(),
            "started_at": None,
            "completed_at": None,
            "worker_id": None,
            "rows_processed": 0,
            "error": None,
        }

        _cluster_state["jobs"][job_id] = job
        log.info("Job submitted via API", job_id=job_id)

        # In a real implementation, this would send to the scheduler
        # For now, simulate processing
        asyncio.create_task(_simulate_job(job_id))

        return {"job_id": job_id, "status": "pending"}

    @post("/jobs/{job_id:str}/cancel")
    async def cancel_job(self, job_id: str) -> dict:
        """Cancel a job"""
        job = _cluster_state["jobs"].get(job_id)
        if not job:
            return {"error": "Job not found"}

        if job["status"] in ("pending", "running"):
            job["status"] = "cancelled"
            job["completed_at"] = datetime.now().isoformat()
            job["error"] = "Cancelled by user"
            log.info("Job cancelled", job_id=job_id)
            return {"cancelled": True}

        return {"error": "Job cannot be cancelled (already completed)"}

    @post("/scheduler/register")
    async def register_scheduler(self, data: dict = Body()) -> dict:
        """Register scheduler (called by scheduler process)"""
        _cluster_state["scheduler"] = {
            "address": data.get("address", "localhost"),
            "port": data.get("port", 8814),
            "started_at": datetime.now().isoformat(),
        }
        log.info("Scheduler registered via API")
        return {"registered": True}

    @post("/scheduler/unregister")
    async def unregister_scheduler(self) -> dict:
        """Unregister scheduler"""
        _cluster_state["scheduler"] = None
        log.info("Scheduler unregistered via API")
        return {"unregistered": True}

    @get("/config")
    async def get_config(self) -> dict:
        """Get cluster connection config"""
        return {
            "scheduler_host": _cluster_config["scheduler_host"],
            "scheduler_port": _cluster_config["scheduler_port"],
            "connected": _cluster_config["connected"],
        }

    @post("/connect")
    async def connect_scheduler(self, data: dict = Body()) -> dict:
        """Connect to a remote scheduler"""
        host = data.get("host", "localhost")
        port = int(data.get("port", 8814))

        _cluster_config["scheduler_host"] = host
        _cluster_config["scheduler_port"] = port

        # Try to connect to the scheduler
        try:
            import pyarrow.flight as flight

            location = f"grpc://{host}:{port}"
            client = flight.connect(location)

            # Try a simple action to verify connection
            try:
                # List actions to verify scheduler is responsive
                list(client.list_actions())
                _cluster_config["connected"] = True

                # Register this connection as the active scheduler
                _cluster_state["scheduler"] = {
                    "address": f"{host}:{port}",
                    "port": port,
                    "started_at": datetime.now().isoformat(),
                }

                log.info("Connected to scheduler", host=host, port=port)
                return {"connected": True, "address": f"{host}:{port}"}
            except Exception as e:
                # Scheduler not running or not responding
                _cluster_config["connected"] = False
                log.warning("Scheduler not responding", host=host, port=port, error=str(e))
                return {"connected": False, "error": f"Scheduler at {host}:{port} not responding"}
            finally:
                client.close()

        except Exception as e:
            _cluster_config["connected"] = False
            log.error("Failed to connect to scheduler", host=host, port=port, error=str(e))
            return {"connected": False, "error": str(e)}

    @post("/disconnect")
    async def disconnect_scheduler(self) -> dict:
        """Disconnect from scheduler"""
        _cluster_config["connected"] = False
        _cluster_state["scheduler"] = None
        _cluster_state["workers"] = {}
        log.info("Disconnected from scheduler")
        return {"disconnected": True}

    @post("/refresh-workers")
    async def refresh_workers_from_scheduler(self) -> dict:
        """Fetch workers from connected scheduler"""
        if not _cluster_config["connected"]:
            return {"error": "Not connected to scheduler"}

        host = _cluster_config["scheduler_host"]
        port = _cluster_config["scheduler_port"]

        try:
            import pyarrow.flight as flight

            location = f"grpc://{host}:{port}"
            client = flight.connect(location)

            try:
                # Request worker list from scheduler
                action = flight.Action("list_workers", b"")
                results = list(client.do_action(action))

                if results:
                    import json
                    workers_data = json.loads(results[0].body.to_pybytes().decode())

                    # Update local worker state
                    _cluster_state["workers"] = {}
                    for w in workers_data.get("workers", []):
                        _cluster_state["workers"][w["id"]] = w

                    return {"refreshed": True, "workers": len(_cluster_state["workers"])}

                return {"refreshed": True, "workers": 0}

            finally:
                client.close()

        except Exception as e:
            log.error("Failed to refresh workers", error=str(e))
            return {"error": str(e)}

    @get("/local/status")
    async def get_local_status(self) -> dict:
        """Get local cluster status"""
        return {
            "running": _local_cluster["running"],
            "pid": _local_cluster["process"].pid if _local_cluster["process"] else None,
        }

    @post("/local/start")
    async def start_local_cluster(self, data: dict = Body()) -> dict:
        """Start a local single-node cluster (scheduler + worker in one process)"""
        if _local_cluster["running"]:
            return {"error": "Local cluster already running"}

        num_workers = data.get("workers", 1)

        try:
            import subprocess
            import sys

            # Start cluster using CLI command in background
            process = subprocess.Popen(
                [sys.executable, "-m", "tusk.cli", "cluster", "--workers", str(num_workers)],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                start_new_session=True,  # Detach from parent
            )

            _local_cluster["process"] = process
            _local_cluster["running"] = True

            # Wait a moment for scheduler to start
            await asyncio.sleep(1.5)

            # Register as connected
            _cluster_state["scheduler"] = {
                "address": "localhost:8814",
                "port": 8814,
                "started_at": datetime.now().isoformat(),
            }

            log.info("Local cluster started", pid=process.pid, workers=num_workers)
            return {"started": True, "pid": process.pid, "workers": num_workers}

        except Exception as e:
            log.error("Failed to start local cluster", error=str(e))
            return {"error": str(e)}

    @post("/local/stop")
    async def stop_local_cluster(self) -> dict:
        """Stop the local cluster"""
        if not _local_cluster["running"] or not _local_cluster["process"]:
            return {"error": "No local cluster running"}

        try:
            import os
            import signal

            process = _local_cluster["process"]
            pid = process.pid

            # Kill the process group (scheduler + workers)
            try:
                os.killpg(os.getpgid(pid), signal.SIGTERM)
            except ProcessLookupError:
                pass  # Already dead

            # Wait for termination
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(os.getpgid(pid), signal.SIGKILL)

            _local_cluster["process"] = None
            _local_cluster["running"] = False

            # Clear cluster state
            _cluster_state["scheduler"] = None
            _cluster_state["workers"] = {}

            log.info("Local cluster stopped", pid=pid)
            return {"stopped": True}

        except Exception as e:
            log.error("Failed to stop local cluster", error=str(e))
            return {"error": str(e)}


async def _simulate_job(job_id: str) -> None:
    """Simulate job execution (for demo purposes)"""
    import random

    await asyncio.sleep(0.5)

    job = _cluster_state["jobs"].get(job_id)
    if not job or job["status"] == "cancelled":
        return

    # Start job
    job["status"] = "running"
    job["started_at"] = datetime.now().isoformat()

    # Simulate progress
    for i in range(10):
        await asyncio.sleep(random.uniform(0.2, 0.5))

        job = _cluster_state["jobs"].get(job_id)
        if not job or job["status"] == "cancelled":
            return

        job["progress"] = (i + 1) / 10
        job["rows_processed"] = (i + 1) * random.randint(100, 1000)

    # Complete job
    job["status"] = "completed"
    job["completed_at"] = datetime.now().isoformat()
    job["progress"] = 1.0
    job["stages_completed"] = 1
