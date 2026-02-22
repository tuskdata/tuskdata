"""Cluster mode — DEPRECATED

Cluster functionality has been moved to the external tusk-cluster plugin.
Install it with: pip install tusk-cluster

This module is kept only for backward compatibility and will be removed in v0.3.0.
"""

import warnings

warnings.warn(
    "tusk.cluster is deprecated. Install and use tusk-cluster plugin instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Backward-compatible imports (will raise ImportError if tusk-cluster not installed)
try:
    from tusk_cluster.models import Job, JobStatus, WorkerInfo, WorkerStatus, ClusterStatus
except ImportError:
    # Provide empty stubs so old code doesn't crash on import
    Job = None
    JobStatus = None
    WorkerInfo = None
    WorkerStatus = None
    ClusterStatus = None

Scheduler = None
Worker = None

__all__ = [
    "Job",
    "JobStatus",
    "WorkerInfo",
    "WorkerStatus",
    "ClusterStatus",
    "Scheduler",
    "Worker",
]
