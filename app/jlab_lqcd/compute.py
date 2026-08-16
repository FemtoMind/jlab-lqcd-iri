"""Compute and job management API functions for JLab LQCD adapter."""

import datetime
import random

from ..routers.compute import models as compute_models
from ..routers.status import models as status_models
from ..types.user import User
from ..request_context import get_iri_facility_project
from ..utils import demo_uuid


def utc_timestamp() -> int:
    """Return current UTC datetime timestamp as integer."""
    return int(datetime.datetime.now(datetime.timezone.utc).timestamp())


async def submit_job(
    adapter,
    resource: status_models.Resource,
    user: User,
    job_spec: compute_models.JobSpec,
) -> compute_models.Job:
    """Submit a job to the compute resource (Slurm)."""
    facility_project = get_iri_facility_project()
    account = facility_project or (
        job_spec.attributes.account if job_spec.attributes else None
    )
    # TODO: Implement actual Slurm submission logic using app.jlab_lqcd.slurm helper
    return compute_models.Job(
        id="job_123",
        status=compute_models.JobStatus(
            state=compute_models.JobState.NEW,
            time=utc_timestamp(),
            message="job submitted",
            exit_code=0,
            meta_data={"account": account},
        ),
    )


async def update_job(
    adapter,
    resource: status_models.Resource,
    user: User,
    job_spec: compute_models.JobSpec,
    job_id: str,
) -> compute_models.Job:
    """Update a job spec/attributes on the compute resource."""
    facility_project = get_iri_facility_project()
    account = facility_project or (
        job_spec.attributes.account if job_spec.attributes else None
    )
    return compute_models.Job(
        id=job_id,
        status=compute_models.JobStatus(
            state=compute_models.JobState.ACTIVE,
            time=utc_timestamp(),
            message="job updated",
            exit_code=0,
            meta_data={"account": account},
        ),
    )


async def get_job(
    adapter,
    resource: status_models.Resource,
    user: User,
    job_id: str,
    historical: bool = False,
    include_spec: bool = False,
) -> compute_models.Job:
    """Query a specific job's status."""
    return compute_models.Job(
        id=job_id,
        status=compute_models.JobStatus(
            state=compute_models.JobState.COMPLETED,
            time=utc_timestamp(),
            message="job completed successfully",
            exit_code=0,
            meta_data={"account": "account1"},
        ),
    )


async def get_jobs(
    adapter,
    resource: status_models.Resource,
    user: User,
    offset: int,
    limit: int,
    filters: dict[str, object] | None = None,
    historical: bool = False,
    include_spec: bool = False,
) -> list[compute_models.Job]:
    """Query jobs under the compute resource."""
    return [
        compute_models.Job(
            id=f"job_{i}",
            status=compute_models.JobStatus(
                state=random.choice([s for s in compute_models.JobState]),
                time=utc_timestamp() - int(random.random() * 100),
                message="",
                exit_code=random.choice([0, 0, 0, 0, 0, 1, 1, 128, 127]),
                meta_data={"account": "account1"},
            ),
        )
        for i in range(random.randint(3, 10))
    ]


async def cancel_job(
    adapter,
    resource: status_models.Resource,
    user: User,
    job_id: str,
) -> bool:
    """Cancel a running or pending job."""
    # TODO: Implement actual Slurm cancel (scancel) logic
    return True
