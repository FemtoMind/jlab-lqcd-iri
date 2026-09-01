"""Compute and job management API functions for JLab LQCD adapter."""

import datetime

from ..routers.compute import models as compute_models
from ..routers.status import models as status_models
from ..types.user import User
from ..request_context import get_iri_facility_project
from ..apilogger import get_stream_logger
from ..config import LOG_LEVEL
from . import slurm

logger = get_stream_logger(__name__, LOG_LEVEL)


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
    account = facility_project or (job_spec.attributes.account if job_spec.attributes else None)
    return await slurm.submit_job(user.name, account, job_spec)


async def update_job(
    adapter,
    resource: status_models.Resource,
    user: User,
    job_spec: compute_models.JobSpec,
    job_id: str,
) -> compute_models.Job:
    """Update a job spec/attributes on the compute resource."""
    facility_project = get_iri_facility_project()

    account = facility_project or (job_spec.attributes.account if job_spec.attributes else None)

    return await slurm.update_job(user.name, account, job_spec, job_id)


async def get_job(
    adapter,
    resource: status_models.Resource,
    user: User,
    job_id: str,
    historical: bool = False,
    include_spec: bool = False,
) -> compute_models.Job:
    """Query a specific job's status."""

    try:
        job_status, my_job_spec = await slurm.get_job_status(
            user.name, job_id, historical, include_spec
        )
    except ValueError as e:
        # Failed to get job status
        return slurm._make_failed_job(f"Failed to get job status: {str(e)}")
    except Exception as e:
        logger.error(f"Failed to get job status: {e}")
        return slurm._make_failed_job(f"Failed to get job status: {str(e)}")

    return compute_models.Job(
        id=job_id,
        status=job_status,
        job_spec=my_job_spec,
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
    return await slurm.get_user_jobs(user.name, filters, historical, include_spec, offset, limit)


async def cancel_job(
    adapter,
    resource: status_models.Resource,
    user: User,
    job_id: str,
) -> tuple[bool, str]:
    """Cancel a running or pending job."""
    return await slurm.cancel_job(job_id, user.name)
