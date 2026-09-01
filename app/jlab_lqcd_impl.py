"""
A demo adapter for the IRI Facility API that returns hardcoded data.
This is useful for testing and development of the API without needing to connect to real resources
"""

from pydantic import HttpUrl
import asyncio
import datetime
import os
import random
import subprocess
import sys

# Setup Python search path for proxy OIDC module if LQCD_PROXY_DIR is provided
LQCD_PROXY_DIR = os.environ.get("LQCD_PROXY_DIR")
if LQCD_PROXY_DIR and os.path.exists(LQCD_PROXY_DIR):
    if LQCD_PROXY_DIR not in sys.path:
        sys.path.append(LQCD_PROXY_DIR)

from fastapi import HTTPException
from pydantic import BaseModel

from .routers.account import facility_adapter as account_adapter
from .routers.account import models as account_models
from .routers.compute import facility_adapter as compute_adapter
from .routers.compute import models as compute_models
from .routers.facility import facility_adapter
from .routers.facility import models as facility_models
from .routers.filesystem import facility_adapter as filesystem_adapter
from .routers.filesystem import models as filesystem_models
from .routers.storage import facility_adapter as storage_adapter
from .routers.storage import models as storage_models
from .routers.status import facility_adapter as status_adapter
from .routers.status import models as status_models
from .routers.task import facility_adapter as task_adapter
from .routers.task import models as task_models
from .types.models import Capability
from .types.user import User
from .types.scalars import AllocationUnit
from .apilogger import get_stream_logger
from .config import LOG_LEVEL
from .utils import demo_uuid, PathSandbox
from .jlab_lqcd import account, compute, slurm, lqcdweb, filesystem

logger = get_stream_logger(__name__, LOG_LEVEL)

DEMO_QUEUE_UPDATE_SECS = int(os.environ.get("DEMO_QUEUE_UPDATE_SECS", 5))


def paginate_list(items, offset: int | None, limit: int | None):
    """Return a sliced items using offset and limit."""
    if offset is not None and offset > 0:
        items = items[offset:]
    if limit is not None and limit >= 0:
        items = items[:limit]
    return items


class CommandError(RuntimeError):
    """Raised when an external subprocess command fails."""

    def __init__(self, cmd, returncode=None, stdout=None, stderr=None):
        self.cmd = cmd
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr

        super().__init__(f"Command failed: {cmd} (rc={returncode})")


def utc_now() -> datetime.datetime:
    """Return current UTC datetime timestamp"""
    return datetime.datetime.now(datetime.timezone.utc)


def utc_timestamp() -> int:
    """Return current UTC datetime timestamp as integer"""
    return int(utc_now().timestamp())


class JlabLQCDImpl(
    status_adapter.FacilityAdapter,
    account_adapter.FacilityAdapter,
    compute_adapter.FacilityAdapter,
    filesystem_adapter.FacilityAdapter,
    storage_adapter.FacilityAdapter,
    task_adapter.FacilityAdapter,
    facility_adapter.FacilityAdapter,
):
    """A demo implementation of the FacilityAdapter that returns hardcoded data."""

    def __init__(self):
        self.resources = []
        self.incidents = []
        self.events = []
        self.capabilities = {}
        self.user = User(id="gtorok", name="Gabor Torok", api_key="12345", client_ip="1.2.3.4")
        self.projects = []
        self.project_allocations = []
        self.user_allocations = []
        self.facility: facility_models.Facility
        self.locations = {}  # resource_id -> list[StorageInstance templates]
        self.sites = []
        self._init_state()

        # Load OIDC settings if proxy codebase is available
        if LQCD_PROXY_DIR and os.path.exists(LQCD_PROXY_DIR):
            try:
                from lqcd_oidc_auth import (
                    read_oidc_auth_info,
                    load_user_account_mapping,
                )  # pylint: disable=import-outside-toplevel, import-error # noqa: F401
                import lqcd_oidc_auth  # pylint: disable=import-outside-toplevel, import-error # noqa: F401

                # Check if info is not yet loaded
                if getattr(lqcd_oidc_auth, "__auth_info", None) is None:
                    # If the OIDC settings file is not set in environment,
                    # try to load the .server_env file from the proxy directory.
                    if not os.environ.get("LQCDMCP_OIDC_INFO_FILE"):
                        env_path = os.path.join(LQCD_PROXY_DIR, ".server_env")
                        if os.path.exists(env_path):
                            from dotenv import (
                                load_dotenv,
                            )  # pylint: disable=import-outside-toplevel # noqa: F401

                            load_dotenv(env_path, override=False)

                    read_oidc_auth_info()
                    load_user_account_mapping()
            except Exception as e:
                logger.error(f"Failed to initialize proxy OIDC authentication: {e}")

    def _init_state(self):
        now = utc_now()

        site1 = facility_models.Site(
            id=demo_uuid("site", "Jlab LQCD Cluster"),
            name="Jlab LQCD Cluster",
            description="Jlab LQCD Cluster",
            last_modified=now,
            short_name="JlabLQCD",
            operating_organization="Jefferson Lab",
            country_name="USA",
            locality_name="Newport News",
            state_or_province_name="VA",
            latitude=37.2333333,
            longitude=-76.475,
            resource_ids=[],
        )

        self.facility = facility_models.Facility(
            id=demo_uuid("facility", "Jefferson Lab LQCD"),
            name="Jefferson Lab LQCD",
            description="Jefferson Lab Lattice QCD Computing Facility",
            last_modified=now,
            short_name="JlabLQCD",
            organization_name="Jefferson Lab",
            support_uri=HttpUrl("https://lqcd.jlab.org/lqcd/support"),
            site_ids=[site1.id],
        )

        self.sites = [site1]

        day_ago = utc_now() - datetime.timedelta(days=1)
        self.capabilities = {
            "cpu": Capability(
                id=demo_uuid("capability", "cpu"),
                name="CPU Nodes",
                description="24s CPU cluster",
                units=[AllocationUnit.node_hours],
                last_modified=datetime.datetime(2024, 7, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
            ),
            "gpu": Capability(
                id=demo_uuid("capability", "gpu"),
                name="GPU Nodes",
                description="21g GPU cluster",
                units=[AllocationUnit.node_hours],
                last_modified=datetime.datetime(2021, 7, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
            ),
            "cache": Capability(
                id=demo_uuid("capability", "cache"),
                name="LUSTRE Cache Storage",
                description="LUSTRE Cache Storage backed by a tape library",
                units=[AllocationUnit.bytes],
                last_modified=datetime.datetime(2025, 7, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
            ),
            "volatile": Capability(
                id=demo_uuid("capability", "volatile"),
                name="Lustre Volatile Storage",
                description="Lustre Volatile Storage cleaned up to 6-month inactive files",
                units=[AllocationUnit.bytes],
                last_modified=datetime.datetime(2025, 7, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
            ),
            "workdisk": Capability(
                id=demo_uuid("capability", "workdisk"),
                name="NFS Workdisk Storage",
                description="NFS Workdisk Storage",
                units=[AllocationUnit.bytes],
                last_modified=datetime.datetime(2025, 7, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
            ),
            "home": Capability(
                id=demo_uuid("capability", "home"),
                name="QCD user home Storage",
                description="QCD user home Storage for storing source files and small configuration files",
                units=[AllocationUnit.bytes],
                last_modified=datetime.datetime(
                    2020, 7, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
            ),
        }

        jlab_lqcd_cluster = status_models.Resource(
            id=demo_uuid("resource", "jlab-lqcd-nodes"),
            site_id=site1.id,
            group="jlab-lqcd",
            name="Jlab LQCD Cluster",
            description="the Jefferson Lab LQCD compute nodes",
            capability_ids=[
                self.capabilities["cpu"].id,
                self.capabilities["gpu"].id,
            ],
            current_status=status_models.Status.unknown,
            last_modified=now,
            resource_type=status_models.ResourceType.compute,
            supported_endpoints=[
                status_models.Endpoint.compute,
            ],
        )

        cache = status_models.Resource(
            id=demo_uuid("resource", "cache"),
            site_id=site1.id,
            group="jlab-lqcd",
            name="cache",
            description="cache storage",
            capability_ids=[self.capabilities["cache"].id],
            current_status=status_models.Status.up,
            last_modified=day_ago,
            resource_type=status_models.ResourceType.storage,
            supported_endpoints=[status_models.Endpoint.filesystem],
        )

        workdisk = status_models.Resource(
            id=demo_uuid("resource", "workdisk"),
            site_id=site1.id,
            group="jlab-lqcd",
            name="workdisk",
            description="workdisk storage",
            capability_ids=[self.capabilities["workdisk"].id],
            current_status=status_models.Status.degraded,
            last_modified=day_ago,
            resource_type=status_models.ResourceType.storage,
            supported_endpoints=[status_models.Endpoint.filesystem],
        )

        volatile = status_models.Resource(
            id=demo_uuid("resource", "volatile"),
            site_id=site1.id,
            group="jlab-lqcd",
            name="volatile",
            description="volatile storage",
            capability_ids=[self.capabilities["volatile"].id],
            current_status=status_models.Status.up,
            last_modified=day_ago,
            resource_type=status_models.ResourceType.storage,
            supported_endpoints=[status_models.Endpoint.filesystem],
        )

        nersc_home = status_models.Resource(
            id=demo_uuid("resource", "nersc_home"),
            site_id=site1.id,
            group="jlab-lqcd",
            name="nersc_home",
            description="nersc home storage",
            capability_ids=[self.capabilities["home"].id],
            current_status=status_models.Status.up,
            last_modified=day_ago,
            resource_type=status_models.ResourceType.storage,
            supported_endpoints=[status_models.Endpoint.filesystem],
        )

        # home = status_models.Resource(
        #    id=demo_uuid("resource", "home"),
        #    site_id=site1.id,
        #    group="jlab-lqcd",
        #    name="home",
        #    description="home storage",
        #    capability_ids=[self.capabilities["home"].id],
        #    current_status=status_models.Status.degraded,
        #    last_modified=day_ago,
        #    resource_type=status_models.ResourceType.storage,
        #    supported_endpoints=[status_models.Endpoint.filesystem],
        # )

        # self.resources = [jlab_lqcd_cluster, cache, workdisk, volatile, home]
        self.resources = [jlab_lqcd_cluster, cache, workdisk, volatile, nersc_home]

        _rw = storage_models.AccessPermissions(read=True, write=True, execute=True)
        _ro = storage_models.AccessPermissions(read=True, write=False, execute=True)

        # Paths use {user}, {first} (first letter of username), and {project} as placeholders.
        # Project-scoped entries (containing {project}) are expanded per-project at query time.
        # Each resource_id carries the access semantics for its own context — a compute
        # resource shows in-job permissions, a login/DTN/Globus resource shows what that
        # endpoint can do. There is no separate access_outside_of_job field.

        # Perlmutter compute nodes: in-job semantics. Home is read-only inside a job;
        # archive (HPSS) is not accessible from compute, so it isn't mounted here at all.
        self.locations[jlab_lqcd_cluster.id] = [
            storage_models.StorageInstance(
                logical_name=storage_models.LogicalName.scratch,
                path="/qcd/volatile/users/{user}",
                access=_rw,
                filesystem="/qcd/volatile",
                performance_tier="high",
                quota_bytes=20 * 1024**4,
                available_bytes=14 * 1024**4,
                purge_policy_days=60,
                shared=False,
            ),
            storage_models.StorageInstance(
                logical_name=storage_models.LogicalName.project,
                path="/qcd/work/{project}",
                access=_rw,
                filesystem="/qcd/work",
                performance_tier="medium",
                quota_bytes=2 * 1024**4,
                available_bytes=1024**4,
                purge_policy_days=None,
                shared=True,
            ),
            storage_models.StorageInstance(
                logical_name=storage_models.LogicalName.campaign,
                path="/qcd/cache/{project}",
                access=_rw,
                filesystem="/qcd/cache",
                performance_tier="medium",
                quota_bytes=10 * 1024**4,
                available_bytes=8 * 1024**4,
                purge_policy_days=90,
                shared=True,
            ),
            storage_models.StorageInstance(
                logical_name=storage_models.LogicalName.home,
                path="/global/homes/j/{user}",
                access=_rw,
                filesystem="/global/homes",
                performance_tier="medium",
                quota_bytes=10 * 1024**4,
                available_bytes=8 * 1024**4,
                purge_policy_days=90,
                shared=True,
            ),
        ]

        # Populate site resource_ids based on which resources are at each site
        site1.resource_ids = [r.id for r in self.resources if r.site_id == site1.id]

        self.projects = [
            account_models.Project(
                id=demo_uuid("project", "staff_research"),
                name="Staff research project",
                description="Compute and storage allocation for staff research use",
                user_ids=["gtorok"],
                last_modified=day_ago,
            ),
            account_models.Project(
                id=demo_uuid("project", "test_project"),
                name="Test project",
                description="Compute and storage allocation for testing use",
                user_ids=["gtorok"],
                last_modified=day_ago,
            ),
        ]

        for p in self.projects:
            for c in self.capabilities.values():
                pa = account_models.ProjectAllocation(
                    id=demo_uuid("project_allocation", f"{p.id}_{c.id}"),
                    project_id=p.id,
                    capability_id=c.id,
                    entries=[
                        account_models.AllocationEntry(
                            allocation=500 + random.random() * 500,
                            usage=100 + random.random() * 100,
                            unit=cu,
                        )
                        for cu in c.units
                    ],
                )
                self.project_allocations.append(pa)
                self.user_allocations.append(
                    account_models.UserAllocation(
                        id=demo_uuid("user_allocation", f"{pa.id}_gtorok"),
                        project_id=pa.project_id,
                        project_allocation_id=pa.id,
                        user_id="gtorok",
                        entries=[
                            account_models.AllocationEntry(
                                allocation=a.allocation / 10,
                                usage=a.usage / 10,
                                unit=a.unit,
                            )
                            for a in pa.entries
                        ],
                    )
                )

        statuses = {r.name: status_models.Status.up for r in self.resources}
        last_incidents = {}
        d = datetime.datetime(2025, 3, 1, 10, 0, 0, tzinfo=datetime.timezone.utc)

    # ----------------------------
    # Facility API
    # ----------------------------

    async def get_facility(
        self: "JlabLQCDImpl", modified_since: str | None = None
    ) -> facility_models.Facility:
        return self.facility

    async def list_sites(
        self: "JlabLQCDImpl",
        modified_since: str | None = None,
        name: str | None = None,
        offset: int | None = None,
        limit: int | None = None,
        short_name: str | None = None,
    ) -> list[facility_models.Site]:
        sites = self.sites

        if name:
            sites = [s for s in sites if s.name and name.lower() in s.name.lower()]

        if short_name:
            sites = [s for s in sites if s.short_name == short_name]

        if modified_since:
            ms = datetime.datetime.fromisoformat(str(modified_since))
            sites = [s for s in sites if s.last_modified > ms]

        o = offset or 0
        l = limit or len(sites)
        return sites[o : o + l]

    async def get_site(
        self: "JlabLQCDImpl", site_id: str, modified_since: str | None = None
    ) -> facility_models.Site:
        site = next((s for s in self.sites if s.id == site_id), None)
        if not site:
            raise HTTPException(status_code=404, detail="Site not found")

        if modified_since:
            ms = datetime.datetime.fromisoformat(str(modified_since))
            if site.last_modified <= ms:
                raise HTTPException(
                    status_code=304,
                    headers={"Last-Modified": site.last_modified.isoformat()},
                )

        return site

    # ----------------------------
    # Status API
    # ----------------------------

    async def get_resources(
        self: "JlabLQCDImpl",
        offset: int,
        limit: int,
        name: str | None = None,
        description: str | None = None,
        group: str | None = None,
        modified_since: datetime.datetime | None = None,
        resource_type: status_models.ResourceType | None = None,
        current_status: status_models.Status | None = None,
        capability: Capability | None = None,
        site_id: str | None = None,
    ) -> list[status_models.Resource]:
        # for now we just update status of computing node cluster
        for r in self.resources:
            if r.name == "Jlab LQCD Cluster":
                r.current_status = await slurm.get_cluster_status()
                r.last_modified = datetime.datetime.now()

        resources = status_models.Resource.find(
            self.resources,
            name=name,
            description=description,
            group=group,
            modified_since=modified_since,
            resource_type=resource_type,
            current_status=current_status,
            capability=capability,
            site_id=site_id,
        )
        return paginate_list(resources, offset, limit)

    async def get_resource(self: "JlabLQCDImpl", id_: str) -> status_models.Resource | None:
        return status_models.Resource.find_by_id(self.resources, id_)

    async def get_resources_for_endpoint(
        self: "JlabLQCDImpl", endpoint: status_models.Endpoint
    ) -> list[status_models.Resource]:
        return [r for r in self.resources if endpoint in r.supported_endpoints]

    async def get_events(
        self: "JlabLQCDImpl",
        offset: int,
        limit: int,
        incident_id: str | None = None,
        resource_id: str | None = None,
        name: str | None = None,
        description: str | None = None,
        status: status_models.Status | None = None,
        from_: datetime.datetime | None = None,
        to: datetime.datetime | None = None,
        time_: datetime.datetime | None = None,
        modified_since: datetime.datetime | None = None,
    ) -> list[status_models.Event]:
        lqcd_events = await lqcdweb.get_lqcd_cluster_events()
        total_events: list[status_models.Event] = []
        for event in lqcd_events:
            lustre_name = "Lustre"
            if lustre_name.casefold() in event.subject.casefold():
                r_id = demo_uuid("resource", "cache")
            else:
                r_id = demo_uuid("resource", "jlab-lqcd-nodes")

            total_events.append(
                status_models.Event(
                    id=demo_uuid("event", str(event.id)),
                    name=event.subject,
                    description=event.content,
                    last_modified=datetime.datetime.fromtimestamp(event.ctime / 1000),
                    occurred_at=datetime.datetime.fromtimestamp(event.ctime / 1000),
                    status=status_models.Status.unknown,
                    resource_id=r_id,
                )
            )

        self.events = total_events  # save to self.events for later use
        events = status_models.Event.find(
            total_events,
            incident_id=incident_id,
            resource_id=resource_id,
            name=name,
            description=description,
            status=status,
            from_=from_,
            to=to,
            time_=time_,
            modified_since=modified_since,
        )
        return paginate_list(events, offset, limit)

    async def get_event(self: "JlabLQCDImpl", id_: str) -> status_models.Event | None:
        if not self.events:
            await self.get_events(0, 1000)
        return status_models.Event.find_by_id(self.events, id_)

    async def get_incidents(
        self: "JlabLQCDImpl",
        offset: int,
        limit: int,
        name: str | None = None,
        description: str | None = None,
        status: status_models.Status | None = None,
        type_: status_models.IncidentType | None = None,
        from_: datetime.datetime | None = None,
        to: datetime.datetime | None = None,
        time_: datetime.datetime | None = None,
        modified_since: datetime.datetime | None = None,
        resource_id: str | None = None,
        resolution: status_models.Resolution | None = None,
    ) -> list[status_models.Incident]:
        incidents = status_models.Incident.find(
            self.incidents,
            name=name,
            description=description,
            status=status,
            type_=type_,
            from_=from_,
            to=to,
            time_=time_,
            modified_since=modified_since,
            resource_id=resource_id,
            resolution=resolution,
        )
        return paginate_list(incidents, offset, limit)

    async def get_incident(self: "JlabLQCDImpl", id_: str) -> status_models.Incident | None:
        return status_models.Incident.find_by_id(self.incidents, id_)

    async def get_capabilities(
        self: "JlabLQCDImpl",
        name: str | None = None,
        modified_since: str | None = None,
        offset: int = 0,
        limit: int = 1000,
    ) -> list[Capability]:
        caps = list(self.capabilities.values())
        if name:
            caps = [c for c in caps if c.name == name]
        if modified_since:
            ms = datetime.datetime.fromisoformat(str(modified_since))
            caps = [c for c in caps if c.last_modified and c.last_modified > ms]
        return paginate_list(caps, offset, limit)

    async def get_current_user(
        self: "JlabLQCDImpl",
        api_key: str,
        client_ip: str | None,
    ) -> str:
        """
        Validate OIDC token and map user identity to local username.
        If LQCD_PROXY_DIR is not configured, fall back to testing mode.
        """
        if LQCD_PROXY_DIR and os.path.exists(LQCD_PROXY_DIR):
            try:
                from lqcd_oidc_auth import (
                    validate_authorized_token,
                    get_local_account,
                )  # pylint: disable=import-outside-toplevel, import-error # noqa: F401

                # In FastAPI headers, Bearer prefix might be passed or stripped.
                # validate_authorized_token expects the raw token.
                token = api_key.strip()
                if token.startswith("Bearer "):
                    token = token[len("Bearer ") :].strip()

                valid, user_info = validate_authorized_token(token)
                if not valid or not user_info:
                    raise HTTPException(status_code=401, detail="OIDC token validation failed")

                user_login = (
                    user_info.get("email")
                    or user_info.get("preferred_username")
                    or user_info.get("sub")
                    or user_info.get("login")
                    or "unknown"
                )

                local_account = get_local_account(user_login)
                if not local_account:
                    raise HTTPException(
                        status_code=403,
                        detail=f"Could not map user identity '{user_login}' to any local account.",
                    )
                return local_account
            except Exception as e:
                if isinstance(e, HTTPException):
                    raise e
                logger.exception("OIDC authentication failed:")
                raise HTTPException(status_code=401, detail=f"OIDC authentication failed: {str(e)}")

        # Standalone/Demo fallback
        if api_key != self.user.api_key:
            raise HTTPException(status_code=401, detail="Invalid API key")
        return "gtorok"

    async def get_current_user_globus(
        self: "JlabLQCDImpl",
        api_key: str,
        client_ip: str | None,
        globus_introspect: dict | None,
    ) -> str:
        """
        Map introspected Globus token info to local user account.
        """
        if LQCD_PROXY_DIR and os.path.exists(LQCD_PROXY_DIR) and globus_introspect:
            try:
                from lqcd_oidc_auth import (
                    get_local_account,
                )  # pylint: disable=import-outside-toplevel, import-error # noqa: F401

                user_login = (
                    globus_introspect.get("username")
                    or globus_introspect.get("email")
                    or globus_introspect.get("sub")
                    or "unknown"
                )
                local_account = get_local_account(user_login)
                if not local_account:
                    raise HTTPException(
                        status_code=403,
                        detail=f"Could not map Globus identity '{user_login}' to any local account.",
                    )
                return local_account
            except Exception as e:
                if isinstance(e, HTTPException):
                    raise e
                raise HTTPException(status_code=401, detail=f"Globus OIDC mapping failed: {str(e)}")

        return "gtorok"

    async def get_user(
        self: "JlabLQCDImpl",
        user_id: str,
        api_key: str,
        client_ip: str | None,
        globus_introspect: dict | None,
    ) -> User:
        token = api_key.strip()
        if token.startswith("Bearer "):
            token = token[len("Bearer ") :].strip()

        if LQCD_PROXY_DIR and os.path.exists(LQCD_PROXY_DIR):
            # Return a User object representing the dynamically authenticated local user
            return User(id=user_id, name=user_id, api_key=token, client_ip=client_ip)

        # Standalone fallback validation
        if user_id != self.user.id:
            raise HTTPException(status_code=403, detail="User not found")
        return self.user

    async def get_projects(self: "JlabLQCDImpl", user: User) -> list[account_models.Project]:
        return await account.get_projects(self, user)

    async def get_project_allocations(
        self: "JlabLQCDImpl",
        project: account_models.Project,
        user: User,
    ) -> list[account_models.ProjectAllocation]:
        try:
            allocations = await account.get_project_allocations(self, project, user)
            return allocations
        except ValueError as e:
            logger.warning(
                f"Failed to get project allocations for user: {user.name} and project: {project.name}. Details: {e}"
            )
            return []

    async def get_user_allocations(
        self: "JlabLQCDImpl",
        user: User,
        project_allocation: account_models.ProjectAllocation,
    ) -> list[account_models.UserAllocation]:
        return await account.get_user_allocations(self, user, project_allocation)

    async def submit_job(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        job_spec: compute_models.JobSpec,
    ) -> compute_models.Job:
        return await compute.submit_job(self, resource, user, job_spec)

    async def update_job(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        job_spec: compute_models.JobSpec,
        job_id: str,
    ) -> compute_models.Job:
        return await compute.update_job(self, resource, user, job_spec, job_id)

    async def get_job(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        job_id: str,
        historical: bool = False,
        include_spec: bool = False,
    ) -> compute_models.Job:
        return await compute.get_job(self, resource, user, job_id, historical, include_spec)

    async def get_jobs(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        offset: int,
        limit: int,
        filters: dict[str, object] | None = None,
        historical: bool = False,
        include_spec: bool = False,
    ) -> list[compute_models.Job]:
        return await compute.get_jobs(
            self,
            resource,
            user,
            offset,
            limit,
            filters,
            historical,
            include_spec,
        )

    async def cancel_job(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        job_id: str,
    ) -> bool:

        status, msg = await compute.cancel_job(self, resource, user, job_id)

        if status == False:
            raise HTTPException(status_code=404, detail=msg)
        return True

    # ----------------------------------------------
    # Storage API
    # ----------------------------------------------

    @staticmethod
    def _slugify_project(name: str) -> str:
        """Convert a project name to a path-safe slug (real facilities use codes like 'm1234')."""
        return name.lower().replace(" ", "_")

    def _user_project_codes(self, user: User) -> list[str]:
        """Return the path-slug codes of all projects the user belongs to."""
        return [
            self._slugify_project(p.name)
            for p in self.projects
            if user.id in p.user_ids or LQCD_PROXY_DIR
        ]

    def _user_member_of(self, user: User, project_code: str) -> bool:
        """Authorization check: is the user a member of the named project?"""
        return any(
            (user.id in p.user_ids or LQCD_PROXY_DIR)
            and self._slugify_project(p.name) == project_code
            for p in self.projects
        )

    def _resolve_path(self, template: str, user: User, project: str | None) -> str:
        first = user.id[0] if user.id else "u"
        path = template.replace("{user}", user.id).replace("{first}", first)
        if project:
            path = path.replace("{project}", project)
        return path

    def _apply_intent_filter(
        self,
        instance: storage_models.StorageInstance,
        intent: storage_models.StorageIntent | None,
    ) -> bool:
        """Return False if this storage instance should be excluded for the given intent."""
        if intent == storage_models.StorageIntent.long_term_storage:
            return instance.logical_name == storage_models.LogicalName.archive
        if intent == storage_models.StorageIntent.staging:
            return instance.logical_name != storage_models.LogicalName.archive
        if intent == storage_models.StorageIntent.write:
            return instance.access.write
        return True

    async def get_locations(
        self,
        resource: status_models.Resource,
        user: User,
        logicalpath: storage_models.LogicalName | None,
        project: str | None,
        allocation: str | None,
        intent: storage_models.StorageIntent | None,
    ) -> list[storage_models.StorageInstance]:
        templates = self.locations.get(resource.id, [])
        effective_project = project or allocation

        # Authorization: a user can only resolve paths for their own projects
        if effective_project and not self._user_member_of(user, effective_project):
            raise HTTPException(
                status_code=403,
                detail=f"User is not a member of project '{effective_project}'",
            )

        # Expand project-scoped paths across ALL of the user's projects when none specified
        project_codes = [effective_project] if effective_project else self._user_project_codes(user)

        result = []
        for m in templates:
            if logicalpath and m.logical_name != logicalpath:
                continue
            if not self._apply_intent_filter(m, intent):
                continue

            is_project_scoped = "{project}" in m.path
            expand_over = project_codes if is_project_scoped else [None]

            for code in expand_over:
                result.append(
                    storage_models.StorageInstance(
                        logical_name=m.logical_name,
                        path=self._resolve_path(m.path, user, code),
                        filesystem=m.filesystem,
                        performance_tier=m.performance_tier,
                        quota_bytes=m.quota_bytes,
                        available_bytes=m.available_bytes,
                        purge_policy_days=m.purge_policy_days,
                        shared=m.shared,
                        access=m.access,
                    )
                )
        return result

    def validate_path(self, path: str, allow_symlinks: bool = True) -> str:
        """Validate that the given path is within the sandbox base directory and optionally check for symlinks."""
        basedir = PathSandbox.get_base_temp_dir()
        real_path = os.path.realpath(os.path.join(basedir, path))

        # Check within sandbox
        if not real_path.startswith(basedir + os.sep) and real_path != basedir:
            raise HTTPException(status_code=400, detail=f"Path outside sandbox: {path}")

        # Optionally block symlinks that point outside sandbox
        if not allow_symlinks and os.path.islink(os.path.join(basedir, path)):
            link_target = os.readlink(os.path.join(basedir, path))
            if os.path.isabs(link_target):
                raise HTTPException(status_code=400, detail=f"Absolute symlink not allowed: {path}")

        return real_path

    # ----------------------------------------------
    # Filesystem API
    # ----------------------------------------------
    def _run(
        self,
        args,
        *,
        shell: bool = False,
        timeout: int | None = 3600,
        text: bool = True,
    ) -> subprocess.CompletedProcess:
        """
        Run a subprocess command and catch exceptions.
        Raises CommandError on failure with captured diagnostics.
        """
        try:
            return subprocess.run(
                args,
                shell=shell,
                capture_output=True,
                text=text,
                check=True,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as exc:
            logger.warning(f"Command timed out: {args} (after {timeout} seconds)")
            raise CommandError(
                cmd=args, returncode=None, stdout=exc.stdout, stderr=exc.stderr
            ) from exc
        except subprocess.CalledProcessError as exc:
            logger.warning(
                f"Command failed: {args} (rc={exc.returncode})\nstdout: {exc.stdout}\nstderr: {exc.stderr}"
            )
            raise CommandError(
                cmd=args,
                returncode=exc.returncode,
                stdout=exc.stdout,
                stderr=exc.stderr,
            ) from exc
        except OSError as exc:
            logger.warning(f"OS error running command: {args}\nError: {exc}")
            raise CommandError(cmd=args, returncode=None, stdout=None, stderr=str(exc)) from exc

    async def chmod(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        request_model: filesystem_models.PutFileChmodRequest,
    ) -> filesystem_models.PutFileChmodResponse:
        print("chmod is called")
        return await filesystem.chmod(self, resource, user, request_model)

    async def chown(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        request_model: filesystem_models.PutFileChownRequest,
    ) -> filesystem_models.PutFileChownResponse:
        return await filesystem.chown(self, resource, user, request_model)

    async def ls(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        path: str,
        show_hidden: bool,
        numeric_uid: bool,
        recursive: bool,
        dereference: bool,
        transfer_token: str | None = None,
    ) -> filesystem_models.GetDirectoryLsResponse:
        return await filesystem.ls(
            self,
            resource,
            user,
            path,
            show_hidden,
            numeric_uid,
            recursive,
            dereference,
            transfer_token,
        )

    async def head(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        path: str,
        file_bytes: int | None,
        lines: int | None,
        skip_trailing: bool = False,
    ) -> filesystem_models.GetFileHeadResponse:
        return await filesystem.head(
            self,
            resource,
            user,
            path,
            file_bytes,
            lines,
            skip_trailing,
        )

    async def tail(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        path: str,
        file_bytes: int | None,
        lines: int | None,
        skip_heading: bool = False,
    ) -> filesystem_models.GetFileTailResponse:
        return await filesystem.tail(
            self,
            resource,
            user,
            path,
            file_bytes,
            lines,
            skip_heading,
        )

    async def view(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        path: str,
        size: int,
        offset: int,
    ) -> filesystem_models.GetViewFileResponse:
        return await filesystem.view(self, resource, user, path, size, offset)

    async def checksum(
        self: "JlabLQCDImpl", resource: status_models.Resource, user: User, path: str
    ) -> filesystem_models.GetFileChecksumResponse:
        return await filesystem.checksum(self, resource, user, path)

    async def file(
        self: "JlabLQCDImpl", resource: status_models.Resource, user: User, path: str,
        transfer_token: str | None = None,
    ) -> filesystem_models.GetFileTypeResponse:
        return await filesystem.file(self, resource, user, path, transfer_token)

    async def stat(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        path: str,
        dereference: bool,
        transfer_token: str | None = None,
    ) -> filesystem_models.GetFileStatResponse:
        return await filesystem.stat(self, resource, user, path, dereference, transfer_token)

    async def rm(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        path: str,
        transfer_token: str | None = None,
    ) -> filesystem_models.RemoveResponse:
        return await filesystem.rm(self, resource, user, path, transfer_token)

    async def mkdir(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        request_model: filesystem_models.PostMakeDirRequest,
        transfer_token: str | None = None,
    ) -> filesystem_models.PostMkdirResponse:
        return await filesystem.mkdir(self, resource, user, request_model, transfer_token)

    async def symlink(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        request_model: filesystem_models.PostFileSymlinkRequest,
    ) -> filesystem_models.PostFileSymlinkResponse:
        return await filesystem.symlink(self, resource, user, request_model)

    async def download(
        self: "JlabLQCDImpl", resource: status_models.Resource, user: User, path: str,
        transfer_token: str | None = None,
    ) -> filesystem_models.GetFileDownloadResponse:
        return await filesystem.download(self, resource, user, path, transfer_token)

    async def upload(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        path: str,
        content: str,
        transfer_token: str | None = None,
    ) -> filesystem_models.PutFileUploadResponse:
        return await filesystem.upload(self, resource, user, path, content, transfer_token)

    async def compress(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        request_model: filesystem_models.PostCompressRequest,
    ) -> filesystem_models.PostCompressResponse:
        return await filesystem.compress(self, resource, user, request_model)

    async def extract(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        request_model: filesystem_models.PostExtractRequest,
    ) -> filesystem_models.PostExtractResponse:
        return await filesystem.extract(self, resource, user, request_model)

    async def mv(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        request_model: filesystem_models.PostMoveRequest,
        transfer_token: str | None = None,
    ) -> filesystem_models.PostMoveResponse:
        return await filesystem.mv(self, resource, user, request_model, transfer_token)

    async def cp(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        request_model: filesystem_models.PostCopyRequest,
    ) -> filesystem_models.PostCopyResponse:
        return await filesystem.cp(self, resource, user, request_model)


    async def transfer(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        dest_resource: status_models.Resource,
        request_model: filesystem_models.PostCopyRequest,
        transfer_token: str | None = None,
    ) -> filesystem_models.PostCopyResponse:
        return await filesystem.transfer(self, resource, user, dest_resource, request_model, transfer_token)

    async def get_task(self: "JlabLQCDImpl", user: User, task_id: str) -> task_models.Task | None:
        await SimpleTaskQueue.process_tasks(self)
        return next(
            (t for t in SimpleTaskQueue.tasks if t.user.name == user.name and t.id == task_id),
            None,
        )

    async def get_tasks(self: "JlabLQCDImpl", user: User) -> list[task_models.Task]:
        await SimpleTaskQueue.process_tasks(self)
        return [t for t in SimpleTaskQueue.tasks if t.user.name == user.name]

    async def put_task(
        self: "JlabLQCDImpl",
        user: User,
        resource: status_models.Resource | None,
        task: task_models.TaskCommand,
    ) -> task_models.TaskSubmitResponse:
        result = SimpleTaskQueue.create_task(user, resource, task)
        await SimpleTaskQueue.process_tasks(self)
        return result

    async def delete_task(self: "JlabLQCDImpl", user: User, task_id: str) -> None:
        await SimpleTaskQueue.process_tasks(self)
        for t in SimpleTaskQueue.tasks:
            if t.user.name == user.name and t.id == task_id:
                t.status = task_models.TaskStatus.canceled
                t.result = None
                break


class JlabLQCDTask(BaseModel):
    """A simple in-memory task queue for demonstration purposes."""

    id: str
    task: str
    resource: status_models.Resource | None
    user: User
    start: float
    status: task_models.TaskStatus = task_models.TaskStatus.pending
    result: dict | None = None


class SimpleTaskQueue:
    """A simple in-memory task queue for demonstration purposes."""

    tasks = []
    _background_task = None
    _starting_task_id = 1234

    @staticmethod
    async def _run_loop(da: JlabLQCDImpl):
        while True:
            await asyncio.sleep(DEMO_QUEUE_UPDATE_SECS)
            try:
                await SimpleTaskQueue.process_tasks(da)
            except Exception as e:
                logger.error(f"Error in SimpleTaskQueue background loop: {e}", exc_info=True)

    @staticmethod
    async def _execute_task_in_background(task_item: JlabLQCDTask, cmd: task_models.TaskCommand):
        try:
            if task_item.resource is None:
                raise ValueError("Resource is required to execute task")
            (result, status) = await JlabLQCDImpl.on_task(task_item.resource, task_item.user, cmd)
            if isinstance(result, BaseModel):
                task_item.result = result.model_dump()
            elif isinstance(result, dict):
                task_item.result = result
            else:
                task_item.result = {"output": result}
            task_item.status = status
        except Exception as e:
            logger.error(f"Error executing task {task_item.id}: {e}", exc_info=True)
            task_item.status = task_models.TaskStatus.failed
            task_item.result = {"error": str(e)}

    @staticmethod
    async def process_tasks(da: JlabLQCDImpl):
        """Process tasks in the queue, simulating task execution and completion."""
        if SimpleTaskQueue._background_task is None:
            try:
                loop = asyncio.get_running_loop()
                SimpleTaskQueue._background_task = loop.create_task(SimpleTaskQueue._run_loop(da))
                logger.info("Started SimpleTaskQueue background loop")
            except RuntimeError:
                pass

        now = utc_timestamp()
        _tasks = []
        for t in SimpleTaskQueue.tasks:
            if now - t.start > 5 * 60 and t.status in [
                task_models.TaskStatus.completed,
                task_models.TaskStatus.canceled,
                task_models.TaskStatus.failed,
            ]:
                # delete old tasks
                continue
            if t.status == task_models.TaskStatus.pending:
                t.status = task_models.TaskStatus.active
                t.start = now
                cmd = task_models.TaskCommand.model_validate_json(t.task)
                asyncio.create_task(SimpleTaskQueue._execute_task_in_background(t, cmd))
            _tasks.append(t)
        SimpleTaskQueue.tasks = _tasks

    @staticmethod
    def create_task(
        user: User,
        resource: status_models.Resource | None,
        command: task_models.TaskCommand,
    ) -> task_models.TaskSubmitResponse:
        """Create a new task in the queue."""
        task_id = f"{SimpleTaskQueue._starting_task_id}"
        SimpleTaskQueue._starting_task_id += 1
        SimpleTaskQueue.tasks.append(
            JlabLQCDTask(
                id=task_id,
                task=command.model_dump_json(),
                user=user,
                resource=resource,
                start=utc_timestamp(),
            )
        )
        logger.info(f"Created task: {task_id}")
        return task_models.TaskSubmitResponse(task_id=task_id)
