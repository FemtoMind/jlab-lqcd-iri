"""
A demo adapter for the IRI Facility API that returns hardcoded data.
This is useful for testing and development of the API without needing to connect to real resources
"""

from pydantic import HttpUrl
import base64
import datetime
import glob
import grp
import os
import pathlib
import pwd
import random
import stat
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
from .utils import demo_uuid
from .jlab_lqcd import account, compute, slurm, lqcdweb

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


class PathSandbox:
    """A simple sandbox for file operations."""

    _base_temp_dir = None

    @classmethod
    def get_base_temp_dir(cls):
        """Get the base temporary directory for the sandbox."""
        if cls._base_temp_dir is None:
            # Create in system temp with a fixed name
            cls._base_temp_dir = os.path.join(os.getcwd(), "iri_sandbox")
            os.makedirs(cls._base_temp_dir, exist_ok=True)

            # create a test file
            with open(
                f"{cls._base_temp_dir}/test.txt", encoding="utf-8", mode="w"
            ) as f:
                f.write("hello world")
            logger.info(f"Created test file in sandbox: {cls._base_temp_dir}/test.txt")
        return cls._base_temp_dir


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
        self.user = User(
            id="gtorok", name="Gabor Torok", api_key="12345", client_ip="1.2.3.4"
        )
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
                last_modified=datetime.datetime(
                    2024, 7, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
            ),
            "gpu": Capability(
                id=demo_uuid("capability", "gpu"),
                name="GPU Nodes",
                description="21g GPU cluster",
                units=[AllocationUnit.node_hours],
                last_modified=datetime.datetime(
                    2021, 7, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
            ),
            "cache": Capability(
                id=demo_uuid("capability", "cache"),
                name="LUSTRE Cache Storage",
                description="LUSTRE Cache Storage backed by a tape library",
                units=[AllocationUnit.bytes],
                last_modified=datetime.datetime(
                    2025, 7, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
            ),
            "volatile": Capability(
                id=demo_uuid("capability", "volatile"),
                name="Lustre Volatile Storage",
                description="Lustre Volatile Storage cleaned up to 6-month inactive files",
                units=[AllocationUnit.bytes],
                last_modified=datetime.datetime(
                    2025, 7, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
            ),
            "workdisk": Capability(
                id=demo_uuid("capability", "workdisk"),
                name="NFS Workdisk Storage",
                description="NFS Workdisk Storage",
                units=[AllocationUnit.bytes],
                last_modified=datetime.datetime(
                    2025, 7, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
                ),
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
                status_models.Endpoint.filesystem,
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

        home = status_models.Resource(
            id=demo_uuid("resource", "home"),
            site_id=site1.id,
            group="jlab-lqcd",
            name="home",
            description="home storage",
            capability_ids=[self.capabilities["home"].id],
            current_status=status_models.Status.degraded,
            last_modified=day_ago,
            resource_type=status_models.ResourceType.storage,
            supported_endpoints=[status_models.Endpoint.filesystem],
        )

        self.resources = [jlab_lqcd_cluster, cache, workdisk, volatile, home]

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
                logical_name=storage_models.LogicalName.home,
                path="/home/{user}",
                access=_rw,
                filesystem="/home",
                performance_tier="medium",
                quota_bytes=40 * 1024**3,
                available_bytes=28 * 1024**3,
                purge_policy_days=None,
                shared=False,
            ),
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

    async def get_resource(
        self: "JlabLQCDImpl", id_: str
    ) -> status_models.Resource | None:
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

    async def get_incident(
        self: "JlabLQCDImpl", id_: str
    ) -> status_models.Incident | None:
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
                    raise HTTPException(
                        status_code=401, detail="OIDC token validation failed"
                    )

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
                raise HTTPException(
                    status_code=401, detail=f"OIDC authentication failed: {str(e)}"
                )

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
                raise HTTPException(
                    status_code=401, detail=f"Globus OIDC mapping failed: {str(e)}"
                )

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

    async def get_projects(
        self: "JlabLQCDImpl", user: User
    ) -> list[account_models.Project]:
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
        return await compute.get_job(
            self, resource, user, job_id, historical, include_spec
        )

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
        project_codes = (
            [effective_project] if effective_project else self._user_project_codes(user)
        )

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
                raise HTTPException(
                    status_code=400, detail=f"Absolute symlink not allowed: {path}"
                )

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
            raise CommandError(
                cmd=args, returncode=None, stdout=None, stderr=str(exc)
            ) from exc

    def _file(self, path: str) -> filesystem_models.File:
        # Get file stats (follows symlinks by default)
        rp = self.validate_path(path)
        file_stat = os.stat(rp)  # Use lstat to not follow symlinks

        # Get file type
        if stat.S_ISDIR(file_stat.st_mode):
            file_type = "directory"
        elif stat.S_ISLNK(file_stat.st_mode):
            file_type = "symlink"
        elif stat.S_ISREG(file_stat.st_mode):
            file_type = "file"
        else:
            file_type = "other"

        # Get link target if it's a symlink
        link_target = None
        if stat.S_ISLNK(file_stat.st_mode):
            link_target = os.readlink(rp)

        # Get user and group names
        user = pwd.getpwuid(file_stat.st_uid).pw_name
        group = grp.getgrgid(file_stat.st_gid).gr_name

        # Get permissions in rwxrwxrwx format
        permissions = stat.filemode(file_stat.st_mode)

        # Get last modified time
        last_modified = datetime.datetime.fromtimestamp(file_stat.st_mtime).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        # Get size
        size = str(file_stat.st_size)
        data = dict(
            name=os.path.basename(rp),
            type=file_type,
            user=user,
            group=group,
            permissions=permissions,
            last_modified=last_modified,
            size=size,
        )

        if link_target is not None:
            data["link_target"] = link_target

        return filesystem_models.File(**data)

    async def chmod(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        request_model: filesystem_models.PutFileChmodRequest,
    ) -> filesystem_models.PutFileChmodResponse:
        if request_model.path is None:
            raise HTTPException(status_code=400, detail="Path is required")

        rp = self.validate_path(request_model.path)
        os.chmod(rp, int(request_model.mode, 8))
        return filesystem_models.PutFileChmodResponse(output=self._file(rp))

    async def chown(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        request_model: filesystem_models.PutFileChownRequest,
    ) -> filesystem_models.PutFileChownResponse:
        if request_model.path is None:
            raise HTTPException(status_code=400, detail="Path is required")
        rp = self.validate_path(request_model.path)
        if request_model.owner is None:
            raise HTTPException(status_code=400, detail="Owner is required")
        if request_model.group is None:
            raise HTTPException(status_code=400, detail="Group is required")
        try:
            uid = pwd.getpwnam(request_model.owner).pw_uid
        except KeyError as e:
            raise HTTPException(
                status_code=400, detail=f"Owner not found: {request_model.owner}"
            ) from e
        try:
            gid = grp.getgrnam(request_model.group).gr_gid
        except KeyError as e:
            raise HTTPException(
                status_code=400, detail=f"Group not found: {request_model.group}"
            ) from e

        os.chown(rp, uid, gid)
        return filesystem_models.PutFileChownResponse(output=self._file(rp))

    async def ls(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        path: str,
        show_hidden: bool,
        numeric_uid: bool,
        recursive: bool,
        dereference: bool,
    ) -> filesystem_models.GetDirectoryLsResponse:
        rp = self.validate_path(path)
        files = glob.glob(rp, recursive=recursive)
        return filesystem_models.GetDirectoryLsResponse(
            output=[self._file(f) for f in files]
        )

    def _headtail(
        self: "JlabLQCDImpl",
        cmd: str,
        path: str,
        file_bytes: int | None,
        lines: int | None,
        skip_heading: bool = False,
        skip_trailing: bool = False,
    ) -> str:
        args = [cmd]

        if cmd == "tail" and skip_heading:
            if file_bytes is not None:
                args.extend(["-c", f"+{file_bytes + 1}"])
            elif lines is not None:
                args.extend(["-n", f"+{lines + 1}"])
        if cmd == "head" and skip_trailing:
            if file_bytes is not None:
                args.extend(["-c", f"-{file_bytes}"])
            elif lines is not None:
                args.extend(["-n", f"-{lines}"])
        else:
            if file_bytes is not None:
                args.extend(["-c", str(file_bytes)])
            elif lines is not None:
                args.extend(["-n", str(lines)])

        rp = self.validate_path(path)
        args.append(rp)

        result = self._run(args)
        return result.stdout

    async def head(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        path: str,
        file_bytes: int | None,
        lines: int | None,
        skip_trailing: bool = False,
    ) -> filesystem_models.GetFileHeadResponse:
        content = self._headtail(
            "head", path, file_bytes, lines, skip_trailing=skip_trailing
        )

        fc = filesystem_models.FileContent(
            content=content,
            content_type=(
                filesystem_models.ContentUnit.bytes
                if file_bytes is not None
                else filesystem_models.ContentUnit.lines
            ),
            start_position=0,
            end_position=len(content),
        )

        return filesystem_models.GetFileHeadResponse(output=fc)

    async def tail(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        path: str,
        file_bytes: int | None,
        lines: int | None,
        skip_heading: bool = False,
    ) -> filesystem_models.GetFileTailResponse:

        content = self._headtail(
            "tail", path, file_bytes, lines, skip_heading=skip_heading
        )

        fc = filesystem_models.FileContent(
            content=content,
            content_type=(
                filesystem_models.ContentUnit.bytes
                if file_bytes is not None
                else filesystem_models.ContentUnit.lines
            ),
            start_position=0,
            end_position=len(content),
        )

        return filesystem_models.GetFileTailResponse(output=fc)

    async def view(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        path: str,
        size: int,
        offset: int,
    ) -> filesystem_models.GetViewFileResponse:
        rp = self.validate_path(path)
        result = self._run(f"tail -c +{offset + 1} {rp} | head -c {size}", shell=True)
        content = result.stdout
        return filesystem_models.GetViewFileResponse(
            output=filesystem_models.FileContent(
                content=content,
                content_type=filesystem_models.ContentUnit.bytes,
                start_position=offset,
                end_position=offset + len(content),
            ),
        )

    async def checksum(
        self: "JlabLQCDImpl", resource: status_models.Resource, user: User, path: str
    ) -> filesystem_models.GetFileChecksumResponse:
        rp = self.validate_path(path)
        result = self._run(["sha256sum", rp])
        checksum = result.stdout.split()[0]
        return filesystem_models.GetFileChecksumResponse(
            output=filesystem_models.FileChecksum(
                checksum=checksum,
            )
        )

    async def file(
        self: "JlabLQCDImpl", resource: status_models.Resource, user: User, path: str
    ) -> filesystem_models.GetFileTypeResponse:
        rp = self.validate_path(path)
        result = self._run(["file", "-b", rp])
        return filesystem_models.GetFileTypeResponse(
            output=result.stdout.strip(),
        )

    async def stat(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        path: str,
        dereference: bool,
    ) -> filesystem_models.GetFileStatResponse:
        rp = self.validate_path(path)
        if dereference:
            stat_info = os.stat(rp)
        else:
            stat_info = os.lstat(rp)
        return filesystem_models.GetFileStatResponse(
            output=filesystem_models.FileStat(
                mode=stat_info.st_mode,
                ino=stat_info.st_ino,
                dev=stat_info.st_dev,
                nlink=stat_info.st_nlink,
                uid=stat_info.st_uid,
                gid=stat_info.st_gid,
                size=stat_info.st_size,
                atime=int(stat_info.st_atime),
                ctime=int(stat_info.st_ctime),
                mtime=int(stat_info.st_mtime),
            )
        )

    async def rm(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        path: str,
    ) -> filesystem_models.RemoveResponse:
        rp = self.validate_path(path)
        if rp == PathSandbox.get_base_temp_dir():
            raise HTTPException(status_code=400, detail="Cannot delete sandbox")
        self._run(["rm", "-rf", rp])
        return filesystem_models.RemoveResponse(output=f"Removed {rp}")

    async def mkdir(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        request_model: filesystem_models.PostMakeDirRequest,
    ) -> filesystem_models.PostMkdirResponse:
        if request_model.path is None:
            raise HTTPException(status_code=400, detail="Path is required")
        rp = self.validate_path(request_model.path)
        args = ["mkdir"]
        if request_model.parent:
            args.append("-p")
        args.append(rp)
        self._run(args)
        return filesystem_models.PostMkdirResponse(output=self._file(rp))

    async def symlink(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        request_model: filesystem_models.PostFileSymlinkRequest,
    ) -> filesystem_models.PostFileSymlinkResponse:
        if request_model.path is None:
            raise HTTPException(status_code=400, detail="Path is required")
        rp_src = self.validate_path(request_model.path)
        rp_dst = self.validate_path(request_model.link_path)
        self._run(["ln", "-s", rp_src, rp_dst])
        return filesystem_models.PostFileSymlinkResponse(output=self._file(rp_dst))

    async def download(
        self: "JlabLQCDImpl", resource: status_models.Resource, user: User, path: str
    ) -> filesystem_models.GetFileDownloadResponse:
        rp = self.validate_path(path)
        raw_content = pathlib.Path(rp).read_bytes()

        if len(raw_content) > filesystem_adapter.OPS_SIZE_LIMIT:
            raise Exception("File to download is too large.")

        return filesystem_models.GetFileDownloadResponse(
            output=base64.b64encode(raw_content).decode("utf-8"),
        )

    async def upload(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        path: str,
        content: str,
    ) -> filesystem_models.PutFileUploadResponse:
        rp = self.validate_path(path)
        if isinstance(content, bytes):
            pathlib.Path(rp).write_bytes(content)
        elif isinstance(content, str):
            pathlib.Path(rp).write_bytes(base64.b64decode(content))
        else:
            raise Exception(
                f"Don't know how to handle variable of type: {type(content)}"
            )
        return filesystem_models.PutFileUploadResponse(output=f"Uploaded to {rp}")

    async def compress(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        request_model: filesystem_models.PostCompressRequest,
    ) -> filesystem_models.PostCompressResponse:
        if request_model.path is None:
            raise HTTPException(status_code=400, detail="Path is required")
        if request_model.target_path is None:
            raise HTTPException(status_code=400, detail="Target path is required")
        src_rp = self.validate_path(request_model.path)
        dst_rp = self.validate_path(request_model.target_path)

        args = ["tar"]
        if request_model.compression == filesystem_models.CompressionType.gzip:
            args.append("-czf")
        elif request_model.compression == filesystem_models.CompressionType.bzip2:
            args.append("-cjf")
        elif request_model.compression == filesystem_models.CompressionType.xz:
            args.append("-cJf")
        args.append(dst_rp)
        if request_model.dereference:
            args.append("--dereference")
        if request_model.match_pattern:
            args.append(f"--include={request_model.match_pattern}")

        args.append("-C")
        args.append(PathSandbox.get_base_temp_dir())
        p = pathlib.Path(src_rp)
        args.append(str(p.relative_to(PathSandbox.get_base_temp_dir())))
        subprocess.run(args, check=True)

        return filesystem_models.PostCompressResponse(output=self._file(dst_rp))

    async def extract(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        request_model: filesystem_models.PostExtractRequest,
    ) -> filesystem_models.PostExtractResponse:
        if request_model.path is None:
            raise HTTPException(status_code=400, detail="Path is required")
        if request_model.target_path is None:
            raise HTTPException(status_code=400, detail="Target path is required")
        src_rp = self.validate_path(request_model.path)
        dst_rp = self.validate_path(request_model.target_path)

        if os.path.exists(dst_rp):
            if os.path.isdir(dst_rp):
                raise Exception(
                    f"Target path already exists: {request_model.target_path}"
                )
            else:
                raise Exception(
                    f"Target path already exists and is not a directory: {request_model.target_path}"
                )
        os.makedirs(dst_rp)

        args = ["tar"]
        if request_model.compression == filesystem_models.CompressionType.gzip:
            args.append("-xzf")
        elif request_model.compression == filesystem_models.CompressionType.bzip2:
            args.append("-xjf")
        elif request_model.compression == filesystem_models.CompressionType.xz:
            args.append("-xJf")
        else:
            args.append("-xf")
        args.append(src_rp)
        args.append("-C")
        args.append(dst_rp)
        subprocess.run(args, check=True)

        return filesystem_models.PostExtractResponse(output=self._file(dst_rp))

    async def mv(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        request_model: filesystem_models.PostMoveRequest,
    ) -> filesystem_models.PostMoveResponse:
        if request_model.path is None:
            raise HTTPException(status_code=400, detail="Path is required")
        if request_model.target_path is None:
            raise HTTPException(status_code=400, detail="Target path is required")
        src_rp = self.validate_path(request_model.path)
        dst_rp = self.validate_path(request_model.target_path)
        self._run(["mv", src_rp, dst_rp])
        return filesystem_models.PostMoveResponse(output=self._file(dst_rp))

    async def cp(
        self: "JlabLQCDImpl",
        resource: status_models.Resource,
        user: User,
        request_model: filesystem_models.PostCopyRequest,
    ) -> filesystem_models.PostCopyResponse:
        if request_model.path is None:
            raise HTTPException(status_code=400, detail="Path is required")
        if request_model.target_path is None:
            raise HTTPException(status_code=400, detail="Target path is required")
        src_rp = self.validate_path(request_model.path)
        dst_rp = self.validate_path(request_model.target_path)
        args = ["cp"]
        if request_model.dereference:
            args.append("-L")
        args.append(src_rp)
        args.append(dst_rp)
        subprocess.run(args, check=True)
        return filesystem_models.PostCopyResponse(output=self._file(dst_rp))

    async def get_task(
        self: "JlabLQCDImpl", user: User, task_id: str
    ) -> task_models.Task | None:
        await DemoTaskQueue.process_tasks(self)
        return next(
            (
                t
                for t in DemoTaskQueue.tasks
                if t.user.name == user.name and t.id == task_id
            ),
            None,
        )

    async def get_tasks(self: "JlabLQCDImpl", user: User) -> list[task_models.Task]:
        await DemoTaskQueue.process_tasks(self)
        return [t for t in DemoTaskQueue.tasks if t.user.name == user.name]

    async def put_task(
        self: "JlabLQCDImpl",
        user: User,
        resource: status_models.Resource | None,
        task: task_models.TaskCommand,
    ) -> task_models.TaskSubmitResponse:
        await DemoTaskQueue.process_tasks(self)
        return DemoTaskQueue.create_task(user, resource, task)

    async def delete_task(self: "JlabLQCDImpl", user: User, task_id: str) -> None:
        await DemoTaskQueue.process_tasks(self)
        for t in DemoTaskQueue.tasks:
            if t.user.name == user.name and t.id == task_id:
                t.status = task_models.TaskStatus.canceled
                t.result = None
                break


class DemoTask(BaseModel):
    """A simple in-memory task queue for demonstration purposes."""

    id: str
    task: str
    resource: status_models.Resource | None
    user: User
    start: float
    status: task_models.TaskStatus = task_models.TaskStatus.pending
    result: dict | None = None


class DemoTaskQueue:
    """A simple in-memory task queue for demonstration purposes."""

    tasks = []

    @staticmethod
    async def process_tasks(da: JlabLQCDImpl):
        """Process tasks in the queue, simulating task execution and completion."""
        now = utc_timestamp()
        _tasks = []
        for t in DemoTaskQueue.tasks:
            if now - t.start > 5 * 60 and t.status in [
                task_models.TaskStatus.completed,
                task_models.TaskStatus.canceled,
                task_models.TaskStatus.failed,
            ]:
                # delete old tasks
                continue
            if (
                t.status == task_models.TaskStatus.pending
                and now - t.start > DEMO_QUEUE_UPDATE_SECS
            ):
                t.status = task_models.TaskStatus.active
                t.start = now
            elif (
                t.status == task_models.TaskStatus.active
                and now - t.start > DEMO_QUEUE_UPDATE_SECS
            ):
                cmd = task_models.TaskCommand.model_validate_json(t.task)
                (result, status) = await JlabLQCDImpl.on_task(t.resource, t.user, cmd)
                if isinstance(result, BaseModel):
                    t.result = result.model_dump()
                elif isinstance(result, dict):
                    t.result = result
                else:
                    t.result = {"output": result}
                t.status = status
            _tasks.append(t)
        DemoTaskQueue.tasks = _tasks

    @staticmethod
    def create_task(
        user: User,
        resource: status_models.Resource | None,
        command: task_models.TaskCommand,
    ) -> task_models.TaskSubmitResponse:
        """Create a new task in the queue."""
        task_id = f"task_{len(DemoTaskQueue.tasks)}"
        DemoTaskQueue.tasks.append(
            DemoTask(
                id=task_id,
                task=command.model_dump_json(),
                user=user,
                resource=resource,
                start=utc_timestamp(),
            )
        )
        logger.info(f"Created task: {task_id}")
        return task_models.TaskSubmitResponse(task_id=task_id)
