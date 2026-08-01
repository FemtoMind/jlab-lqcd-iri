"""Account-related API functions for JLab LQCD adapter."""

from datetime import datetime
from ..routers.account import models as account_models
from ..types.user import User
from . import slurm 
from .models import SlurmProject

async def get_projects(adapter, user: User) -> list[account_models.Project]:
    """Retrieve projects associated with the user."""
    slurm_projects: list[SlurmProject] = slurm.get_all_slurm_projects()
    iri_projects: list[account_models.Project] = []
    # Use ISO-8601 format string: lqcd project starts on July 1st every year
    # Set last_modified date to July 1st of the current or previous year
    lm = datetime.fromisoformat('2026-07-01')
    if datetime.now().month < 7:
        lm = lm.replace(year=datetime.now().year - 1)
    else:
        lm = lm.replace(year=datetime.now().year)
    
    for sp in slurm_projects:
        if user.id not in sp.users:
            continue
        iri_project = account_models.Project(
            id=sp.name,
            name=sp.name,
            user_ids=sp.users,
            last_modified=lm,
            description="",
            )
        iri_projects.append(iri_project)
    return iri_projects


async def get_project_allocations(
    adapter,
    project: account_models.Project,
    user: User,
) -> list[account_models.ProjectAllocation]:
    """Retrieve allocations for the specified project."""
    slurm_projects: list[SlurmProject] = slurm.get_all_slurm_projects()
    iri_projects: list[account_models.Project] = []
    # Use ISO-8601 format string: lqcd project starts on July 1st every year
    # Set last_modified date to July 1st of the current or previous year
    lm = datetime.fromisoformat('2026-07-01')
    if datetime.now().month < 7:
        lm = lm.replace(year=datetime.now().year - 1)
    else:
        lm = lm.replace(year=datetime.now().year)
    
    pa: list[account_models.ProjectAllocation] = []

    f_slurm_project: SlurmProject | None = None
    for sp in slurm_projects:
        if sp.name == project.name:
            f_slurm_project = sp
            break
    
    if f_slurm_project is None:
        raise ValueError(f"Project {project.name} not found")
    
    if user.id not in f_slurm_project.users:
        raise ValueError(f"User {user.id} is not in project {project.name}")
    
    allocations: list[account_models.AllocationEntry] = slurm.get_project_allocation(project.name)
    
    # Jlab lqcd project has one allocation entry
    if len(allocations) != 1:
        raise ValueError(f"Project {project.name} has {len(allocations)} allocation entries.")
    
    # We don't have a good way to map each lqcd project to a Capability, 
    # and the capability names are not well organized in JLab Slurm. 
    # find capability id: if an account ends with "g", capability is "gpu", 
    # otherwise capability is "cpu".
    if project.name.endswith("g"):
        capability_id = "gpu"
    else:
        capability_id = "cpu"
    
    project_allocation: account_models.ProjectAllocation = account_models.ProjectAllocation(
        id=project.name,
        project_id=project.id,
        capability_id=capability_id,
        entries=allocations,
    )
    pa.append(project_allocation)

    return pa

async def get_user_allocations(
    adapter,
    user: User,
    project_allocation: account_models.ProjectAllocation,
) -> list[account_models.UserAllocation]:
    """Retrieve user allocations for the specified project allocation.
    
    For JLab LQCD, user allocations are equally distributed among all users in the project.
    There is only one "user allocation" in the system, which is the project allocation itself.
    """
    slurm_projects: list[SlurmProject] = slurm.get_all_slurm_projects()
    iri_projects: list[account_models.Project] = []
    # Use ISO-8601 format string: lqcd project starts on July 1st every year
    # Set last_modified date to July 1st of the current or previous year
    lm = datetime.fromisoformat('2026-07-01')
    if datetime.now().month < 7:
        lm = lm.replace(year=datetime.now().year - 1)
    else:
        lm = lm.replace(year=datetime.now().year)
    
    ua: list[account_models.UserAllocation] = []

    # Check whether we specified a correct project_allocation
    for sp in slurm_projects:
        if user.id in sp.users:
            if project_allocation.id == sp.name:
                allocations: list[account_models.AllocationEntry] = project_allocation.entries
                
                ua.append(account_models.UserAllocation(
                    id=user.id,
                    project_id=sp.name,
                    project_allocation_id=project_allocation.id,
                    user_id=user.id,
                    entries=allocations,
                ))
    return ua
