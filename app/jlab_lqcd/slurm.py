"""Utilities for executing and parsing Slurm commands at Jefferson Lab."""
import datetime

from app.types.scalars import AllocationUnit
import subprocess
import logging
from ..routers.account import models as account_models
from .models import SlurmProject
from ..apilogger import get_stream_logger
from ..config import LOG_LEVEL

logger = get_stream_logger(__name__, LOG_LEVEL)


class SlurmCommandError(RuntimeError):
    """Raised when an external Slurm/subprocess command fails."""

    def __init__(self, cmd, returncode=None, stdout=None, stderr=None):
        self.cmd = cmd
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        super().__init__(f"Slurm command failed: {cmd} (rc={returncode})")


def run_slurm_command(cmd: list[str], timeout: float = 30.0) -> str:
    """Run an external command and return its stdout.

    Raises SlurmCommandError if the command fails.
    """
    logger.debug("Executing command: %s", " ".join(cmd))
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout,
            check=False,
        )
        if result.returncode != 0:
            raise SlurmCommandError(
                cmd=" ".join(cmd),
                returncode=result.returncode,
                stdout=result.stdout,
                stderr=result.stderr,
            )
        return result.stdout
    except subprocess.TimeoutExpired as e:
        logger.error("Command timed out after %s seconds: %s", timeout, " ".join(cmd))
        raise SlurmCommandError(
            cmd=" ".join(cmd),
            returncode=-1,
            stdout="",
            stderr=f"Timeout expired: {e}",
        ) from e
    except Exception as e:
        logger.exception("Failed to execute command: %s", " ".join(cmd))
        raise SlurmCommandError(
            cmd=" ".join(cmd),
            returncode=-1,
            stdout="",
            stderr=str(e),
        ) from e


# Get all projects for all users
"""sacctmgr show associations where Account=~* -n"""
def get_all_slurm_projects() -> list[SlurmProject]:
    """Get all projects for all users."""
    cmd = ["sacctmgr", "show", "associations", "format=Account%20,User,Partition,Cluster", "-n"]
    result = run_slurm_command(cmd)
    # Process the result line by line
    lines = result.strip().split("\n")
    projects: list[SlurmProject] = []
    account = ""
    user = "" 
    partition = ""
    cluster = ""
    prev_account = "N/A"
    users_in_account = []

    for line in lines:
        parts = line.split()
        plen = len(parts)
        if plen == 4:
            account, user, partition, cluster = parts
        elif plen == 3:
            account, user, cluster = parts
        else:
            continue

        if account != prev_account:
            if len(users_in_account) > 0:
                projects.append(SlurmProject(
                    name=prev_account,
                    users=users_in_account,
                ))
            prev_account = account
            users_in_account = []
        
        users_in_account.append(user)
    
    # Add the last project
    if len(users_in_account) > 0:
        projects.append(SlurmProject(
            name=account,
            users=users_in_account,
        ))
    
    return projects 


# Get a project allocation information including used information
def get_project_allocation(project_name: str) -> list[account_models.AllocationEntry]:
    """Get a project allocation information including used information."""
    cmd = ["sacctmgr", "show", "associations", "where", f"Account={project_name}", "format=Share", "-n"]
    result = run_slurm_command(cmd)
    # Process the result line by line
    lines = result.strip().split("\n")

    if len(lines) < 1:
        return []

    # In jlab the first line is the allocated fairshare information.
    alloc_entry = account_models.AllocationEntry(
        allocation=float(lines[0].strip()),
        usage=0.0,
        unit=AllocationUnit("node_hours"),
    )
    
    # Now check the usage of this project by running the sreport command.
    # Find start date which is July 1st every year
    today = datetime.date.today()
    
    if today.month >= 7:
        start_date = datetime.date(today.year, 7, 1)
    else:
        start_date = datetime.date(today.year - 1, 7, 1)

    # convert start_date to string YYYY-MM-DD
    start_date_str = start_date.strftime("%Y-%m-%d")
    
    
    cmd=["sreport", "-t", "hours", "cluster", "AccountUtilizationByUser", f"start={start_date_str}", f"Account={project_name}", "-n"]
    result = run_slurm_command(cmd)
    # Process the result line by line
    lines = result.strip().split("\n")

    if len(lines) < 1:
        return []

    # check the first line
    first_line = lines[0].strip()
    parts = first_line.split()
    plen = len(parts)
    if plen != 4:
        raise ValueError("Invalid sreport output format.")

    # The 3rd column is usage
    alloc_entry.usage = float(parts[2])

    # Create and return a list of allocation entries.
    # In this implementation, there is only one allocation entry per project
    allocations: list[account_models.AllocationEntry] = []
    allocations.append(alloc_entry)
    return allocations
