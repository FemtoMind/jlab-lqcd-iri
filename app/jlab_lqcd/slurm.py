"""Utilities for executing and parsing Slurm commands at Jefferson Lab."""

from app.utils import get_process_owner
import datetime
import asyncio
import os
import shlex
import re

from app.types.scalars import AllocationUnit
from ..routers.account import models as account_models
from ..routers.status import models as status_models
from ..routers.compute import models as compute_models
from .models import SlurmProject
from .sbatch_utils import (
    job_spec_to_sbatch,
    sbatch_to_job_spec,
    sbatch_file_to_job_spec,
    _parse_slurm_duration,
    _parse_slurm_memory,
)
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


async def run_slurm_command(cmd: list[str], timeout: float = 30.0, stdin: str | None = None) -> str:
    """Run an external command asynchronously and return its stdout.

    Raises SlurmCommandError if the command fails.
    """
    logger.debug("Executing command: %s", " ".join(cmd))
    # This command will be run with the current user's permissions
    # There is no shell in this command, so no quoting is necessary.
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdin=asyncio.subprocess.PIPE if stdin is not None else None,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdin_bytes = stdin.encode("utf-8") if stdin is not None else None
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(input=stdin_bytes), timeout=timeout
            )
        except asyncio.TimeoutError as e:
            try:
                proc.kill()
                await proc.wait()
            except Exception:
                pass
            logger.error("Command timed out after %s seconds: %s", timeout, " ".join(cmd))
            raise SlurmCommandError(
                cmd=" ".join(cmd),
                returncode=-1,
                stdout="",
                stderr=f"Timeout expired: {e}",
            ) from e

        stdout_str = stdout.decode("utf-8", errors="replace")
        stderr_str = stderr.decode("utf-8", errors="replace")

        if proc.returncode != 0:
            raise SlurmCommandError(
                cmd=" ".join(cmd),
                returncode=proc.returncode,
                stdout=stdout_str,
                stderr=stderr_str,
            )
        return stdout_str
    except Exception as e:
        if not isinstance(e, SlurmCommandError):
            logger.exception("Failed to execute command: %s", " ".join(cmd))
            raise SlurmCommandError(
                cmd=" ".join(cmd),
                returncode=-1,
                stdout="",
                stderr=str(e),
            ) from e
        raise e


# Get all projects for all users
"""sacctmgr show associations where Account=~* -n"""


async def get_all_slurm_projects() -> list[SlurmProject]:
    """Get all projects for all users."""
    cmd = [
        "sacctmgr",
        "show",
        "associations",
        "format=Account%20,User,Partition,Cluster",
        "-n",
    ]
    result = await run_slurm_command(cmd)
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
                projects.append(
                    SlurmProject(
                        name=prev_account,
                        users=users_in_account,
                    )
                )
            prev_account = account
            users_in_account = []

        users_in_account.append(user)

    # Add the last project
    if len(users_in_account) > 0:
        projects.append(
            SlurmProject(
                name=account,
                users=users_in_account,
            )
        )

    return projects


# Get a project allocation information including used information
async def get_project_allocation(
    project_name: str,
) -> list[account_models.AllocationEntry]:
    """Get a project allocation information including used information."""
    cmd = [
        "sacctmgr",
        "show",
        "associations",
        "where",
        f"Account={project_name}",
        "format=Share",
        "-n",
    ]
    result = await run_slurm_command(cmd)
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

    cmd = [
        "sreport",
        "-t",
        "hours",
        "cluster",
        "AccountUtilizationByUser",
        f"start={start_date_str}",
        f"Account={project_name}",
        "-n",
    ]
    result = await run_slurm_command(cmd)
    # Process the result line by line
    lines = result.strip().split("\n")

    no_usage = False
    if len(lines) < 1:
        no_usage = True

    # check the first line
    first_line = lines[0].strip()
    if first_line == "":
        no_usage = True

    if not no_usage:
        parts = first_line.split()
        plen = len(parts)
        if plen != 4:
            alloc_entry.usage = float("nan")
        else:
            # The 3rd column is usage
            alloc_entry.usage = float(parts[2])
    else:
        alloc_entry.usage = 0.0

    # Create and return a list of allocation entries.
    # In this implementation, there is only one allocation entry per project
    allocations: list[account_models.AllocationEntry] = []
    allocations.append(alloc_entry)
    return allocations


# Get cluster status by checking whether we can reach sinfo and at least 3/4 of nodes are available
async def get_cluster_status() -> status_models.Status:
    """Get cluster status by checking whether we can reach sinfo and at least 3/4 of nodes are available"""
    cmd = ["sinfo", "--format=%F", "-h"]
    try:
        result = await run_slurm_command(cmd)
    except SlurmCommandError:
        return status_models.Status.down

    # Process the result line by line
    lines = result.strip().split("\n")
    if len(lines) != 1:
        return status_models.Status.degraded

    line = lines[0].strip()
    if line == "":
        return status_models.Status.unknown

    # output format:Allocated / Idle / Other/ Total
    parts = line.split("/")
    if len(parts) != 4:
        return status_models.Status.unknown

    total_nodes = int(parts[3])
    other_nodes = int(parts[2])

    if other_nodes / total_nodes > 0.25:
        return status_models.Status.degraded

    return status_models.Status.up


# Return a job with jobid -1 and msg as a reason of failed execution
def _make_failed_job(msg: str) -> compute_models.Job:
    return compute_models.Job(
        id="-1",
        status=compute_models.JobStatus(
            state=compute_models.JobState.FAILED,
            message=msg,
        ),
    )


# Submit a job specified by a JobSpec and return a Job
async def submit_job(
    user: str, account: str | None, job_spec: compute_models.JobSpec
) -> compute_models.Job:
    # Use sbatch to submit the job.
    # check whether this process is running as root or not
    # if not, check if it is running as the same user as the slurm job
    # if not, use sudo to submit the job
    need_privilege: bool = True
    if os.geteuid() != 0:
        logger.warning("This process is not running as root!")
        server_owner = get_process_owner()
        if server_owner != user:
            logger.warning("This process is not running as the same user as the slurm job!")
            cmd: list[str] = ["sudo", "sbatch"]
        else:
            logger.info("This process is running as the same user as the slurm job!")
            cmd: list[str] = ["sbatch"]
            need_privilege = False
    else:
        logger.info("This process is running as root!")
        cmd: list[str] = ["sbatch"]

    # Convert JobSpec to sbatch arguments and commands
    import pwd

    try:
        user_pw = pwd.getpwnam(user)
        gid = user_pw.pw_gid
        home_dir = user_pw.pw_dir
    except KeyError:
        gid = None
        home_dir = f"/home/{user}"

    if need_privilege or os.geteuid() == 0:
        cmd.extend(["--uid", user])
        if gid is not None:
            cmd.extend(["--gid", str(gid)])
    cmd.append(f"--export=HOME={home_dir}")

    # Convert JobSpec to sbatch script
    script = job_spec_to_sbatch(job_spec, account)

    try:
        output = await run_slurm_command(cmd, stdin=script)
    except SlurmCommandError as e:
        logger.error("Failed to submit slurm job: %s", e.stderr)
        return _make_failed_job(str(e.stderr))

    # Parse the job ID from output
    # e.g., "Submitted batch job 123456"
    match = re.search(r"Submitted batch job (\d+)", output)
    if not match:
        return _make_failed_job(f"Could not parse job ID from sbatch output: {output}")

    job_id = match.group(1)
    logger.info("Job submitted successfully with ID: %s", job_id)

    return compute_models.Job(
        id=job_id,
        status=compute_models.JobStatus(
            state=compute_models.JobState.QUEUED,
            time=datetime.datetime.now(datetime.timezone.utc).timestamp(),
            message="Job submitted to Slurm",
            exit_code=None,
            meta_data={"account": account},
        ),
        job_spec=job_spec,
    )


# convert slurm job state to compute_models.JobState and a message
def convert_slurm_state_to_model_state_and_msg(
    state: str,
) -> tuple[compute_models.JobState, str]:
    """
    Convert a Slurm job state to a compute_models.JobState and a message.

    Args:
        state: The Slurm job state.

    Returns:
        A tuple of (compute_models.JobState, message).
    """
    job_state: compute_models.JobState = compute_models.JobState.FAILED
    msg: str = "Unknown error"
    if state == "PD" or state == "PENDING":
        job_state = compute_models.JobState.QUEUED
        msg = "Job is waiting for resources"
    elif state == "BF" or state == "BOOT_FAIL":
        job_state = compute_models.JobState.FAILED
        msg = "Job is failed because of launch failure."
    elif state == "R" or state == "RUNNING":
        job_state = compute_models.JobState.ACTIVE
        msg = "Job is running"
    elif state == "CG" or state == "COMPLETING":
        job_state = compute_models.JobState.COMPLETED
        msg = "Job is completing its cleanup"
    elif state == "CD" or state == "COMPLETED":
        job_state = compute_models.JobState.COMPLETED
        msg = "Job is completed"
    elif state == "CA" or state == "CANCELLED" or state == "CANCELLED+":
        job_state = compute_models.JobState.CANCELED
        msg = "Job is canceled"
    elif state == "F" or state == "FAILED":
        job_state = compute_models.JobState.FAILED
        msg = "Job is failed"
    elif state == "TO" or state == "TIMEOUT":
        job_state = compute_models.JobState.FAILED
        msg = "Job is failed"
    elif state == "NF" or state == "NODE_FAIL":
        job_state = compute_models.JobState.FAILED
        msg = "Job is failed because of a node crashed."
    elif state == "OOM" or state == "OUT_OF_MEMORY":
        job_state = compute_models.JobState.FAILED
        msg = "Job is failed because of out of memory."
    elif state == "TO" or state == "TIMEOUT":
        job_state = compute_models.JobState.FAILED
        msg = "Job is terminated because of reaching its time limit."
    elif state == "PR" or state == "PREEMPTED":
        job_state = compute_models.JobState.FAILED
        msg = "Job is terminated because of a preemption."
    elif state == "SO" or state == "STAGE_OUT":
        job_state = compute_models.JobState.ACTIVE
        msg = "Job is staging output file."
    elif state == "ST" or state == "STOPPED":
        job_state = compute_models.JobState.ACTIVE
        msg = "Job is stopped by SIGSTOP signal."
    elif state == "S" or state == "SUSPENDED":
        job_state = compute_models.JobState.ACTIVE
        msg = "Job is suspended."
    elif state == "SI" or state == "SIGNALED":
        job_state = compute_models.JobState.ACTIVE
        msg = "Job is being signaled"
    else:
        job_state = compute_models.JobState.FAILED
        msg = f"Job is in unknwon state '{state}'. "

    return job_state, msg


# Get running job specification
async def get_running_job_spec(user: str, job_id: str) -> compute_models.JobSpec | None:
    spec_cmd: list[str] = ["scontrol", "write", "batch_script", f"{job_id}", "-"]
    # write the contents of the batch script file to the standard output
    try:
        output = await run_slurm_command(spec_cmd)
    except SlurmCommandError as slurm_e:
        logger.warning(
            f"Failed to run scontrol write batch_script command. Details: {slurm_e.stderr}"
        )
        return None

    job_spec: compute_models.JobSpec = sbatch_to_job_spec(output)
    return job_spec


# Get a job status using jobid
async def get_job_status(
    user: str, job_id: str, historical: bool = False, include_spec: bool = False
) -> tuple[compute_models.JobStatus, compute_models.JobSpec | None]:
    hist_cmd: list[str] = ["sacct", "-j", job_id, "-o", "ExitCode,State", "-n"]
    cur_cmd: list[str] = ["squeue", "-j", job_id, "--format=%i %t", "-h"]

    # flag to determine if we should check history
    check_history = True

    # first run the squeue command, which anyone can check.
    output = ""
    try:
        output = await run_slurm_command(cur_cmd)
    except SlurmCommandError as slurm_e:
        logger.info(f"Debug: invalid job id in squeue command. Details: {slurm_e.stderr}")
    except Exception as e:
        logger.info(f"Failed to run squeue command. Details: {e}")
        output = ""
        check_history = False

    # the output should have one line contains username and state
    parts = output.split()
    if len(parts) == 2:
        if parts[0].lower() == job_id.lower():
            state = parts[1]
            check_history = False

            job_state, msg = convert_slurm_state_to_model_state_and_msg(state)
            job_status: compute_models.JobStatus = compute_models.JobStatus(
                state=job_state,
                time=int(datetime.datetime.now(datetime.timezone.utc).timestamp()),
                message=msg,
                exit_code=0,
                meta_data={"username": user, "job_id": job_id},
            )

            job_spec: compute_models.JobSpec | None = None
            if include_spec:
                job_spec = await get_running_job_spec(user, job_id)
            return job_status, job_spec

    # Get a job status from sacct
    if historical and check_history:
        output = await run_slurm_command(hist_cmd)
        lines = output.strip().split("\n")

        if len(lines) < 1:
            state = compute_models.JobState.FAILED
            msg = f"Failed to find job {job_id} in Slurm history."
            job_status: compute_models.JobStatus = compute_models.JobStatus(
                state=state,
                time=int(datetime.datetime.now(datetime.timezone.utc).timestamp()),
                message=msg,
                exit_code=0,
                meta_data={"username": user, "job_id": job_id},
            )
            return job_status, None

        parts = lines[0].split()
        if len(parts) != 2:
            state = compute_models.JobState.FAILED
            msg = f"Failed to parse job {job_id} in Slurm history. The output of {hist_cmd} is {output}"
            job_status: compute_models.JobStatus = compute_models.JobStatus(
                state=state,
                time=int(datetime.datetime.now(datetime.timezone.utc).timestamp()),
                message=msg,
                exit_code=0,
                meta_data={"username": user, "job_id": job_id},
            )
            return job_status, None

        state = parts[1]
        exitcode = int(parts[0].split(":")[0])
        job_state, msg = convert_slurm_state_to_model_state_and_msg(state)
        job_status: compute_models.JobStatus = compute_models.JobStatus(
            state=job_state,
            time=int(datetime.datetime.now(datetime.timezone.utc).timestamp()),
            message=msg,
            exit_code=exitcode,
            meta_data={"username": user, "job_id": job_id},
        )

        return job_status, None

    # This exception will be caught by calling routines
    raise ValueError(f"Failed to find job {job_id} in Slurm history or current status.")


# Get all jobs of a user using a filter. Currently only filter by time range is supported.
# starttime YYYY-MM-DD, endtime YYYY-MM-DD (default now)
async def get_user_jobs(
    user: str,
    filters: dict[str, object] | None = None,
    historical: bool = False,
    include_spec: bool = False,
    offset: int = 0,
    limit: int = 100,
) -> list[compute_models.Job]:
    """Get all jobs of a user using a filter. Currently only filter by time range is supported.

    Args:
        user: The username.
        filters: A dictionary of filters.
        historical: Whether to include historical jobs.
        include_spec: Whether to include job specs.
        offset: The offset.
        limit: The limit.

    Returns:
        A list of compute_models.Job.
    """
    # check filter
    start_time = "now-4weeks"
    end_time = "now"
    if filters:
        if "starttime" in filters:
            start_time = str(filters["starttime"])
        if "endtime" in filters:
            end_time = str(filters["endtime"])

    job_list: list[compute_models.Job] = []
    job_status_list: list[tuple[str, str]] = []
    # We do not want duplicated jobs since job ids can have multiple entries.
    # Jobs can come from squeue and from sacct
    job_id_table = set()
    num = 0
    if end_time == "now":
        cur_cmd: list[str] = ["squeue", "-u", user, "-o", "%i %t", "-h"]
        output = await run_slurm_command(cur_cmd)
        lines = output.strip().split("\n")

        for line in lines:
            parts = line.split()
            if len(parts) != 2:
                continue
            job_id = parts[0]
            state = parts[1]
            job_id_table.add(job_id)
            job_status_list.append((job_id, state))
            num += 1
            if num > limit:
                break

        for job_id, state in job_status_list:
            job_state, msg = convert_slurm_state_to_model_state_and_msg(state)
            job_status: compute_models.JobStatus = compute_models.JobStatus(
                state=job_state,
                time=int(datetime.datetime.now(datetime.timezone.utc).timestamp()),
                message=msg,
                exit_code=0,
                meta_data={"username": user, "job_id": job_id},
            )

            job_spec = None
            if include_spec:
                job_spec = await get_running_job_spec(user, job_id)
            job_list.append(compute_models.Job(id=job_id, status=job_status, job_spec=job_spec))

    # Now check whether we need to check historical data or not
    if historical and num < limit:
        hist_cmd: list[str] = [
            "sacct",
            "-u",
            user,
            "--starttime",
            start_time,
            "--endtime",
            end_time,
            "-o",
            "JobId,State,ExitCode",
            "-n",
        ]
        output = await run_slurm_command(hist_cmd)
        lines = output.strip().split("\n")
        for line in lines:
            parts = line.split()
            if len(parts) != 3:
                continue
            # some job ids looks like xyz.o or xyz.batch
            job_id = parts[0].split(".")[0]

            if job_id in job_id_table:
                continue
            job_id_table.add(job_id)
            state = parts[1]
            exit_code = int(parts[2].split(":")[0])
            job_state, msg = convert_slurm_state_to_model_state_and_msg(state)
            job_status: compute_models.JobStatus = compute_models.JobStatus(
                state=job_state,
                time=int(datetime.datetime.now(datetime.timezone.utc).timestamp()),
                message=msg,
                exit_code=exit_code,
                meta_data={"username": user, "job_id": job_id},
            )
            job_list.append(compute_models.Job(id=job_id, status=job_status, job_spec=None))
            num += 1
            if num > limit:
                break
    return job_list


# The slurm scontrol update job specifications
_supported_job_update_spec_fields: list[str] = [
    "JobName",
    "Account",
    "Comment",
    "CoreSpec",
    "CPUsPerTask",
    "Dependency",
    "ExcNodeList",
    "Extra",
    "Features",
    "Gres",
    "MailUser",
    "Name",
    "Nice",
    "Nodelist",
    "NumCPUs",
    "NumNodes",
    "NumTasks",
    "Partition",
    "Prefer",
    "Priority",
    "Reservation",
    "Requeue",
    "StdIn",
    "StdOut",
    "StdErr",
    "TimeLimit",
    "WorkDir",
]


# Convert a JobSpec to a dictionary of arguments for scontrol update
def job_spec_to_dict(job_spec: compute_models.JobSpec) -> dict[str, object]:
    """Convert a JobSpec to a dictionary of arguments for scontrol update"""
    result: dict[str, object] = {}

    # 1. Direct JobSpec fields
    if job_spec.name is not None:
        result["Name"] = job_spec.name
        result["JobName"] = job_spec.name
    if job_spec.directory is not None:
        result["WorkDir"] = job_spec.directory
    if job_spec.stdin_path is not None:
        result["StdIn"] = job_spec.stdin_path
    if job_spec.stdout_path is not None:
        result["StdOut"] = job_spec.stdout_path
    if job_spec.stderr_path is not None:
        result["StdErr"] = job_spec.stderr_path

    # 2. Resources fields
    if job_spec.resources:
        res = job_spec.resources
        if res.node_count is not None:
            result["NumNodes"] = res.node_count
        if res.process_count is not None:
            result["NumTasks"] = res.process_count
        if res.cpu_cores_per_process is not None:
            result["CPUsPerTask"] = res.cpu_cores_per_process

    # 3. Attributes fields
    if job_spec.attributes:
        attrs = job_spec.attributes
        if attrs.account is not None:
            result["Account"] = attrs.account
        if attrs.queue_name is not None:
            result["Partition"] = attrs.queue_name
        if attrs.reservation_id is not None:
            result["Reservation"] = attrs.reservation_id
        if attrs.duration is not None:
            # Convert seconds to HH:MM:SS
            total_seconds = attrs.duration
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            result["TimeLimit"] = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

        # 4. Custom attributes mapping to supported update fields
        if attrs.custom_attributes:
            # Create a map from lowercase names (with dashes/underscores removed) to the actual supported field name
            supported_map = {
                f.lower().replace("-", "").replace("_", ""): f
                for f in _supported_job_update_spec_fields
            }
            for k, v in attrs.custom_attributes.items():
                clean_k = k.lstrip("-").lower().replace("-", "").replace("_", "")
                if clean_k in supported_map:
                    field_name = supported_map[clean_k]
                    result[field_name] = v

    return result


# inner function to be used by this function to clean the value.
# It will return None if the value is "none", "n/a", "unknown" or None.
def _clean_val(v: str | None) -> str | None:
    if v is None:
        return None
    if v.lower() in ("none", "n/a", "unknown"):
        return None
    return v


# convert scontrol show job xyz to compute_models.JobSpec
async def parse_scontrol_show_job(job_id: str) -> compute_models.JobSpec | None:
    """Convert scontrol show job xyz to compute_models.JobSpec"""
    cmd = ["scontrol", "show", "job", job_id]

    try:
        output = await run_slurm_command(cmd)
    except SlurmCommandError as e:
        logger.error(f"Failed to run scontrol show job command: {e.stderr}")
        return None
    except Exception as e:
        logger.error(f"Failed to run scontrol show job command: {e}")
        return None

    # The output of scontrol show job xyz is in the format of key=value pairs.
    # We need to parse it and convert it to a JobSpec.

    # Regex to find key=value pairs
    # Handles values enclosed in quotes or standard non-space strings
    pattern = re.compile(r'([\w/:]+)=("(?:[^"\\]|\\.)*"|\S+)')

    job_data = {}
    for match in pattern.finditer(output):
        key, value = match.groups()
        # Remove surrounding quotes if present
        if value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        job_data[key] = value

    # clean value to remove None, n/a, unknown
    name = _clean_val(job_data.get("JobName"))
    directory = _clean_val(job_data.get("WorkDir"))
    stdin_path = _clean_val(job_data.get("StdIn"))
    stdout_path = _clean_val(job_data.get("StdOut"))
    stderr_path = _clean_val(job_data.get("StdErr"))
    executable = _clean_val(job_data.get("Command"))

    # Resources
    node_count = None
    num_nodes_str = job_data.get("NumNodes")
    if num_nodes_str:
        if "-" in num_nodes_str:
            num_nodes_str = num_nodes_str.split("-")[0]
        try:
            node_count = int(num_nodes_str)
        except ValueError:
            pass

    process_count = None
    num_tasks_str = job_data.get("NumTasks")
    if num_tasks_str:
        try:
            process_count = int(num_tasks_str)
        except ValueError:
            pass

    processes_per_node = None
    ntasks_per_node_str = job_data.get("NtasksPerNode")
    if ntasks_per_node_str:
        try:
            processes_per_node = int(ntasks_per_node_str)
        except ValueError:
            pass
    else:
        ntasks_per_n = job_data.get("NtasksPerN:B:S:C")
        if ntasks_per_n:
            parts = ntasks_per_n.split(":")
            if parts and parts[0] != "*" and parts[0] != "0":
                try:
                    processes_per_node = int(parts[0])
                except ValueError:
                    pass

    cpu_cores_per_process = None
    cpus_per_task_str = job_data.get("CPUs/Task") or job_data.get("CPUsPerTask")
    if cpus_per_task_str:
        try:
            cpu_cores_per_process = int(cpus_per_task_str)
        except ValueError:
            pass

    gpu_cores_per_process = None
    gpus_per_task_str = job_data.get("GpusPerTask")
    if gpus_per_task_str:
        try:
            gpu_cores_per_process = int(gpus_per_task_str)
        except ValueError:
            pass
    else:
        tres_per_task = job_data.get("TresPerTask")
        if tres_per_task:
            for item in tres_per_task.split(","):
                if item.startswith("gpu:"):
                    try:
                        gpu_cores_per_process = int(item.split(":")[1])
                    except (ValueError, IndexError):
                        pass

    exclusive_node_use = False
    oversubscribe = job_data.get("OverSubscribe") or job_data.get("Shared")
    if oversubscribe and oversubscribe.upper() == "EXCLUSIVE":
        exclusive_node_use = True

    memory = None
    mem_str = job_data.get("MinMemoryNode") or job_data.get("MinMemoryCPU")
    if mem_str:
        memory = _parse_slurm_memory(mem_str)

    resources = None
    if (
        any(
            v is not None
            for v in (
                node_count,
                process_count,
                processes_per_node,
                cpu_cores_per_process,
                gpu_cores_per_process,
                memory,
            )
        )
        or exclusive_node_use
    ):
        resources = compute_models.ResourceSpec(
            node_count=node_count,
            process_count=process_count,
            processes_per_node=processes_per_node,
            cpu_cores_per_process=cpu_cores_per_process,
            gpu_cores_per_process=gpu_cores_per_process,
            exclusive_node_use=exclusive_node_use,
            memory=memory,
        )

    # Attributes
    duration = None
    time_limit_str = job_data.get("TimeLimit")
    if time_limit_str and time_limit_str.upper() not in (
        "UNLIMITED",
        "PARTITION_LIMIT",
    ):
        duration = _parse_slurm_duration(time_limit_str)

    queue_name = _clean_val(job_data.get("Partition"))
    account = _clean_val(job_data.get("Account"))
    reservation_id = _clean_val(job_data.get("Reservation"))

    custom_attributes = {}
    supported_lower = {f.lower(): f for f in _supported_job_update_spec_fields}
    for k, v in job_data.items():
        v_clean = _clean_val(v)
        if v_clean is not None:
            k_lower = k.lower()
            if k_lower in supported_lower:
                if k_lower not in (
                    "jobname",
                    "name",
                    "workdir",
                    "stdin",
                    "stdout",
                    "stderr",
                    "numnodes",
                    "numtasks",
                    "cpuspertask",
                    "cpus/task",
                    "account",
                    "partition",
                    "reservation",
                    "timelimit",
                ):
                    custom_attributes[k.lower()] = v_clean

    attributes = None
    if (
        any(v is not None for v in (duration, queue_name, account, reservation_id))
        or custom_attributes
    ):
        attributes = compute_models.JobAttributes(
            duration=duration,
            queue_name=queue_name,
            account=account,
            reservation_id=reservation_id,
            custom_attributes=custom_attributes,
        )

    return compute_models.JobSpec(
        name=name,
        directory=directory,
        stdin_path=stdin_path,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        executable=executable,
        resources=resources,
        attributes=attributes,
    )


# Update a job using a jobspec and a jobid
async def update_job(
    user: str, account: str | None, job_spec: compute_models.JobSpec, job_id: str
) -> compute_models.Job:
    # This part will never get called since post command will catch this error.
    if account is None:
        raise ValueError("No account specified for job update")

    job_args = job_spec_to_dict(job_spec)
    if not job_args:
        return _make_failed_job("No valid arguments found in job spec for scontrol update")

    cur_cmd: list[str] = ["squeue", "-j", job_id, "--format=%i %t %a", "-h"]

    # first run the squeue command, which anyone can check.
    output = ""
    try:
        output = await run_slurm_command(cur_cmd)
    except SlurmCommandError as slurm_e:
        logger.info(f"Debug: invalid job id in squeue command. Details: {slurm_e.stderr}")
        return _make_failed_job(f"Invalid job id in squeue command. Details: {slurm_e.stderr}")
    except Exception as e:
        logger.info(f"Failed to run squeue command. Details: {e}")
        return _make_failed_job(f"Failed to run squeue command. Details: {e}")

    if output == "":
        return _make_failed_job(f"Job {job_id} not found or not in active and pending state.")

    lines = output.strip().split("\n")
    if len(lines) != 1:
        return _make_failed_job(f"Invalid output from squeue command: {output}")
    parts = lines[0].split()
    if len(parts) != 3:
        return _make_failed_job(f"Invalid output from squeue command: {output}")
    cur_account = parts[2]
    if cur_account != account:
        return _make_failed_job(
            f"Job {job_id} is not in the correct account. Current account is {cur_account}, but expected {account}."
        )

    # Check job status, if the state is active or completed, remove account from job_args
    if parts[1] not in ["PD", "PENDING"]:
        job_args.pop("Account", None)

    # go through every pair of the dictionary and construct the command
    cmd: list[str] = ["scontrol", "update", f"jobid={job_id}"]
    for key, value in job_args.items():
        cmd.append(f"{key}={value}")

    get_status = False
    msg = "job updated"
    try:
        result = await run_slurm_command(cmd)
        get_status = True
    except SlurmCommandError as e:
        logger.warning(f"Failed to execute scontrol update command: {e.stderr}")
        get_status = True
        msg = e.stderr
    except Exception as e:
        logger.error(f"Failed to execute scontrol update command: {e}")
        return _make_failed_job(f"Failed to execute scontrol update command: {e}")

    if get_status:
        try:
            jst, _ = await get_job_status(user, job_id, False, False)
        except ValueError as e:
            # Failed to get job status
            return _make_failed_job(str(e))
        except Exception as e:
            logger.error(f"Failed to get job status: {e}")
            return _make_failed_job(str(e))

        jst.message = msg

        # parse scontrol show job to get the updated job spec
        jspec = await parse_scontrol_show_job(job_id)

        return compute_models.Job(id=job_id, status=jst, job_spec=jspec)

    return _make_failed_job("Failed to execute scontrol update command")


# Cancel a job: return true if successful, false if failed
async def cancel_job(job_id: str, user: str) -> tuple[bool, str]:
    # First check if the job exists and the user is the owner
    chckcmd: list[str] = ["squeue", "-j", job_id, "--format=%i %u %t", "-h"]

    try:
        output = await run_slurm_command(chckcmd)
    except SlurmCommandError as e:
        logger.warning(f"Failed to get job status: {e.stderr}")
        return False, str(e.stderr)
    except Exception as e:
        logger.error(f"Failed to get job status: {e}")
        return False, str(e)

    if output == "":
        return False, "Job not found or not in active and pending state."

    lines = output.strip().split("\n")
    if len(lines) != 1:
        return False, "Squeeu command returned unexpected output format."

    parts = lines[0].split()
    if len(parts) != 3:
        return False, "Squeeu command returned unexpected output format."

    cur_user = parts[1]
    cur_state = parts[2]

    if cur_user != user:
        return (
            False,
            f"Job not owned by user. Current user {cur_user}, expected {user}.",
        )

    # Cancel the job
    try:
        await run_slurm_command(["scancel", job_id])
    except SlurmCommandError as e:
        logger.warning(f"Failed to cancel job {job_id}: {e.stderr}")
        return False, str(e.stderr)
    except Exception as e:
        logger.error(f"Failed to cancel job {job_id}: {e}")
        return False, str(e)

    return True, "Job cancelled successfully"
