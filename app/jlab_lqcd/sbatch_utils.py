"""Lightweight utilities for converting between Slurm sbatch scripts and JobSpec models.

This module has minimal dependencies (standard library and compute models)
and does not require Slurm binaries or server runtimes.
"""

from __future__ import annotations

import re
import shlex

from ..routers.compute import models as compute_models


def parse_slurm_duration(val: str) -> int:
    """Parse Slurm time limit format to seconds.
    Supported formats:
      - minutes
      - minutes:seconds
      - hours:minutes:seconds
      - days-hours
      - days-hours:minutes:seconds
    """
    try:
        days = 0
        if "-" in val:
            days_str, val = val.split("-", 1)
            days = int(days_str)

        parts = list(map(int, val.split(":")))
        if len(parts) == 1:
            if days > 0:
                hours = parts[0]
                return (days * 24 + hours) * 3600
            else:
                return parts[0] * 60
        elif len(parts) == 2:
            return parts[0] * 60 + parts[1]
        elif len(parts) == 3:
            hours, minutes, seconds = parts
            return (days * 24 + hours) * 3600 + minutes * 60 + seconds
    except Exception:
        pass
    return 0


# Alias for internal compatibility
_parse_slurm_duration = parse_slurm_duration


def parse_slurm_memory(val: str) -> int:
    """Parse Slurm memory format (e.g. 1024, 1024M, 16G) to bytes.
    Slurm default unit is Megabytes if no suffix is specified.
    """
    match = re.match(r"^(\d+)([KkMmGgTt])?$", val.strip())
    if not match:
        return 0
    num = int(match.group(1))
    suffix = match.group(2)
    if not suffix:
        return num * 1024 * 1024
    suffix = suffix.upper()
    if suffix == "K":
        return num * 1024
    elif suffix == "M":
        return num * 1024 * 1024
    elif suffix == "G":
        return num * 1024 * 1024 * 1024
    elif suffix == "T":
        return num * 1024 * 1024 * 1024 * 1024
    return num


# Alias for internal compatibility
_parse_slurm_memory = parse_slurm_memory


def job_spec_to_sbatch(job_spec: compute_models.JobSpec, account: str | None = None) -> str:
    """Convert a JobSpec and resolved account into a complete sbatch script string."""
    lines = ["#!/bin/bash"]

    # 1. Map JobSpec direct attributes to #SBATCH directives
    if job_spec.name:
        lines.append(f"#SBATCH --job-name={job_spec.name}")

    if job_spec.directory:
        lines.append(f"#SBATCH --chdir={job_spec.directory}")

    if job_spec.stdin_path:
        lines.append(f"#SBATCH --input={job_spec.stdin_path}")

    if job_spec.stdout_path:
        lines.append(f"#SBATCH --output={job_spec.stdout_path}")

    if job_spec.stderr_path:
        lines.append(f"#SBATCH --error={job_spec.stderr_path}")

    if not job_spec.inherit_environment:
        lines.append("#SBATCH --export=NONE")

    # 2. Account (prioritizing resolved parameter over attributes.account)
    resolved_account = account or (job_spec.attributes.account if job_spec.attributes else None)
    if resolved_account:
        lines.append(f"#SBATCH --account={resolved_account}")

    # 3. Attributes (queue_name, duration, reservation_id, custom_attributes)
    if job_spec.attributes:
        attrs = job_spec.attributes
        if attrs.queue_name:
            lines.append(f"#SBATCH --partition={attrs.queue_name}")

        if attrs.duration is not None:
            # Convert seconds to HH:MM:SS
            total_seconds = attrs.duration
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            seconds = total_seconds % 60
            lines.append(f"#SBATCH --time={hours:02d}:{minutes:02d}:{seconds:02d}")

        if attrs.reservation_id:
            lines.append(f"#SBATCH --reservation={attrs.reservation_id}")

        if attrs.custom_attributes:
            for k, v in attrs.custom_attributes.items():
                key = k if k.startswith("-") else f"--{k}"
                if v is not None and v != "":
                    lines.append(f"#SBATCH {key}={v}")
                else:
                    lines.append(f"#SBATCH {key}")

    # 4. Resources (node_count, process_count, processes_per_node, cpu_cores_per_process, gpu_cores_per_process, exclusive_node_use, memory)
    if job_spec.resources:
        res = job_spec.resources
        if res.node_count is not None:
            lines.append(f"#SBATCH --nodes={res.node_count}")

        if res.process_count is not None:
            lines.append(f"#SBATCH --ntasks={res.process_count}")

        if res.processes_per_node is not None:
            lines.append(f"#SBATCH --ntasks-per-node={res.processes_per_node}")

        if res.cpu_cores_per_process is not None:
            lines.append(f"#SBATCH --cpus-per-task={res.cpu_cores_per_process}")

        if res.gpu_cores_per_process is not None and res.gpu_cores_per_process > 0:
            lines.append(f"#SBATCH --gpus-per-task={res.gpu_cores_per_process}")

        if res.exclusive_node_use:
            lines.append("#SBATCH --exclusive")

        if res.memory is not None:
            # Convert bytes to MB
            mem_mb = res.memory // (1024 * 1024)
            lines.append(f"#SBATCH --mem={mem_mb}M")

    # 5. Pre-launch commands
    if job_spec.pre_launch:
        lines.append("")
        lines.append("# Pre-launch commands")
        lines.append(job_spec.pre_launch)

    # 6. Environment variables
    if job_spec.environment:
        lines.append("")
        lines.append("# Environment variables")
        for k, v in job_spec.environment.items():
            lines.append(f"export {k}={shlex.quote(v)}")
            if job_spec.container:
                lines.append(f"export APPTAINERENV_{k}={shlex.quote(v)}")
                lines.append(f"export SINGULARITYENV_{k}={shlex.quote(v)}")

    # 7. Main run command
    lines.append("")
    lines.append("# Main execution command")

    cmd_parts = []
    if job_spec.container:
        cmd_parts.extend(["apptainer", "exec"])
        if (
            job_spec.resources
            and job_spec.resources.gpu_cores_per_process is not None
            and job_spec.resources.gpu_cores_per_process > 0
        ):
            cmd_parts.append("--nv")

        for mount in job_spec.container.volume_mounts:
            mode = "ro" if mount.read_only else "rw"
            cmd_parts.extend(["--bind", f"{mount.source}:{mount.target}:{mode}"])

        cmd_parts.append(job_spec.container.image)

    if job_spec.executable:
        cmd_parts.append(job_spec.executable)

    if job_spec.arguments:
        cmd_parts.extend(job_spec.arguments)

    if job_spec.launcher:
        cmd_parts = [job_spec.launcher] + cmd_parts

    cmd_line = shlex.join(cmd_parts)
    lines.append(cmd_line)

    # 8. Post-launch commands
    if job_spec.post_launch:
        lines.append("")
        lines.append("# Post-launch commands")
        lines.append(job_spec.post_launch)

    return "\n".join(lines) + "\n"


def sbatch_to_job_spec(sbatch_content: str) -> compute_models.JobSpec:
    """Parse a Slurm sbatch script back into a structured JobSpec."""
    lines = sbatch_content.strip().split("\n")

    # Initialize fields
    name = None
    directory = None
    stdin_path = None
    stdout_path = None
    stderr_path = None
    inherit_environment = True

    # Resource spec fields
    node_count = None
    process_count = None
    processes_per_node = None
    cpu_cores_per_process = None
    gpu_cores_per_process = None
    exclusive_node_use = False
    memory = None

    # Job attributes fields
    duration = None
    queue_name = None
    account = None
    reservation_id = None
    custom_attributes = {}

    # Env & scripts
    environment = {}
    pre_launch_lines = []
    post_launch_lines = []
    main_cmd_line = None

    body_lines = []

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue

        # Parse #SBATCH directives
        if stripped.startswith("#SBATCH"):
            content = stripped[7:].strip()
            if "=" in content:
                key, val = content.split("=", 1)
                key = key.strip()
                val = val.strip()
            else:
                parts = content.split(None, 1)
                key = parts[0].strip()
                val = parts[1].strip() if len(parts) > 1 else None

            opt = key.lstrip("-")

            if opt in ("job-name", "J"):
                name = val
            elif opt in ("chdir", "D"):
                directory = val
            elif opt in ("input", "i"):
                stdin_path = val
            elif opt in ("output", "o"):
                stdout_path = val
            elif opt in ("error", "e"):
                stderr_path = val
            elif opt == "export":
                if val and val.upper() == "NONE":
                    inherit_environment = False
            elif opt in ("account", "A"):
                account = val
            elif opt in ("partition", "p"):
                queue_name = val
            elif opt in ("time", "t"):
                if val:
                    duration = parse_slurm_duration(val)
            elif opt == "reservation":
                reservation_id = val
            elif opt in ("nodes", "N"):
                node_count = int(val) if val else None
            elif opt in ("ntasks", "n"):
                process_count = int(val) if val else None
            elif opt == "ntasks-per-node":
                processes_per_node = int(val) if val else None
            elif opt in ("cpus-per-task", "c"):
                cpu_cores_per_process = int(val) if val else None
            elif opt == "gpus-per-task":
                gpu_cores_per_process = int(val) if val else None
            elif opt == "exclusive":
                exclusive_node_use = True
            elif opt == "mem":
                if val:
                    memory = parse_slurm_memory(val)
            else:
                custom_attributes[opt] = val if val is not None else ""

        elif stripped.startswith("#"):
            continue
        else:
            body_lines.append(line)

    non_env_body_lines = []
    for line in body_lines:
        stripped = line.strip()
        if stripped.startswith("export "):
            expr = stripped[7:].strip()
            if "=" in expr:
                k, v = expr.split("=", 1)
                k = k.strip()
                v = v.strip()
                if (v.startswith('"') and v.endswith('"')) or (
                    v.startswith("'") and v.endswith("'")
                ):
                    v = v[1:-1]

                if k.startswith("APPTAINERENV_"):
                    base_k = k[13:]
                    if base_k not in environment:
                        environment[base_k] = v
                elif k.startswith("SINGULARITYENV_"):
                    base_k = k[15:]
                    if base_k not in environment:
                        environment[base_k] = v
                else:
                    environment[k] = v
        else:
            non_env_body_lines.append(line)

    main_cmd_idx = -1
    for idx, line in enumerate(non_env_body_lines):
        stripped = line.strip()
        if not stripped:
            continue
        tokens = shlex.split(stripped)
        if not tokens:
            continue
        first_token = tokens[0]

        if first_token in ("srun", "mpirun", "mpiexec", "jsrun"):
            main_cmd_idx = idx
            break
        if first_token in ("apptainer", "singularity"):
            main_cmd_idx = idx
            break
        if first_token not in ("module", "echo", "cd", "mkdir", "pwd"):
            main_cmd_idx = idx
            break

    if main_cmd_idx == -1 and non_env_body_lines:
        for idx in range(len(non_env_body_lines) - 1, -1, -1):
            if non_env_body_lines[idx].strip():
                main_cmd_idx = idx
                break

    launcher = None
    container = None
    executable = None
    arguments = []

    if main_cmd_idx != -1:
        pre_launch_lines = non_env_body_lines[:main_cmd_idx]
        post_launch_lines = non_env_body_lines[main_cmd_idx + 1 :]

        main_cmd_line = non_env_body_lines[main_cmd_idx].strip()
        tokens = shlex.split(main_cmd_line)

        if tokens and tokens[0] in ("srun", "mpirun", "mpiexec", "jsrun"):
            launcher = tokens[0]
            tokens = tokens[1:]

        if (
            tokens
            and tokens[0] in ("apptainer", "singularity")
            and len(tokens) > 1
            and tokens[1] == "exec"
        ):
            tokens = tokens[2:]

            volume_mounts = []
            container_image = None

            idx = 0
            while idx < len(tokens):
                tok = tokens[idx]
                if tok == "--nv":
                    if gpu_cores_per_process is None or gpu_cores_per_process == 0:
                        gpu_cores_per_process = 1
                    idx += 1
                elif tok in ("--bind", "-B"):
                    if idx + 1 < len(tokens):
                        bind_val = tokens[idx + 1]
                        parts = bind_val.split(":")
                        src = parts[0]
                        tgt = parts[1] if len(parts) > 1 else src
                        mode = parts[2] if len(parts) > 2 else "rw"
                        volume_mounts.append(
                            compute_models.VolumeMount(
                                source=src, target=tgt, read_only=mode == "ro"
                            )
                        )
                        idx += 2
                    else:
                        idx += 1
                elif tok.startswith("-"):
                    idx += 1
                else:
                    container_image = tok
                    idx += 1
                    break

            if container_image:
                container = compute_models.Container(
                    image=container_image, volume_mounts=volume_mounts
                )

            if idx < len(tokens):
                executable = tokens[idx]
                arguments = tokens[idx + 1 :]
        else:
            if tokens:
                executable = tokens[0]
                arguments = tokens[1:]
    else:
        pre_launch_lines = non_env_body_lines
        post_launch_lines = []

    pre_launch = "\n".join(line.strip() for line in pre_launch_lines if line.strip()) or None
    post_launch = "\n".join(line.strip() for line in post_launch_lines if line.strip()) or None

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
        executable=executable,
        container=container,
        arguments=arguments,
        directory=directory,
        name=name,
        inherit_environment=inherit_environment,
        environment=environment,
        stdin_path=stdin_path,
        stdout_path=stdout_path,
        stderr_path=stderr_path,
        resources=resources,
        attributes=attributes,
        pre_launch=pre_launch,
        post_launch=post_launch,
        launcher=launcher,
    )


def sbatch_file_to_job_spec(file_path: str) -> compute_models.JobSpec:
    """Read a Slurm sbatch file and parse it into a structured JobSpec."""
    with open(file_path, "r", encoding="utf-8") as f:
        return sbatch_to_job_spec(f.read())
