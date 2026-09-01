"""Filesystem management API functions for JLab LQCD adapter."""

import base64
import datetime
import grp
import os
import pathlib
import pwd
import stat as py_stat
import subprocess
from typing import Any, Mapping
from fastapi import HTTPException

# Need globus_sdk for endpoints
import globus_sdk

from ..routers.filesystem import facility_adapter as filesystem_adapter
from ..routers.filesystem import models as filesystem_models
from ..routers.status import models as status_models
from ..types.user import User
from ..utils import PathSandbox

from ..apilogger import get_stream_logger
from ..config import LOG_LEVEL

logger = get_stream_logger(__name__, LOG_LEVEL)

GLOBUS_ENDPOINTS = {
    "jlab#gw1": "b0fca1ad-f485-4a00-8fcd-bca0b93a2a1c",
    "jlab#gw2": "a2f9c453-2bb6-4336-919d-f195efcf327b",
    "NERSC-HOME": "9d6d994a-6d04-11e5-ba46-22000b92c6ec",
    "local": None,
}


def _get_local_endpoint_id() -> str:
    local_ep_id = os.environ.get("GLOBUS_LOCAL_ENDPOINT_ID") or GLOBUS_ENDPOINTS.get("local")
    if not local_ep_id:
        try:
            local_ep_id = globus_sdk.LocalGlobusConnectPersonal().endpoint_id
        except Exception:
            pass
    if not local_ep_id:
        raise HTTPException(
            status_code=400,
            detail="Local Globus endpoint ID could not be determined.",
        )
    return local_ep_id


def _file(adapter, path: str) -> filesystem_models.File:
    # Get file stats (follows symlinks by default)
    rp = adapter.validate_path(path)
    file_stat = os.stat(rp)  # Use lstat to not follow symlinks

    # Get file type
    if py_stat.S_ISDIR(file_stat.st_mode):
        file_type = "directory"
    elif py_stat.S_ISLNK(file_stat.st_mode):
        file_type = "symlink"
    elif py_stat.S_ISREG(file_stat.st_mode):
        file_type = "file"
    else:
        file_type = "other"

    # Get link target if it's a symlink
    link_target = None
    if py_stat.S_ISLNK(file_stat.st_mode):
        link_target = os.readlink(rp)

    # Get user and group names
    user = pwd.getpwuid(file_stat.st_uid).pw_name
    group = grp.getgrgid(file_stat.st_gid).gr_name

    # Get permissions in rwxrwxrwx format
    permissions = py_stat.filemode(file_stat.st_mode)

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
    adapter,
    resource: status_models.Resource,
    user: User,
    request_model: filesystem_models.PutFileChmodRequest,
) -> filesystem_models.PutFileChmodResponse:
    if request_model.path is None:
        raise HTTPException(status_code=400, detail="Path is required")

    raise HTTPException(
        status_code=501,
        detail="chmod is not implemented by the Jefferson Lab LQCD adapter due to security policies.",
    )


async def chown(
    adapter,
    resource: status_models.Resource,
    user: User,
    request_model: filesystem_models.PutFileChownRequest,
) -> filesystem_models.PutFileChownResponse:
    if request_model.path is None:
        raise HTTPException(status_code=400, detail="Path is required")

    raise HTTPException(
        status_code=501,
        detail="chown is not implemented by the Jefferson Lab LQCD adapter due to security policies.",
    )


# Helper function to convert Globus ls result to filesystem_models.File
def _globus_item_to_file(
    item: Mapping[str, Any], default_user: str, rel_prefix: str = ""
) -> filesystem_models.File:
    item_name = item.get("name", "")
    full_rel_name = os.path.join(rel_prefix, item_name) if rel_prefix else item_name

    raw_type = item.get("type", "file")
    file_type = "directory" if raw_type == "dir" else raw_type

    size_val = item.get("size")
    size_str = str(size_val) if size_val is not None else "0"

    return filesystem_models.File(
        name=full_rel_name,
        type=file_type,
        user=item.get("user") or default_user,
        group=item.get("group") or "",
        permissions=item.get("permissions") or "",
        last_modified=item.get("last_modified") or "",
        size=size_str,
        link_target=item.get("link_target"),
    )


# Recursive helper function to fetch Globus ls results and convert them to filesystem_models.File
def _fetch_globus_ls(
    tc: globus_sdk.TransferClient,
    endpoint_id: str,
    current_path: str,
    show_hidden: bool,
    recursive: bool,
    user_name: str,
    rel_prefix: str = "",
) -> list[filesystem_models.File]:
    files = []
    res = tc.operation_ls(endpoint_id, path=current_path, show_hidden=show_hidden)
    for item in res:
        file_obj = _globus_item_to_file(item, user_name, rel_prefix=rel_prefix)
        files.append(file_obj)

        if recursive and item.get("type") == "dir":
            item_name = item.get("name", "")
            sub_path = os.path.join(current_path, item_name)
            sub_rel_prefix = (
                os.path.join(rel_prefix, item_name) if rel_prefix else item_name
            )
            try:
                sub_files = _fetch_globus_ls(
                    tc,
                    endpoint_id,
                    sub_path,
                    show_hidden,
                    recursive,
                    user_name,
                    rel_prefix=sub_rel_prefix,
                )
                files.extend(sub_files)
            except globus_sdk.exc.GlobusAPIError:
                pass
    return files

# Make sure the path is valid for our Jlab resource 
def _validate_resource_path(resource: status_models.Resource, path: str) -> None:
    if resource.name == "cache":
        if not path.startswith("/qcd/cache"):
            raise HTTPException(
                status_code=400, detail="Path must start with /qcd/cache"
            )
    elif resource.name == "volatile":
        if not path.startswith("/qcd/volatile"):
            raise HTTPException(
                status_code=400, detail="Path must start with /qcd/volatile"
            )
    elif resource.name == "workdisk":
        if not path.startswith("/qcd/work"):
            raise HTTPException(
                status_code=400, detail="Path must start with /qcd/work"
            )
    elif resource.name == "nersc_home":
        if not path.startswith("/global/homes"):
            raise HTTPException(
                status_code=400, detail="Path must start with /global/homes"
            )
    else:
        raise HTTPException(
            status_code=400, detail=f"Resource {resource.name} not found"
        )

# Check globus active endpoint and return the transfer client and active endpoint id 
def _get_active_globus_transfer_client(
    resource: status_models.Resource,
    path: str,
    transfer_token: str | None,
) -> tuple[globus_sdk.TransferClient, str]:
    _validate_resource_path(resource, path)

    if transfer_token is None:
        raise HTTPException(
            status_code=400, detail="Transfer token is required for Globus operations."
        )

    # Create authorizer using the transfer token
    authorizer = globus_sdk.AccessTokenAuthorizer(transfer_token)
    # create TransferClient with the authorizer
    tc = globus_sdk.TransferClient(authorizer=authorizer)

    # Map each resource to its available Globus endpoints
    available_endpoints = {
        "nersc_home": [GLOBUS_ENDPOINTS.get("NERSC-HOME")],
        "cache": [GLOBUS_ENDPOINTS.get("jlab#gw1"), GLOBUS_ENDPOINTS.get("jlab#gw2")],
        "volatile": [GLOBUS_ENDPOINTS.get("jlab#gw1"), GLOBUS_ENDPOINTS.get("jlab#gw2")],
        "workdisk": [GLOBUS_ENDPOINTS.get("jlab#gw1"), GLOBUS_ENDPOINTS.get("jlab#gw2")],
    }

    user_preferred_endpoints: list[str | None] = available_endpoints.get(
        resource.name or "", []
    )
    endpoints_to_try: list[str] = [ep for ep in user_preferred_endpoints if ep is not None]
    last_exception = None
    active_endpoint_id = None

    for ep_id in endpoints_to_try:
        try:
            # Check token validity and get endpoint details
            tc.get_endpoint(ep_id)
            # Test connection status to the endpoint's live filesystem
            tc.operation_ls(ep_id, path="/")

            # Connection succeeded
            active_endpoint_id = ep_id
            break
        except globus_sdk.exc.GlobusAPIError as exc:
            # If it's a token authorization issue, fail immediately (switching endpoints won't help)
            if exc.http_status in (401, 403):
                if exc.http_status == 401:
                    raise HTTPException(
                        status_code=401,
                        detail="Globus authentication failed: Invalid or expired transfer token.",
                    ) from exc
                else:
                    raise HTTPException(
                        status_code=403,
                        detail=f"Permission denied to access Globus endpoint {ep_id}.",
                    ) from exc
            last_exception = exc
        except globus_sdk.exc.GlobusConnectionError as exc:
            last_exception = exc

    # If none of the endpoints succeeded
    if active_endpoint_id is None:
        if isinstance(last_exception, globus_sdk.exc.GlobusConnectionError):
            raise HTTPException(
                status_code=503,
                detail="Failed to connect to Globus services. All JLab endpoints may be down.",
            ) from last_exception
        elif isinstance(last_exception, globus_sdk.exc.GlobusAPIError):
            raise HTTPException(
                status_code=400,
                detail=f"Globus API error (Code: {last_exception.code}): {last_exception.message}",
            ) from last_exception
        else:
            raise HTTPException(
                status_code=500,
                detail="Unknown error occurred while connecting to Globus endpoints.",
            )

    return tc, active_endpoint_id


# handle ls through globus
async def ls(
    adapter,
    resource: status_models.Resource,
    user: User,
    path: str,
    show_hidden: bool,
    numeric_uid: bool,
    recursive: bool,
    dereference: bool,
    transfer_token: str | None = None,
) -> filesystem_models.GetDirectoryLsResponse:
    tc, active_endpoint_id = _get_active_globus_transfer_client(
        resource, path, transfer_token
    )

    try:
        files = _fetch_globus_ls(
            tc,
            active_endpoint_id,
            path,
            show_hidden=show_hidden,
            recursive=recursive,
            user_name=user.name,
        )
        return filesystem_models.GetDirectoryLsResponse(output=files)
    except globus_sdk.exc.GlobusAPIError as exc:
        if exc.http_status == 404 or exc.code in ("NotFound", "ClientError.NotFound"):
            raise HTTPException(
                status_code=404, detail=f"Directory or file not found: {path}"
            ) from exc
        raise HTTPException(
            status_code=400,
            detail=f"Globus API error (Code: {exc.code}): {exc.message}",
        ) from exc


def _headtail(
    adapter,
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

    rp = adapter.validate_path(path)
    args.append(rp)

    result = adapter._run(args)
    return result.stdout


async def head(
    adapter,
    resource: status_models.Resource,
    user: User,
    path: str,
    file_bytes: int | None,
    lines: int | None,
    skip_trailing: bool = False,
) -> filesystem_models.GetFileHeadResponse:
    raise HTTPException(
        status_code=501,
        detail="head is not implemented by the Jefferson Lab LQCD adapter due to infrastructure limitation and security policies.",
    )


async def tail(
    adapter,
    resource: status_models.Resource,
    user: User,
    path: str,
    file_bytes: int | None,
    lines: int | None,
    skip_heading: bool = False,
) -> filesystem_models.GetFileTailResponse:
    raise HTTPException(
        status_code=501,
        detail="tail is not implemented by the Jefferson Lab LQCD adapter due to infrastructure limitation and security policies.",
    )


async def view(
    adapter,
    resource: status_models.Resource,
    user: User,
    path: str,
    size: int,
    offset: int,
) -> filesystem_models.GetViewFileResponse:
    raise HTTPException(
        status_code=501,
        detail="view is not implemented by the Jefferson Lab LQCD adapter due to infrastructure limitation and security policies.",
    )


async def checksum(
    adapter, resource: status_models.Resource, user: User, path: str
) -> filesystem_models.GetFileChecksumResponse:
    raise HTTPException(
        status_code=501,
        detail="checksum is not implemented by the Jefferson Lab LQCD adapter due to infrastructure limitation and security policies.",
    )


async def file(
    adapter,
    resource: status_models.Resource,
    user: User,
    path: str,
    transfer_token: str | None,
) -> filesystem_models.GetFileTypeResponse:
    ls_rep: filesystem_models.GetDirectoryLsResponse = await ls(
        adapter,
        resource,
        user,
        path,
        show_hidden=False,
        numeric_uid=False,
        recursive=False,
        dereference=False,
        transfer_token=transfer_token,
    )
    if ls_rep.output is None:
        raise HTTPException(status_code=404, detail=f"File not found: {path}")
    for file in ls_rep.output:
        if file.name == path:
            return filesystem_models.GetFileTypeResponse(
                output=file.type,
            )
    raise HTTPException(status_code=404, detail=f"File not found: {path}")


async def stat(
    adapter,
    resource: status_models.Resource,
    user: User,
    path: str,
    dereference: bool,
    transfer_token: str | None,
) -> filesystem_models.GetFileStatResponse:
    tc, active_endpoint_id = _get_active_globus_transfer_client(
        resource, path, transfer_token
    )

    try:
        stat_res = tc.operation_stat(active_endpoint_id, path=path)
    except globus_sdk.exc.GlobusAPIError as exc:
        if exc.http_status == 404 or exc.code in ("NotFound", "ClientError.NotFound"):
            raise HTTPException(
                status_code=404, detail=f"File or directory not found: {path}"
            ) from exc
        raise HTTPException(
            status_code=400,
            detail=f"Globus API error (Code: {exc.code}): {exc.message}",
        ) from exc

    file_type = stat_res.get("type", "file")
    if file_type == "dir":
        type_bitmask = 0o040000
    elif file_type == "symlink":
        type_bitmask = 0o120000
    else:
        type_bitmask = 0o100000

    perm_str = stat_res.get("permissions")
    perm_bits = 0o644 if file_type != "dir" else 0o755
    if perm_str:
        try:
            perm_bits = int(str(perm_str), 8)
        except Exception:
            pass

    mode = type_bitmask | perm_bits
    size = stat_res.get("size", 0)

    last_modified_str = stat_res.get("last_modified")
    mtime = 0
    if last_modified_str:
        try:
            clean_ts = str(last_modified_str).replace("Z", "+00:00")
            dt = datetime.datetime.fromisoformat(clean_ts)
            mtime = int(dt.timestamp())
        except Exception:
            mtime = 0

    uid = 0
    user_val = stat_res.get("user")
    if user_val is not None:
        try:
            uid = int(user_val)
        except (ValueError, TypeError):
            uid = 0

    gid = 0
    group_val = stat_res.get("group")
    if group_val is not None:
        try:
            gid = int(group_val)
        except (ValueError, TypeError):
            gid = 0

    file_stat = filesystem_models.FileStat(
        mode=mode,
        ino=0,
        dev=0,
        nlink=1,
        uid=uid,
        gid=gid,
        size=size,
        atime=mtime,
        ctime=mtime,
        mtime=mtime,
    )

    return filesystem_models.GetFileStatResponse(output=file_stat)


# Remove or delete operation on a globus file or directory
async def rm(
    adapter,
    resource: status_models.Resource,
    user: User,
    path: str,
    transfer_token: str | None = None,
) -> filesystem_models.RemoveResponse:
    tc, active_endpoint_id = _get_active_globus_transfer_client(
        resource, path, transfer_token
    )

    delete_data = globus_sdk.DeleteData(
        endpoint=active_endpoint_id,
        label=f"Delete request by {user.name}",
        recursive=True,
    )
    delete_data.add_item(path)

    try:
        res = tc.submit_delete(delete_data)
        task_id = res.get("task_id", "")
        return filesystem_models.RemoveResponse(
            output=f"Submitted deletion request for '{path}'. Globus Task ID: {task_id}"
        )
    except globus_sdk.exc.GlobusAPIError as exc:
        if exc.http_status == 404 or exc.code in ("NotFound", "ClientError.NotFound"):
            raise HTTPException(
                status_code=404, detail=f"File or directory not found: {path}"
            ) from exc
        raise HTTPException(
            status_code=400,
            detail=f"Globus API error (Code: {exc.code}): {exc.message}",
        ) from exc


async def mkdir(
    adapter,
    resource: status_models.Resource,
    user: User,
    request_model: filesystem_models.PostMakeDirRequest,
    transfer_token: str | None = None,
) -> filesystem_models.PostMkdirResponse:
    if request_model.path is None:
        raise HTTPException(status_code=400, detail="Path is required")

    dir_path = request_model.path

    tc, active_endpoint_id = _get_active_globus_transfer_client(
        resource, dir_path, transfer_token
    )

    try:
        if request_model.parent:
            # Create parent directories recursively if requested
            parts = pathlib.PurePosixPath(dir_path).parts
            if len(parts) > 1:
                current = ""
                for part in parts:
                    if not part or part == "/":
                        current = "/"
                        continue
                    current = os.path.join(current, part)
                    try:
                        tc.operation_mkdir(active_endpoint_id, path=current)
                    except globus_sdk.exc.GlobusAPIError:
                        pass
        else:
            tc.operation_mkdir(active_endpoint_id, path=dir_path)
    except globus_sdk.exc.GlobusAPIError as exc:
        if exc.http_status in (400, 409) or "Exists" in str(exc.code):
            raise HTTPException(
                status_code=409, detail=f"Directory already exists: {dir_path}"
            ) from exc
        raise HTTPException(
            status_code=400,
            detail=f"Globus API error (Code: {exc.code}): {exc.message}",
        ) from exc

    created_dir = filesystem_models.File(
        name=dir_path,
        type="directory",
        user=user.name,
        group="",
        permissions="rwxr-xr-x",
        last_modified=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        size="0",
    )

    return filesystem_models.PostMkdirResponse(output=created_dir)


async def symlink(
    adapter,
    resource: status_models.Resource,
    user: User,
    request_model: filesystem_models.PostFileSymlinkRequest,
) -> filesystem_models.PostFileSymlinkResponse:
    raise HTTPException(
        status_code=501,
        detail="symlink is not implemented by the Jefferson Lab LQCD adapter due to infrastructure limitation and security policies.",
    )


async def download(
    adapter,
    resource: status_models.Resource,
    user: User,
    path: str,
    transfer_token: str | None = None,
) -> filesystem_models.GetFileDownloadResponse:
    if not path:
        raise HTTPException(status_code=400, detail="Path is required")

    tc, active_endpoint_id = _get_active_globus_transfer_client(
        resource, path, transfer_token
    )
    local_endpoint_id = _get_local_endpoint_id()

    local_dest_dir = PathSandbox.get_base_temp_dir()
    os.makedirs(local_dest_dir, exist_ok=True)
    local_dest_path = os.path.join(local_dest_dir, os.path.basename(path))
    logger.info(f"Downloading '{path}' to '{local_dest_path}'")

    tdata = globus_sdk.TransferData(
        source_endpoint=active_endpoint_id,
        destination_endpoint=local_endpoint_id,
        label=f"Download request by {user.name}",
    )
    tdata.add_item(path, local_dest_path)

    try:
        res = tc.submit_transfer(tdata)
        task_id = res.get("task_id", "")

        while not tc.task_wait(task_id, timeout=300, polling_interval=10):
            pass

        task_info = tc.get_task(task_id)
        if task_info.get("status") == "FAILED":
            fatal_error = task_info.get("fatal_error") or "Transfer failed"
            raise HTTPException(
                status_code=400, detail=f"Globus download transfer failed: {fatal_error}"
            )

        return filesystem_models.GetFileDownloadResponse(
            output=f"Downloaded '{path}' to '{local_dest_path}'. Globus Task ID: {task_id}"
        )
    except globus_sdk.exc.GlobusAPIError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Globus API error (Code: {exc.code}): {exc.message}",
        ) from exc


async def upload(
    adapter,
    resource: status_models.Resource,
    user: User,
    path: str,
    content: str,
    transfer_token: str | None = None,
) -> filesystem_models.PutFileUploadResponse:
    if not path:
        raise HTTPException(status_code=400, detail="Path is required")

    tc, active_endpoint_id = _get_active_globus_transfer_client(
        resource, path, transfer_token
    )
    local_endpoint_id = _get_local_endpoint_id()

    local_src_dir = PathSandbox.get_base_temp_dir()
    os.makedirs(local_src_dir, exist_ok=True)
    local_src_path = os.path.join(local_src_dir, f"upload_{os.path.basename(path)}")

    if isinstance(content, bytes):
        raw_bytes = content
    elif isinstance(content, str):
        try:
            raw_bytes = base64.b64decode(content)
        except Exception:
            raw_bytes = content.encode("utf-8")
    else:
        raise HTTPException(
            status_code=400, detail=f"Unsupported content type: {type(content)}"
        )

    pathlib.Path(local_src_path).write_bytes(raw_bytes)

    tdata = globus_sdk.TransferData(
        source_endpoint=local_endpoint_id,
        destination_endpoint=active_endpoint_id,
        label=f"Upload request by {user.name}",
        sync_level="checksum",
    )
    tdata.add_item(local_src_path, path)

    try:
        res = tc.submit_transfer(tdata)
        task_id = res.get("task_id", "")

        while not tc.task_wait(task_id, timeout=300, polling_interval=10):
            pass

        task_info = tc.get_task(task_id)
        if task_info.get("status") == "FAILED":
            fatal_error = task_info.get("fatal_error") or "Transfer failed"
            raise HTTPException(
                status_code=400, detail=f"Globus upload transfer failed: {fatal_error}"
            )

        return filesystem_models.PutFileUploadResponse(
            output=f"Uploaded to {path}. Globus Task ID: {task_id}"
        )
    except globus_sdk.exc.GlobusAPIError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Globus API error (Code: {exc.code}): {exc.message}",
        ) from exc


async def compress(
    adapter,
    resource: status_models.Resource,
    user: User,
    request_model: filesystem_models.PostCompressRequest,
) -> filesystem_models.PostCompressResponse:
    raise HTTPException(
        status_code=501,
        detail="Compress is not implemented by the Jefferson Lab LQCD adapter due to infrastructure limitation and security policies.",
    )


async def extract(
    adapter,
    resource: status_models.Resource,
    user: User,
    request_model: filesystem_models.PostExtractRequest,
) -> filesystem_models.PostExtractResponse:
    raise HTTPException(
        status_code=501,
        detail="Extract is not implemented by the Jefferson Lab LQCD adapter due to infrastructure limitation and security policies.",
    )


async def mv(
    adapter,
    resource: status_models.Resource,
    user: User,
    request_model: filesystem_models.PostMoveRequest,
    transfer_token: str | None = None,
) -> filesystem_models.PostMoveResponse:
    if request_model.path is None:
        raise HTTPException(status_code=400, detail="Path is required")
    if request_model.target_path is None:
        raise HTTPException(status_code=400, detail="Target path is required")

    # Check both source and target paths are valid for entered resource
    tc, active_endpoint_id = _get_active_globus_transfer_client(
        resource, request_model.path, transfer_token
    )
    # check target path to be valid
    _validate_resource_path(resource, request_model.target_path)

    try:
        tc.operation_rename(
            active_endpoint_id,
            oldpath=request_model.path,
            newpath=request_model.target_path,
        )
    except globus_sdk.exc.GlobusAPIError as exc:
        if exc.http_status == 404 or exc.code in ("NotFound", "ClientError.NotFound"):
            raise HTTPException(
                status_code=404, detail=f"File or directory not found: {request_model.path}"
            ) from exc
        raise HTTPException(
            status_code=400,
            detail=f"Globus API error (Code: {exc.code}): {exc.message}",
        ) from exc

    moved_file = filesystem_models.File(
        name=request_model.target_path,
        type="file",
        user=user.name,
        group="",
        permissions="",
        last_modified=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        size="0",
    )

    return filesystem_models.PostMoveResponse(output=moved_file)


async def cp(
    adapter,
    resource: status_models.Resource,
    user: User,
    request_model: filesystem_models.PostCopyRequest,
) -> filesystem_models.PostCopyResponse:
    raise HTTPException(
        status_code=501,
        detail="Copy is not implemented for the Jefferson Lab LQCD endpoint.",
    )

async def transfer(
    adapter,
    resource: status_models.Resource,
    user: User,
    dest_resource: status_models.Resource,
    request_model: filesystem_models.PostCopyRequest,
    transfer_token: str | None = None,
) -> filesystem_models.PostCopyResponse:
    # check whether source resource and destination resource are the same
    if resource.id == dest_resource.id:
        raise HTTPException(
            status_code=400,
            detail="Source and destination resources must be different.",
        )
    # Get source path and destination path and verify they are valid
    source_path = request_model.path
    dest_path = request_model.target_path
    if not source_path:
        raise HTTPException(status_code=400, detail="Source path is required")
    if not dest_path:
        raise HTTPException(status_code=400, detail="Destination path is required")

    logger.info(f"Transfer source file {source_path} from {resource.name} to {dest_path} at {dest_resource.name}")

    # Check both source and target paths are valid for entered resources and get active endpoint IDs
    tc, source_endpoint_id = _get_active_globus_transfer_client(
        resource, source_path, transfer_token
    )
    _, dest_endpoint_id = _get_active_globus_transfer_client(
        dest_resource, dest_path, transfer_token
    )

    # Check if source_path is a directory
    is_dir = False
    try:
        tc.operation_ls(source_endpoint_id, path=source_path)
        is_dir = True
    except globus_sdk.exc.GlobusAPIError:
        is_dir = False

    tdata = globus_sdk.TransferData(
        source_endpoint=source_endpoint_id,
        destination_endpoint=dest_endpoint_id,
        label=f"Transfer request by {user.name} from {resource.name} to {dest_resource.name}",
        sync_level="checksum",
    )
    tdata.add_item(source_path, dest_path, recursive=is_dir)

    try:
        res = tc.submit_transfer(tdata)
        task_id = res.get("task_id", "")

        while not tc.task_wait(task_id, timeout=300, polling_interval=10):
            pass

        task_info = tc.get_task(task_id)
        if task_info.get("status") == "FAILED":
            fatal_error = task_info.get("fatal_error") or "Transfer failed"
            raise HTTPException(
                status_code=400, detail=f"Globus transfer failed: {fatal_error}"
            )

        transferred_file = filesystem_models.File(
            name=dest_path,
            type="directory" if is_dir else "file",
            user=user.name,
            group="",
            permissions="",
            last_modified=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            size="0",
        )

        return filesystem_models.PostCopyResponse(output=transferred_file)
    except globus_sdk.exc.GlobusAPIError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Globus API error (Code: {exc.code}): {exc.message}",
        ) from exc