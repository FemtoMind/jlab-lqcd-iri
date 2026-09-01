"""Utility helper functions for the IRI Facility API JLab LQCD implementation."""

import pwd
import os
import uuid
from .apilogger import get_stream_logger
from .config import LOG_LEVEL

logger = get_stream_logger(__name__, LOG_LEVEL)


def demo_uuid(kind: str, name: str) -> str:
    """Generate a deterministic UUID based on the kind and name."""
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"jlab-lqcd:demo:{kind}:{name}"))


# Get process owner
def get_process_owner() -> str | None:
    try:
        # Get the real user ID of the current process
        uid = os.getuid()
        # Look up the username associated with that UID
        user_info = pwd.getpwuid(uid)
        return user_info.pw_name
    except Exception as e:
        print(f"Error getting process owner: {e}")
        return None


class PathSandbox:
    """A simple sandbox for file operations."""

    _base_temp_dir: str | None = os.environ.get("IRI_DOWNLOAD_DIR")

    @classmethod
    def get_base_temp_dir(cls) -> str:
        """Get the base temporary directory for the sandbox."""
        if cls._base_temp_dir is None:
            # Create in system temp with a fixed name
            cls._base_temp_dir = os.path.join(os.getcwd(), "iri_sandbox")
            os.makedirs(cls._base_temp_dir, exist_ok=True)

            # create a test file
            with open(f"{cls._base_temp_dir}/test.txt", encoding="utf-8", mode="w") as f:
                f.write("hello world")
            logger.info(f"Created test file in sandbox: {cls._base_temp_dir}/test.txt")
        return cls._base_temp_dir

