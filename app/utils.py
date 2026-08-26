"""Utility helper functions for the IRI Facility API JLab LQCD implementation."""
import pwd
import os
import uuid


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
