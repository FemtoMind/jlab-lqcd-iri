"""Utility helper functions for the IRI Facility API JLab LQCD implementation."""

import uuid

def demo_uuid(kind: str, name: str) -> str:
    """Generate a deterministic UUID based on the kind and name."""
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"jlab-lqcd:demo:{kind}:{name}"))
