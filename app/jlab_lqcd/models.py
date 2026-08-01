# Some commom data models for Jlab-LCDQ Project
import datetime
from pydantic import Field, computed_field, field_validator
from ..request_context import get_url_prefix
from ..types.base import IRIBaseModel
from ..types.scalars import AllocationUnit

class SlurmProject(IRIBaseModel):
    """Slurm Project"""
    name: str = Field(..., description="Project name")
    users: list[str] = Field(..., description="List of users in the project")
    
