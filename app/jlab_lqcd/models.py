# Some commom data models for Jlab-LCDQ Project
from pydantic import Field
from ..types.base import IRIBaseModel


class SlurmProject(IRIBaseModel):
    """Slurm Project"""

    name: str = Field(..., description="Project name")
    users: list[str] = Field(..., description="List of users in the project")
