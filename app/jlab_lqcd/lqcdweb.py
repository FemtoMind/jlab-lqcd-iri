# Access Jlab lqcd web page through https url and get some information
import httpx
import json
from pydantic import BaseModel, Field
from ..apilogger import get_stream_logger
from ..config import LOG_LEVEL

logger = get_stream_logger(__name__, LOG_LEVEL)


class JlabLqcdEvent(BaseModel):
    id: int = Field(description="Event ID", default=0)
    subject: str = Field(description="Event subject", default="N/A")
    content: str = Field(description="Event content", default="N/A")
    ctime: int = Field(description="Event creation time in seconds since epoch", default=0)
    display: bool = Field(description="Whether to display the event", default=False)

    # Convert to json string
    def to_json(self):
        return self.model_dump_json()

    # Convert from json string
    @staticmethod
    def from_json(json_str: str | dict):
        if isinstance(json_str, dict):
            return JlabLqcdEvent(**json_str)
        return JlabLqcdEvent(**json.loads(json_str))


# get data from https://lqcd.jlab.org/lqcd2/news?type=lqcd
async def get_lqcd_cluster_events() -> list[JlabLqcdEvent]:
    url = "https://lqcd.jlab.org/lqcd2/news?type=lqcd&display=1"
    async with httpx.AsyncClient(verify=False) as client:
        response = await client.get(url)
    if response.status_code != 200:
        logger.warning(f"Failed to get data from {url}")
        return []

    data = response.json()
    lqcd_events: list[JlabLqcdEvent] = []
    if isinstance(data, list):
        for item in data:
            lqcd_events.append(JlabLqcdEvent.from_json(item))
    else:
        logger.warning(f"Invalid data format from {url}")
        return []

    url_old_events = "https://lqcd.jlab.org/lqcd2/news?type=lqcd&display=0"
    async with httpx.AsyncClient(verify=False) as client:
        response = await client.get(url_old_events)
    if response.status_code != 200:
        logger.warning(f"Failed to get data from {url_old_events}")
        return []

    data = response.json()
    if isinstance(data, list):
        for item in data:
            lqcd_events.append(JlabLqcdEvent.from_json(item))
    else:
        logger.warning(f"Invalid data format from {url_old_events}")
        return []

    return lqcd_events


# Get total cache size in TB
async def get_total_cache_size() -> int:
    url = "https://lqcd.jlab.org/lqcd2/cache?type=size"
    async with httpx.AsyncClient(verify=False) as client:
        response = await client.get(url)
    if response.status_code != 200:
        logger.warning(f"Failed to get data from {url}")
        return 0

    data = response.text
    parts = data.split()
    if len(parts) < 2:
        logger.warning(f"Invalid data format from {url}")
        return 0
    return int(parts[0])


# Get total work disk size in TB
async def get_total_work_disk_size() -> float:
    url = "https://lqcd.jlab.org/lqcd2/hpcWork?type=workChart"
    async with httpx.AsyncClient(verify=False) as client:
        response = await client.get(url)
    if response.status_code != 200:
        logger.warning(f"Failed to get workdisk data from {url}")
        return 0

    data = response.json()
    if isinstance(data, list):
        wdsize = float(data[0]["size"])
    else:
        wdsize = float(data["size"])

    return wdsize


# Get total volatile disk size in TB
async def get_total_volatile_disk_size() -> int:
    url = "https://lqcd.jlab.org/lqcd2/volatile?type=size"
    async with httpx.AsyncClient(verify=False) as client:
        response = await client.get(url)
    if response.status_code != 200:
        logger.warning(f"Failed to get volatile disk data from {url}")
        return 0

    data = response.text
    # data looks like 500 TB
    parts = data.split()
    if len(parts) < 2:
        logger.warning(f"Invalid data format from {url}")
        return 0
    return int(parts[0])
