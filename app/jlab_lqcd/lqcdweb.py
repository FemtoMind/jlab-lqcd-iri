# Access Jlab lqcd web page through https url and get some information
import httpx
import json
from pydantic import BaseModel, Field


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
        raise Exception(f"Failed to get data from {url}")

    data = response.json()
    lqcd_events: list[JlabLqcdEvent] = []
    if isinstance(data, list):
        for item in data:
            lqcd_events.append(JlabLqcdEvent.from_json(item))
    else:
        raise Exception(f"Invalid data format from {url}")

    url_old_events = "https://lqcd.jlab.org/lqcd2/news?type=lqcd&display=0"
    async with httpx.AsyncClient(verify=False) as client:
        response = await client.get(url_old_events)
    if response.status_code != 200:
        raise Exception(f"Failed to get data from {url_old_events}")

    data = response.json()
    if isinstance(data, list):
        for item in data:
            lqcd_events.append(JlabLqcdEvent.from_json(item))
    else:
        raise Exception(f"Invalid data format from {url_old_events}")

    return lqcd_events
