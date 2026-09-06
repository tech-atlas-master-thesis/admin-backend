from typing import Optional

import aiohttp
from pydantic import BaseModel

from pipelineFramework import BaseEnrichmentClient, FetchResult


class GeoResultModel(BaseModel):
    """TODO: replace with the fields the geo provider actually returns."""

    lat: float
    lon: float
    display_name: Optional[str] = None


class GeoClient(BaseEnrichmentClient[GeoResultModel]):
    provider = "geo"
    query_type = "org_name"
    result_model = GeoResultModel
    # TODO: still pointing at ROR - swap in the real geocoding endpoint.
    BASE_URL = "https://api.ror.org/v2/organizations"
    TIMEOUT = aiohttp.ClientTimeout(total=10)

    async def _fetch(self, query: str) -> FetchResult[GeoResultModel]:
        async with aiohttp.ClientSession(timeout=self.TIMEOUT) as session:
            async with session.get(self.BASE_URL, params={"query": query}) as response:
                response.raise_for_status()
                data = await response.json()
                url = str(response.url)

        items = data.get("items", [])
        if not items:
            return FetchResult.not_found(source={"url": url})

        # TODO: real matching logic against the geocoding response.
        best = items[0]
        return FetchResult(
            result=GeoResultModel(
                lat=best["lat"],
                lon=best["lon"],
                display_name=best.get("display_name"),
            ),
            source={"url": url, "candidate_count": len(items)},
        )
