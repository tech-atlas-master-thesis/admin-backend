from typing import Optional

import aiohttp
from pydantic import BaseModel

from pipelineFramework import BaseEnrichmentClient, FetchResult


class RorOrganisation(BaseModel):
    ror_id: str
    matched_name: str
    score: Optional[float] = None


class RorClient(BaseEnrichmentClient[RorOrganisation]):
    provider = "ror"
    query_type = "org_name"
    result_model = RorOrganisation
    BASE_URL = "https://api.ror.org/v2/organizations"
    TIMEOUT = aiohttp.ClientTimeout(total=10)

    async def _fetch(self, query: str) -> FetchResult[RorOrganisation]:
        async with aiohttp.ClientSession(timeout=self.TIMEOUT) as session:
            async with session.get(self.BASE_URL, params={"query": query}) as response:
                response.raise_for_status()
                data = await response.json()
                url = str(response.url)

        items = data.get("items", [])
        if not items:
            return FetchResult.not_found(source={"url": url})

        # Your matching logic lives here, e.g. pick highest score, or apply
        # your own scoring/disambiguation on top of what ROR returns.
        # NOTE: `best["name"]` is the v1 response shape; the v2 endpoint above
        # returns a `names` list instead. Check this against a live response.
        best = items[0]
        return FetchResult(
            result=RorOrganisation(
                ror_id=best["id"],
                matched_name=best["name"],
                score=best.get("score"),
            ),
            source={"url": url, "candidate_count": len(items)},
        )
