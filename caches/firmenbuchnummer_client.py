from typing import Optional

import aiohttp
from pydantic import BaseModel

from pipelineFramework import BaseEnrichmentClient, FetchResult


class FirmenbuchEntry(BaseModel):
    """TODO: replace with the fields the Firmenbuch provider actually returns."""

    firmenbuchnummer: str
    matched_name: str
    score: Optional[float] = None


class FirmenbuchnummerClient(BaseEnrichmentClient[FirmenbuchEntry]):
    provider = "firmenbuchnummer"
    query_type = "org_name"
    result_model = FirmenbuchEntry
    # TODO: still pointing at ROR - swap in the real Firmenbuch endpoint.
    BASE_URL = "https://api.ror.org/v2/organizations"
    TIMEOUT = aiohttp.ClientTimeout(total=10)

    async def _fetch(self, query: str) -> FetchResult[FirmenbuchEntry]:
        async with aiohttp.ClientSession(timeout=self.TIMEOUT) as session:
            async with session.get(self.BASE_URL, params={"query": query}) as response:
                response.raise_for_status()
                data = await response.json()
                url = str(response.url)

        items = data.get("items", [])
        if not items:
            return FetchResult.not_found(source={"url": url})

        # TODO: real matching logic against the Firmenbuch response.
        best = items[0]
        return FetchResult(
            result=FirmenbuchEntry(
                firmenbuchnummer=best["id"],
                matched_name=best["name"],
                score=best.get("score"),
            ),
            source={"url": url, "candidate_count": len(items)},
        )
