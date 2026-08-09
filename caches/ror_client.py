"""Example client: ROR (Research Organization Registry) lookup by name."""

import requests

from pipelineFramework import BaseEnrichmentClient


class RorClient(BaseEnrichmentClient):
    provider = "ror"
    query_type = "org_name"
    BASE_URL = "https://api.ror.org/v2/organizations"

    def _fetch(self, query: str) -> tuple:
        resp = requests.get(self.BASE_URL, params={"query": query}, timeout=10)
        resp.raise_for_status()
        data = resp.json()

        items = data.get("items", [])
        if not items:
            return None, {"url": resp.url}

        # Your matching logic lives here, e.g. pick highest score, or apply
        # your own scoring/disambiguation on top of what ROR returns.
        best = items[0]
        result = {
            "ror_id": best["id"],
            "matched_name": best["name"],
            "score": best.get("score"),
        }
        source = {"url": resp.url, "candidate_count": len(items)}
        return result, source
