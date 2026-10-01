import os
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
from urllib.parse import parse_qsl, urlencode

import aiohttp
from pydantic import BaseModel

from pipelineFramework import BaseEnrichmentClient, EnrichmentCache, FetchResult
from user_agent import DEFAULT_HEADERS

DEFAULT_BASE_URL = "http://photon:2322"

SEARCH = "search"
STRUCTURED = "structured"

# Photon's own feature type, from most to least precise.
PRECISION_BY_TYPE = {
    "house": "rooftop",
    "street": "street",
    "locality": "building",
    "district": "city",
    "city": "city",
    "county": "region",
    "state": "region",
    "country": "country",
    "other": "building",
}


@dataclass(frozen=True)
class PhotonQuery:
    mode: str
    params: Tuple[Tuple[str, str], ...]

    @classmethod
    def search(
        cls,
        text: str,
        country_code: Optional[str] = None,
        city: Optional[str] = None,
        layer: Optional[str] = None,
    ) -> "PhotonQuery":
        query = text if not city or city.lower() in text.lower() else f"{text}, {city}"
        params = [("q", query)]
        if country_code:
            params.append(("countrycode", country_code.lower()))
        if layer:
            params.append(("layer", layer))
        return cls(SEARCH, tuple(sorted(params)))

    @classmethod
    def structured(cls, **fields: Optional[str]) -> "PhotonQuery":
        if fields.get("countrycode"):
            fields["countrycode"] = fields["countrycode"].lower()
        params = [(key, value) for key, value in sorted(fields.items()) if value]
        return cls(STRUCTURED, tuple(sorted(params)))

    def as_query(self) -> str:
        return urlencode([("mode", self.mode), *sorted(self.params)])

    @classmethod
    def parse(cls, query: str) -> "PhotonQuery":
        pairs = parse_qsl(query, keep_blank_values=False)
        mode = next((value for key, value in pairs if key == "mode"), SEARCH)
        return cls(mode, tuple((key, value) for key, value in pairs if key != "mode"))


class PhotonPlace(BaseModel):
    lat: float
    lon: float
    type: Optional[str] = None
    name: Optional[str] = None
    street: Optional[str] = None
    housenumber: Optional[str] = None
    postcode: Optional[str] = None
    city: Optional[str] = None
    district: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None
    countrycode: Optional[str] = None
    osm_key: Optional[str] = None
    osm_value: Optional[str] = None
    osm_id: Optional[int] = None

    @property
    def precision(self) -> str:
        return PRECISION_BY_TYPE.get(self.type or "other", "building")


class PhotonClient(BaseEnrichmentClient[PhotonPlace]):
    provider = "photon"
    query_type = "place"
    result_model = PhotonPlace
    match_version = 1

    LIMIT = 1
    LANGUAGE = "de"
    TIMEOUT = aiohttp.ClientTimeout(total=15)
    PROPERTIES = set(PhotonPlace.model_fields) - {"lat", "lon"}

    def __init__(
        self,
        cache: EnrichmentCache,
        base_url: Optional[str] = None,
        session: Optional[aiohttp.ClientSession] = None,
    ):
        super().__init__(cache)
        self.base_url = (base_url or os.environ.get("PHOTON_URL") or DEFAULT_BASE_URL).strip().rstrip("/")
        self.session = session
        self.requests = 0
        self.lookups = 0

    @property
    def cache_hits(self) -> int:
        return self.lookups - self.requests

    async def lookup_place(self, query: PhotonQuery) -> Optional[PhotonPlace]:
        self.lookups += 1
        return await self.lookup(query.as_query())

    async def _fetch(self, query: str) -> FetchResult[PhotonPlace]:
        parsed = PhotonQuery.parse(query)
        endpoint = STRUCTURED if parsed.mode == STRUCTURED else "api"
        params = [*parsed.params, ("limit", str(self.LIMIT)), ("lang", self.LANGUAGE)]

        self.requests += 1
        if self.session is not None:
            payload, url = await self._request(self.session, endpoint, params)
        else:
            async with aiohttp.ClientSession(timeout=self.TIMEOUT, headers=DEFAULT_HEADERS) as session:
                payload, url = await self._request(session, endpoint, params)

        features = payload.get("features") or []
        if not features:
            return FetchResult.not_found(source={"url": url})

        feature = features[0]
        coordinates = ((feature.get("geometry") or {}).get("coordinates")) or []
        if len(coordinates) < 2:
            return FetchResult.not_found(source={"url": url})

        properties = feature.get("properties") or {}
        return FetchResult(
            result=PhotonPlace(
                lon=float(coordinates[0]),
                lat=float(coordinates[1]),
                **{key: properties.get(key) for key in self.PROPERTIES},
            ),
            source={"url": url, "candidate_count": len(features)},
        )

    async def _request(self, session: aiohttp.ClientSession, endpoint: str, params: list) -> Tuple[Dict[str, Any], str]:
        async with session.get(f"{self.base_url}/{endpoint}", params=params, timeout=self.TIMEOUT) as response:
            response.raise_for_status()
            return await response.json(content_type=None), str(response.url)
