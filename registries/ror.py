import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from pymongo import ASCENDING, IndexModel

from pipelineFramework import binary_stream, normalize_key
from . import matching

SOURCE = "ror"

ZENODO_API = "https://zenodo.org/api"
# Concept record for DOI 10.5281/zenodo.6347574; its links.latest always points at the newest release.
ZENODO_CONCEPT_RECORD = "6347574"
DATA_MEMBER = "v2.*.json"


def parse_zenodo_release(record: Dict[str, Any]) -> Tuple[str, str]:
    version = (record.get("metadata") or {}).get("version") or record.get("id")
    if not version:
        raise RuntimeError(f"Zenodo record has no version or id. Top-level keys: {sorted(record)}")

    files = record.get("files") or []
    archives = [entry for entry in files if str(entry.get("key", "")).lower().endswith(".zip")]
    if not archives:
        raise RuntimeError(
            f"No .zip asset in ROR Zenodo release {version}. Files: {[entry.get('key') for entry in files]}"
        )
    archive = max(archives, key=lambda entry: entry.get("size") or 0)
    url = (archive.get("links") or {}).get("self") or archive.get("link")
    if not url:
        raise RuntimeError(f"ROR asset {archive.get('key')} has no download link. Keys: {sorted(archive)}")
    return str(version), str(url)


def _names(record: Dict[str, Any]) -> Dict[str, Any]:
    display: Optional[str] = None
    labels: List[str] = []
    aliases: List[str] = []
    acronyms: List[str] = []

    for entry in record.get("names") or []:
        value = entry.get("value")
        if not value:
            continue
        types = set(entry.get("types") or [])
        if "ror_display" in types:
            display = value
        if "label" in types:
            labels.append(value)
        if "alias" in types:
            aliases.append(value)
        if "acronym" in types:
            acronyms.append(value)

    every = [name for name in [display, *labels, *aliases, *acronyms] if name]
    return {
        "display": display or (every[0] if every else None),
        "labels": labels,
        "aliases": aliases,
        "acronyms": acronyms,
        "all": every,
    }


def _location(record: Dict[str, Any]) -> Dict[str, Any]:
    for location in record.get("locations") or []:
        details = location.get("geonames_details") or {}
        return {
            "city": details.get("name"),
            "state": details.get("country_subdivision_name"),
            "state_code": details.get("country_subdivision_code"),
            "country": details.get("country_name"),
            "country_code": details.get("country_code"),
            # GeoNames city centroid, not the campus - consumers must record this as city precision.
            "lat": details.get("lat"),
            "lon": details.get("lng"),
            "geonames_id": location.get("geonames_id"),
        }
    return {}


def _links(record: Dict[str, Any]) -> Dict[str, Any]:
    website = None
    wikipedia = None
    for link in record.get("links") or []:
        if link.get("type") == "website" and not website:
            website = link.get("value")
        elif link.get("type") == "wikipedia" and not wikipedia:
            wikipedia = link.get("value")
    return {"website": website, "wikipedia": wikipedia}


def _external_ids(record: Dict[str, Any]) -> Dict[str, Any]:
    external: Dict[str, Any] = {}
    for entry in record.get("external_ids") or []:
        identifier_type = entry.get("type")
        if identifier_type:
            external[identifier_type] = entry.get("preferred") or (entry.get("all") or [None])[0]
    return external


def to_document(record: Dict[str, Any]) -> Dict[str, Any]:
    ror_url = record.get("id") or ""
    names = _names(record)
    return {
        "_id": ror_url.rsplit("/", 1)[-1],
        "ror_url": ror_url,
        "status": record.get("status"),
        "established": record.get("established"),
        "types": record.get("types") or [],
        "names": names,
        "search_names": sorted({normalize_key(name) for name in names["all"] if name}),
        "domains": record.get("domains") or [],
        "location": _location(record),
        "relationships": [
            {"type": rel.get("type"), "id": (rel.get("id") or "").rsplit("/", 1)[-1], "label": rel.get("label")}
            for rel in record.get("relationships") or []
        ],
        "external_ids": _external_ids(record),
        **_links(record),
    }


def iter_documents(path: Path) -> Iterator[Dict[str, Any]]:
    with binary_stream(path, DATA_MEMBER) as stream:
        for record in json.load(stream):
            yield to_document(record)


def indexes() -> List[IndexModel]:
    return [
        IndexModel([("search_names", ASCENDING)], name="search_names"),
        IndexModel([("domains", ASCENDING)], name="domains"),
        IndexModel([("location.country_code", ASCENDING)], name="country"),
    ]


CHUNK_SIZE = 1000
POOL_LIMIT = 50_000
PREFIX_LIMIT = 50


def _chunks(values: List[str], size: int = CHUNK_SIZE) -> Iterator[List[str]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


async def load_by_names(collection, keys: Iterable[str]) -> Dict[str, List[Dict[str, Any]]]:
    wanted = {key for key in keys if key}
    index: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for chunk in _chunks(sorted(wanted)):
        async for document in collection.find({"search_names": {"$in": chunk}}):
            for key in document.get("search_names") or []:
                if key in wanted:
                    index.setdefault(key, {})[document["_id"]] = document
    return {key: list(documents.values()) for key, documents in index.items()}


async def load_by_domains(collection, domains: Iterable[str]) -> Dict[str, List[Dict[str, Any]]]:
    wanted = {domain for domain in domains if domain}
    index: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for chunk in _chunks(sorted(wanted)):
        async for document in collection.find({"domains": {"$in": chunk}}):
            for domain in document.get("domains") or []:
                if domain in wanted:
                    index.setdefault(domain, {})[document["_id"]] = document
    return {domain: list(documents.values()) for domain, documents in index.items()}


async def load_by_ids(collection, identifiers: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    wanted = {identifier.rsplit("/", 1)[-1] for identifier in identifiers if identifier}
    found: Dict[str, Dict[str, Any]] = {}
    for chunk in _chunks(sorted(wanted)):
        async for document in collection.find({"_id": {"$in": chunk}}):
            found[document["_id"]] = document
    return found


async def load_pool(collection, country_code: Optional[str] = None, limit: int = POOL_LIMIT) -> List[Dict[str, Any]]:
    query = {"location.country_code": country_code} if country_code else {}
    return [document async for document in collection.find(query).limit(limit)]


async def load_by_name_prefix(
    collection, prefix: str, country_code: Optional[str] = None, limit: int = PREFIX_LIMIT
) -> List[Dict[str, Any]]:
    if len(prefix) < 3:
        return []
    query: Dict[str, Any] = {"search_names": {"$regex": f"^{re.escape(prefix)}"}}
    if country_code:
        query["location.country_code"] = country_code
    return [document async for document in collection.find(query).limit(limit)]


def names_of(document: Dict[str, Any]) -> List[str]:
    names = document.get("names") or {}
    return [name for name in names.get("all") or [] if name]


def candidates(documents: Iterable[Dict[str, Any]]) -> List[Tuple[str, str, Dict[str, Any]]]:
    pool: List[Tuple[str, str, Dict[str, Any]]] = []
    for document in documents:
        for folded in matching.fold_names(*names_of(document)):
            pool.append((folded, document["_id"], document))
    return pool


def address_of(document: Dict[str, Any]) -> Dict[str, Optional[str]]:
    location = document.get("location") or {}
    return {
        "country": location.get("country"),
        "country_code": location.get("country_code"),
        "state": location.get("state"),
        "city": location.get("city"),
        "postal_code": None,
        "street": None,
        "house_number": None,
    }


def coordinates_of(document: Dict[str, Any]) -> Optional[Tuple[float, float]]:
    location = document.get("location") or {}
    latitude, longitude = location.get("lat"), location.get("lon")
    if not matching.is_valid_coordinate(latitude, longitude):
        return None
    return float(latitude), float(longitude)


def is_active(document: Dict[str, Any]) -> bool:
    return (document.get("status") or "active") == "active"
