import json
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import ijson
from pymongo import ASCENDING, IndexModel

from pipelineFramework import binary_stream, normalize_key

SOURCE = "ror"
COUNTRY_CODE = "AT"

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


def is_austrian(record: Dict[str, Any]) -> bool:
    return any(
        (location.get("geonames_details") or {}).get("country_code") == COUNTRY_CODE
        for location in record.get("locations") or []
    )


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
            if is_austrian(record):
                yield to_document(record)


def indexes() -> List[IndexModel]:
    return [
        IndexModel([("search_names", ASCENDING)], name="search_names"),
        IndexModel([("domains", ASCENDING)], name="domains"),
        IndexModel([("location.country_code", ASCENDING)], name="country"),
    ]
