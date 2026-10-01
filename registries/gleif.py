import csv
import re
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from pymongo import ASCENDING, IndexModel

from pipelineFramework import normalize_key, text_stream
from . import matching

SOURCE = "gleif"
COUNTRY_CODE = "AT"

GOLDEN_COPY_API = "https://goldencopy.gleif.org/api/v2/golden-copies/publishes?scope=concatenated_files"
DATA_MEMBER = "*.csv"

COLUMN_LEI = "LEI"
COLUMN_LEGAL_NAME = "Entity.LegalName"
COLUMN_ENTITY_STATUS = "Entity.EntityStatus"
COLUMN_LEGAL_FORM = "Entity.LegalForm.EntityLegalFormCode"
COLUMN_REGISTRATION_STATUS = "Registration.RegistrationStatus"
COLUMN_RA_ID = "Entity.RegistrationAuthority.RegistrationAuthorityID"
COLUMN_RA_ENTITY_ID = "Entity.RegistrationAuthority.RegistrationAuthorityEntityID"

LEGAL_ADDRESS_PREFIX = "Entity.LegalAddress"
HEADQUARTERS_PREFIX = "Entity.HeadquartersAddress"
COUNTRY_COLUMN = f"{LEGAL_ADDRESS_PREFIX}.Country"
ADDRESS_FIELDS = {
    "street": "FirstAddressLine",
    "additional": "AdditionalAddressLine.1",
    "city": "City",
    "region": "Region",
    "country": "Country",
    "postal_code": "PostalCode",
}

# Matched by shape rather than by GLEIF's registration-authority code for the Firmenbuch,
# because that code lives in a separately published list we have not verified. Every document
# keeps registration_authority_id so the real code can be read off a loaded snapshot.
FIRMENBUCHNUMMER_PATTERN = re.compile(r"^(?:FN\s*)?(\d{1,6})\s*([a-z])$", re.IGNORECASE)


def parse_golden_copy_publish(payload: Dict[str, Any]) -> Tuple[str, str]:
    entries = payload.get("data") or []
    if not entries:
        raise RuntimeError(f"GLEIF golden-copy response contained no publishes. Keys: {sorted(payload)}")

    latest = entries[0]
    version = latest.get("publish_date") or latest.get("id")
    if not version:
        raise RuntimeError(f"GLEIF publish has no publish_date. Keys: {sorted(latest)}")

    node: Any = latest.get("lei2") or {}
    for key in ("full_file", "csv", "url"):
        if not isinstance(node, dict):
            break
        node = node.get(key)
    if not isinstance(node, str):
        raise RuntimeError(
            f"No lei2.full_file.csv.url in GLEIF publish {version}. lei2 keys: {sorted(latest.get('lei2') or {})}"
        )
    return str(version), node


def _require_columns(header: Optional[List[str]], required: List[str], member: str) -> None:
    if header is None:
        raise ValueError(f"{member} has no header row")
    missing = [column for column in required if column not in header]
    if missing:
        raise ValueError(f"{member} is missing expected column(s) {missing}. Header: {header[:40]}")


def _address(row: Dict[str, str], prefix: str) -> Dict[str, Optional[str]]:
    return {target: (row.get(f"{prefix}.{suffix}") or "").strip() or None for target, suffix in ADDRESS_FIELDS.items()}


def parse_firmenbuchnummer(raw: Optional[str]) -> Optional[str]:
    match = FIRMENBUCHNUMMER_PATTERN.match((raw or "").strip())
    return f"{match.group(1)}{match.group(2).lower()}" if match else None


def to_document(row: Dict[str, str]) -> Dict[str, Any]:
    legal_name = (row.get(COLUMN_LEGAL_NAME) or "").strip()
    registration_entity_id = (row.get(COLUMN_RA_ENTITY_ID) or "").strip() or None
    return {
        "_id": (row.get(COLUMN_LEI) or "").strip(),
        "legal_name": legal_name,
        "search_name": normalize_key(legal_name) if legal_name else None,
        "entity_status": (row.get(COLUMN_ENTITY_STATUS) or "").strip() or None,
        "registration_status": (row.get(COLUMN_REGISTRATION_STATUS) or "").strip() or None,
        "legal_form_code": (row.get(COLUMN_LEGAL_FORM) or "").strip() or None,
        "registration_authority_id": (row.get(COLUMN_RA_ID) or "").strip() or None,
        "registration_authority_entity_id": registration_entity_id,
        "firmenbuchnummer": parse_firmenbuchnummer(registration_entity_id),
        "legal_address": _address(row, LEGAL_ADDRESS_PREFIX),
        "headquarters_address": _address(row, HEADQUARTERS_PREFIX),
    }


def iter_documents(path: Path) -> Iterator[Dict[str, Any]]:
    csv.field_size_limit(1 << 24)
    with text_stream(path, DATA_MEMBER, encoding="utf-8") as stream:
        reader = csv.DictReader(stream)
        _require_columns(reader.fieldnames, [COLUMN_LEI, COLUMN_LEGAL_NAME, COUNTRY_COLUMN], DATA_MEMBER)
        for row in reader:
            if (row.get(COUNTRY_COLUMN) or "").strip().upper() != COUNTRY_CODE:
                continue
            document = to_document(row)
            if document["_id"]:
                yield document


def indexes() -> List[IndexModel]:
    return [
        IndexModel([("search_name", ASCENDING)], name="search_name"),
        IndexModel([("firmenbuchnummer", ASCENDING)], name="firmenbuchnummer", sparse=True),
        IndexModel([("legal_address.postal_code", ASCENDING)], name="postal_code"),
    ]


CHUNK_SIZE = 1000
POOL_LIMIT = 50_000


def _chunks(values: List[str], size: int = CHUNK_SIZE) -> Iterator[List[str]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


async def load_by_names(collection, keys: Iterable[str]) -> Dict[str, List[Dict[str, Any]]]:
    wanted = {key for key in keys if key}
    index: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for chunk in _chunks(sorted(wanted)):
        async for document in collection.find({"search_name": {"$in": chunk}}):
            key = document.get("search_name")
            if key in wanted:
                index.setdefault(key, {})[document["_id"]] = document
    return {key: list(documents.values()) for key, documents in index.items()}


async def load_pool(collection, limit: int = POOL_LIMIT) -> List[Dict[str, Any]]:
    return [document async for document in collection.find({}).limit(limit)]


def candidates(documents: Iterable[Dict[str, Any]]) -> List[Tuple[str, str, Dict[str, Any]]]:
    pool: List[Tuple[str, str, Dict[str, Any]]] = []
    for document in documents:
        for folded in matching.fold_names(document.get("legal_name")):
            pool.append((folded, document["_id"], document))
    return pool


def address_of(document: Dict[str, Any]) -> Dict[str, Optional[str]]:
    # The legal address is the registered seat; headquarters only fills what it leaves empty.
    legal = document.get("legal_address") or {}
    headquarters = document.get("headquarters_address") or {}
    merged = {field: legal.get(field) or headquarters.get(field) for field in ADDRESS_FIELDS}
    street, house_number = matching.split_street(merged.get("street"))
    return {
        "country": merged.get("country"),
        "country_code": (merged.get("country") or "").strip().upper() or None,
        "state": matching.state_name(merged.get("region")),
        "city": merged.get("city"),
        "postal_code": merged.get("postal_code"),
        "street": street,
        "house_number": house_number,
    }


def is_active(document: Dict[str, Any]) -> bool:
    return (document.get("entity_status") or "ACTIVE").upper() == "ACTIVE"
