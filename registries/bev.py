import csv
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

from pymongo import ASCENDING, IndexModel
from pyproj import Transformer

from pipelineFramework import normalize_key, text_stream

SOURCE = "bev"

# BEV dates its filenames; verify against the current listing at https://data.bev.gv.at/geonetwork
# and override with the step's SOURCE_URL if it moved.
DEFAULT_URL = "https://data.bev.gv.at/download/Adressregister/Archiv_Adressregister/Adresse_Relationale_Tabellen_Stichtagsdaten_20251001.zip"

DELIMITER = ";"
ENCODING = "utf-8-sig"

MEMBER_ADRESSE = "ADRESSE.csv"
MEMBER_STRASSE = "STRASSE.csv"
MEMBER_GEMEINDE = "GEMEINDE.csv"
MEMBER_ORTSCHAFT = "ORTSCHAFT.csv"

COL_ADRCD = "ADRCD"
COL_SKZ = "SKZ"
COL_GKZ = "GKZ"
COL_OKZ = "OKZ"
COL_PLZ = "PLZ"
COL_RW = "RW"
COL_HW = "HW"
COL_EPSG = "EPSG"
COL_STRASSENNAME = "STRASSENNAME"
COL_GEMEINDENAME = "GEMEINDENAME"
COL_ORTSNAME = "ORTSNAME"

BUNDESLAND_BY_GKZ_PREFIX = {
    "1": "Burgenland",
    "2": "Kärnten",
    "3": "Niederösterreich",
    "4": "Oberösterreich",
    "5": "Salzburg",
    "6": "Steiermark",
    "7": "Tirol",
    "8": "Vorarlberg",
    "9": "Wien",
}


@lru_cache(maxsize=16)
def _transformer(epsg: int) -> Transformer:
    return Transformer.from_crs(f"EPSG:{epsg}", "EPSG:4326", always_xy=True)


def _require_columns(header: Optional[List[str]], required: List[str], member: str) -> None:
    if header is None:
        raise ValueError(f"{member} has no header row")
    missing = [column for column in required if column not in header]
    if missing:
        raise ValueError(f"{member} is missing expected column(s) {missing}. Header: {header[:40]}")


def _to_float(raw: Optional[str]) -> Optional[float]:
    value = (raw or "").strip().replace(",", ".")
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _house_number(row: Dict[str, str]) -> Optional[str]:
    first = f"{(row.get('HAUSNRZAHL1') or '').strip()}{(row.get('HAUSNRBUCHSTABE1') or '').strip()}"
    second = f"{(row.get('HAUSNRZAHL2') or '').strip()}{(row.get('HAUSNRBUCHSTABE2') or '').strip()}"
    number = "-".join(part for part in (first, second) if part)
    return number or (row.get("HAUSNRTEXT") or "").strip() or None


def _load_lookup(path: Path, member: str, key: str, value: str) -> Dict[str, str]:
    lookup: Dict[str, str] = {}
    with text_stream(path, member, encoding=ENCODING) as stream:
        reader = csv.DictReader(stream, delimiter=DELIMITER, skipinitialspace=True)
        _require_columns(reader.fieldnames, [key, value], member)
        for row in reader:
            identifier = (row.get(key) or "").strip()
            if identifier:
                lookup[identifier] = (row.get(value) or "").strip()
    return lookup


def to_document(
    row: Dict[str, str],
    streets: Dict[str, str],
    municipalities: Dict[str, str],
    localities: Dict[str, str],
) -> Optional[Dict[str, Any]]:
    easting = _to_float(row.get(COL_RW))
    northing = _to_float(row.get(COL_HW))
    epsg = (row.get(COL_EPSG) or "").strip()
    if easting is None or northing is None or not epsg.isdigit():
        return None

    longitude, latitude = _transformer(int(epsg)).transform(easting, northing)

    gkz = (row.get(COL_GKZ) or "").strip()
    street = streets.get((row.get(COL_SKZ) or "").strip()) or None
    municipality = municipalities.get(gkz) or None
    locality = localities.get((row.get(COL_OKZ) or "").strip()) or None
    house_number = _house_number(row)
    postal_code = (row.get(COL_PLZ) or "").strip() or None

    return {
        "_id": (row.get(COL_ADRCD) or "").strip(),
        "street": street,
        "house_number": house_number,
        "postal_code": postal_code,
        "city": locality or municipality,
        "municipality": municipality,
        "state": BUNDESLAND_BY_GKZ_PREFIX.get(gkz[:1]) if gkz else None,
        "lat": latitude,
        "lon": longitude,
        "source_crs": f"EPSG:{epsg}",
        "street_key": normalize_key(street) if street else None,
        "search_key": normalize_key(" ".join(part for part in (street, house_number, postal_code) if part)),
    }


def iter_documents(path: Path) -> Iterator[Dict[str, Any]]:
    streets = _load_lookup(path, MEMBER_STRASSE, COL_SKZ, COL_STRASSENNAME)
    municipalities = _load_lookup(path, MEMBER_GEMEINDE, COL_GKZ, COL_GEMEINDENAME)
    localities = _load_lookup(path, MEMBER_ORTSCHAFT, COL_OKZ, COL_ORTSNAME)

    with text_stream(path, MEMBER_ADRESSE, encoding=ENCODING) as stream:
        reader = csv.DictReader(stream, delimiter=DELIMITER)
        _require_columns(reader.fieldnames, [COL_ADRCD, COL_SKZ, COL_RW, COL_HW, COL_EPSG], MEMBER_ADRESSE)
        for row in reader:
            document = to_document(row, streets, municipalities, localities)
            if document and document["_id"]:
                yield document


def indexes() -> List[IndexModel]:
    return [
        IndexModel([("search_key", ASCENDING)], name="search_key"),
        IndexModel([("postal_code", ASCENDING), ("street_key", ASCENDING)], name="postal_code_street"),
        IndexModel([("street_key", ASCENDING)], name="street_key"),
    ]


CHUNK_SIZE = 200
CENTROID_SAMPLE = 400


def street_key(street: Optional[str]) -> Optional[str]:
    return normalize_key(street) if street else None


def _chunks(values: List[Any], size: int = CHUNK_SIZE) -> Iterator[List[Any]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


async def load_streets(collection, requests: Iterable[Tuple[str, str]]) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
    wanted = sorted({(postal_code, key) for postal_code, key in requests if postal_code and key})
    found: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for chunk in _chunks(wanted):
        query = {"$or": [{"postal_code": postal_code, "street_key": key} for postal_code, key in chunk]}
        async for document in collection.find(query):
            found.setdefault((document.get("postal_code"), document.get("street_key")), []).append(document)
    return found


def pick_address(documents: List[Dict[str, Any]], house_number: Optional[str]) -> Optional[Tuple[Dict[str, Any], str]]:
    if not documents:
        return None
    if house_number:
        wanted = normalize_key(house_number)
        for document in documents:
            if normalize_key(document.get("house_number") or "") == wanted:
                return document, "rooftop"
        # A house number that is not in the register still pins the street it belongs to.
        stem = "".join(character for character in wanted if character.isdigit())
        for document in documents:
            digits = "".join(character for character in (document.get("house_number") or "") if character.isdigit())
            if stem and digits == stem:
                return document, "rooftop"
    return documents[0], "street"


async def load_postal_code_centroid(collection, postal_code: str) -> Optional[Tuple[float, float, Dict[str, Any]]]:
    documents = [document async for document in collection.find({"postal_code": postal_code}).limit(CENTROID_SAMPLE)]
    points = [
        (document["lat"], document["lon"])
        for document in documents
        if document.get("lat") is not None and document.get("lon") is not None
    ]
    if not points:
        return None
    latitude = sum(point[0] for point in points) / len(points)
    longitude = sum(point[1] for point in points) / len(points)
    return latitude, longitude, {"sampled": len(points), "postal_code": postal_code}


def address_of(document: Dict[str, Any]) -> Dict[str, Optional[str]]:
    return {
        "country": "Österreich",
        "country_code": "AT",
        "state": document.get("state"),
        "city": document.get("city"),
        "postal_code": document.get("postal_code"),
        "street": document.get("street"),
        "house_number": document.get("house_number"),
    }
