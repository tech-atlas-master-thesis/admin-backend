import asyncio
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union

import aiohttp

from caches import PhotonClient, PhotonQuery
from pipelineFramework import (
    EnrichmentCache,
    Event,
    EventType,
    LocalisationString,
    LocalisationStringType,
    SnapshotStore,
    StepConfig,
    StepUserConfig,
    UserStepConfig,
    get_cache_db_client,
    get_registry_db_client,
    normalize_key,
)
from pipeline_configs.transform_steps.organisations_normalize import OrganisationNormalizeStep
from registries import bev, gleif, matching, ror
from user_agent import DEFAULT_HEADERS

ADDRESS_FIELDS = ("country", "country_code", "state", "city", "postal_code", "street", "house_number")

# Most precise first; a later stage only overwrites a coordinate it can beat.
PRECISION_RANK = {"rooftop": 5, "street": 4, "building": 3, "city": 2, "region": 1, "country": 0}

# What the scrapers deliver is never overwritten; among the registries an address that was looked
# up street by street replaces the city ROR derived from a centroid.
ADDRESS_RANK = {"source": 4, "bev": 3, "gleif": 2, "photon": 1, "ror": 0}


@dataclass
class Settings:
    fuzzy_threshold: float = 90.0
    fuzzy_margin: float = 4.0
    geocode_concurrency: int = 8
    pool_limit: int = 50_000
    photon_url: Optional[str] = None
    photon_failure_limit: int = 5


@dataclass
class OrganisationRecord:
    key: str
    source: Dict[str, Any]
    name: str
    primary_key: str
    name_keys: List[str]
    fold_names: List[str]
    domain: Optional[str]
    source_ror: Optional[str]
    address: Dict[str, Optional[str]]
    match: Dict[str, Any]
    website: Optional[str] = None
    ror_id: Optional[str] = None
    firmenbuchnummer: Optional[str] = None
    geo: Optional[Dict[str, Any]] = None
    precision: Optional[str] = None
    centroid: Optional[Tuple[float, float]] = None
    warnings: List[str] = field(default_factory=list)

    @classmethod
    def build(cls, key: str, organisation: Dict[str, Any]) -> "OrganisationRecord":
        name = matching.clean(organisation.get("name")) or matching.clean(key) or key
        source_address = organisation.get("address") or {}
        street, house_number = matching.split_street(source_address.get("street"))
        address: Dict[str, Optional[str]] = {
            "country": matching.clean(source_address.get("country")),
            "country_code": matching.country_code_of(source_address.get("country")),
            "state": matching.clean(source_address.get("state")),
            "city": matching.clean(source_address.get("city")),
            "postal_code": matching.postal_code_of(source_address.get("postal_code")),
            "street": street,
            "house_number": house_number,
        }
        domain = matching.domain_of(organisation.get("website"))
        source_ror = matching.clean(organisation.get("ror"))
        source_ror = source_ror.rsplit("/", 1)[-1] if source_ror else None
        keys = matching.name_keys(name)
        return cls(
            key=key,
            source=organisation,
            name=name,
            primary_key=normalize_key(name),
            name_keys=keys,
            fold_names=matching.fold_names(name, matching.strip_legal_forms(name)),
            domain=domain,
            source_ror=source_ror,
            address=address,
            match={
                "keys": {
                    "name": keys,
                    "domain": domain,
                    "city": matching.city_keys(source_address.get("city")),
                    "country_code": address["country_code"],
                },
                "address": {field_name: "source" for field_name, value in address.items() if value},
                "ror": {"status": "unmatched"},
                "gleif": {"status": "unmatched"},
                "geo": {"status": "unmatched"},
            },
            website=matching.clean(organisation.get("website")),
            ror_id=source_ror,
            firmenbuchnummer=matching.clean(organisation.get("firmenbuchnummer")),
        )

    @property
    def country_code(self) -> Optional[str]:
        return self.address.get("country_code")

    @property
    def is_austrian(self) -> bool:
        return self.country_code in (None, "AT")

    def fill_address(self, values: Dict[str, Optional[str]], source: str) -> None:
        rank = ADDRESS_RANK[source]
        for field_name in ADDRESS_FIELDS:
            value = matching.clean(values.get(field_name))
            if not value:
                continue
            held = self.match["address"].get(field_name)
            if self.address.get(field_name) and rank <= ADDRESS_RANK.get(held, ADDRESS_RANK["source"]):
                continue
            self.address[field_name] = value
            self.match["address"][field_name] = source

    def add_names(self, names: List[str]) -> None:
        keys = set(self.name_keys)
        folded = set(self.fold_names)
        for name in names:
            keys.update(matching.name_keys(name))
            folded.update(matching.fold_names(name))
        self.name_keys = sorted(keys)
        self.fold_names = sorted(folded)

    def set_geo(self, latitude: float, longitude: float, precision: str, source: Dict[str, Any]) -> bool:
        if not matching.is_valid_coordinate(latitude, longitude):
            return False
        if self.country_code == "AT" and not matching.in_austria(latitude, longitude):
            self.warnings.append(f"{source.get('source')} put {self.name} outside Austria - discarded")
            return False
        if self.precision and PRECISION_RANK.get(precision, 0) <= PRECISION_RANK.get(self.precision, 0):
            return False
        self.geo = matching.point(latitude, longitude)
        self.precision = precision
        self.match["geo"] = {"status": "matched", "precision": precision, **source}
        return True

    def as_organisation(self) -> Dict[str, Any]:
        return {
            **self.source,
            "name": self.name,
            "website": self.website,
            "ror": self.ror_id,
            "firmenbuchnummer": self.firmenbuchnummer,
            "address": dict(self.address),
            "geo": self.geo,
            "_match": {**self.match, "warnings": self.warnings},
        }


class OrganisationEnrichStep(StepConfig):
    SOURCES = ("ror", "gleif", "bev")

    async def run(
        self,
        user_config: Optional[UserStepConfig] = None,
        results: Optional[Dict[str, Any]] = None,
        warnings: Optional[List[Event]] = None,
        **_,
    ):
        if results is None:
            results = {}
        organisations: Dict[str, Dict[str, Any]] = results.get(OrganisationNormalizeStep.name())
        if organisations is None:
            raise FileNotFoundError("No organisation data found")
        warnings = warnings if warnings is not None else []
        settings = self.settings(user_config)

        registry_db = get_registry_db_client()
        store = SnapshotStore(registry_db)
        snapshots: Dict[str, Dict[str, Any]] = {}
        collections = {}
        for source in self.SOURCES:
            active = await store.require_active(source)
            snapshots[source] = {"version": active.version, "collection": active.collection}
            collections[source] = registry_db[active.collection]
        yield (
            "Registry snapshots: " + ", ".join(f"{source} {state['version']}" for source, state in snapshots.items()),
            EventType.INFO,
        )

        started = time.perf_counter()
        records = [OrganisationRecord.build(key, organisation) for key, organisation in organisations.items()]
        for record in records:
            record.match["snapshots"] = snapshots
        yield (
            f"Prepared match keys for {len(records)} organisations "
            f"({sum(1 for r in records if r.domain)} with a usable domain, "
            f"{sum(1 for r in records if r.address['city'])} with a city, "
            f"{sum(1 for r in records if r.ror_id)} with a ROR id from the source) "
            f"in {time.perf_counter() - started:.2f}s",
            EventType.INFO,
        )

        stats = await self.match_ror(collections["ror"], records, settings)
        yield self.describe("ROR", stats), EventType.INFO

        stats = await self.match_gleif(collections["gleif"], records, settings)
        yield self.describe("GLEIF", stats), EventType.INFO

        cache = EnrichmentCache(get_cache_db_client())
        async with aiohttp.ClientSession(headers=DEFAULT_HEADERS) as session:
            client = PhotonClient(cache, base_url=settings.photon_url, session=session)
            await client.ensure_indexes()
            stats = await self.geocode(collections["bev"], records, settings, client)
        yield self.describe("Geocoding", stats), EventType.INFO

        for record in records:
            for message in record.warnings:
                warnings.append(Event.now(message, EventType.WARNING))

        yield self.coverage(records), EventType.INFO
        yield {record.key: record.as_organisation() for record in records}, EventType.RESULT

    async def match_ror(self, collection, records: List[OrganisationRecord], settings: Settings) -> Dict[str, Any]:
        started = time.perf_counter()
        by_name = await ror.load_by_names(collection, {key for record in records for key in record.name_keys})
        by_domain = await ror.load_by_domains(collection, {record.domain for record in records if record.domain})
        by_id = await ror.load_by_ids(collection, {record.ror_id for record in records if record.ror_id})
        scanned = sum(len(documents) for documents in by_name.values()) + len(by_id)

        methods: Counter = Counter()
        pools: Dict[str, List[Tuple[str, str, Dict[str, Any]]]] = {}

        for record in records:
            document, method, score, ambiguous = None, None, None, False

            if record.source_ror:
                document = by_id.get(record.source_ror)
                if document is None:
                    record.match["ror"] = {"status": "source_only", "id": record.source_ror}
                    methods["source_only"] += 1
                    continue
                method, score = "source_id", 100.0

            if document is None and record.domain:
                candidates = self.plausible(record, by_domain.get(record.domain, []))
                if len(candidates) == 1:
                    document, method, score = candidates[0], "domain", 100.0

            if document is None:
                for key in record.name_keys:
                    candidates = self.plausible(record, by_name.get(key, []))
                    active = [candidate for candidate in candidates if ror.is_active(candidate)] or candidates
                    if len(active) == 1:
                        document, method, score = active[0], "name", 100.0
                        break
                    if len(active) > 1:
                        ambiguous = True
                        record.warnings.append(f'ROR name "{key}" matches {len(active)} organisations - left empty')
                        break

            if document is None and not ambiguous:
                pool = await self.ror_pool(collection, record, pools, settings)
                found = matching.best_match(record.fold_names, pool, settings.fuzzy_threshold, settings.fuzzy_margin)
                if found:
                    document, method, score = found[0], "fuzzy", found[1]

            if document is None:
                record.match["ror"] = {"status": "ambiguous" if ambiguous else "unmatched"}
                methods["unmatched"] += 1
                continue

            self.apply_ror(record, document, method, score)
            methods[method] += 1

        return {
            "matched": sum(count for method, count in methods.items() if method != "unmatched"),
            "total": len(records),
            "methods": methods,
            "scanned": scanned,
            "elapsed": time.perf_counter() - started,
        }

    async def ror_pool(
        self,
        collection,
        record: OrganisationRecord,
        pools: Dict[str, List[Tuple[str, str, Dict[str, Any]]]],
        settings: Settings,
    ) -> List[Tuple[str, str, Dict[str, Any]]]:
        if record.is_austrian:
            if "AT" not in pools:
                pools["AT"] = ror.candidates(await ror.load_pool(collection, "AT", settings.pool_limit))
            return pools["AT"]
        # A worldwide fuzzy pass would be both slow and ambiguous, so foreign organisations are
        # narrowed by their country and the first token of their name before scoring.
        prefix = record.primary_key.split(" ")[0]
        return ror.candidates(await ror.load_by_name_prefix(collection, prefix, record.country_code))

    def apply_ror(self, record: OrganisationRecord, document: Dict[str, Any], method: str, score: float) -> None:
        names = document.get("names") or {}
        record.ror_id = document["_id"]
        record.match["ror"] = {
            "status": "matched",
            "method": method,
            "score": round(float(score), 1),
            "id": document["_id"],
            "name": names.get("display"),
            "record_status": document.get("status"),
        }
        if not record.website:
            record.website = matching.clean(document.get("website"))
        record.fill_address(ror.address_of(document), "ror")
        record.add_names(ror.names_of(document))
        record.centroid = ror.coordinates_of(document)

    async def match_gleif(self, collection, records: List[OrganisationRecord], settings: Settings) -> Dict[str, Any]:
        started = time.perf_counter()
        # GLEIF only carries Austrian entities, and only they can have a Firmenbuchnummer.
        wanted = [record for record in records if record.is_austrian]
        by_name = await gleif.load_by_names(collection, {key for record in wanted for key in record.name_keys})
        scanned = sum(len(documents) for documents in by_name.values())

        methods: Counter = Counter()
        pool: Optional[List[Tuple[str, str, Dict[str, Any]]]] = None

        for record in records:
            if not record.is_austrian:
                record.match["gleif"] = {"status": "skipped", "reason": "not an Austrian organisation"}
                continue

            document, method, score, ambiguous = None, None, None, False
            for key in record.name_keys:
                candidates = by_name.get(key, [])
                active = [candidate for candidate in candidates if gleif.is_active(candidate)] or candidates
                if len(active) == 1:
                    document, method, score = active[0], "name", 100.0
                    break
                if len(active) > 1:
                    ambiguous = True
                    record.warnings.append(f'GLEIF name "{key}" matches {len(active)} entities - left empty')
                    break

            if document is None and not ambiguous:
                if pool is None:
                    pool = gleif.candidates(await gleif.load_pool(collection, settings.pool_limit))
                found = matching.best_match(record.fold_names, pool, settings.fuzzy_threshold, settings.fuzzy_margin)
                if found:
                    document, method, score = found[0], "fuzzy", found[1]

            if document is None:
                record.match["gleif"] = {"status": "ambiguous" if ambiguous else "unmatched"}
                methods["unmatched"] += 1
                continue

            record.match["gleif"] = {
                "status": "matched",
                "method": method,
                "score": round(float(score), 1),
                "lei": document["_id"],
                "legal_name": document.get("legal_name"),
                "registration_authority_id": document.get("registration_authority_id"),
            }
            if not record.firmenbuchnummer:
                record.firmenbuchnummer = document.get("firmenbuchnummer")
            record.fill_address(gleif.address_of(document), "gleif")
            methods[method] += 1

        return {
            "matched": sum(count for method, count in methods.items() if method != "unmatched"),
            "total": len(wanted),
            "methods": methods,
            "scanned": scanned + (len(pool) if pool else 0),
            "elapsed": time.perf_counter() - started,
            "extra": f"{sum(1 for record in records if record.firmenbuchnummer)} Firmenbuchnummer(n)",
        }

    async def geocode(
        self, collection, records: List[OrganisationRecord], settings: Settings, client: PhotonClient
    ) -> Dict[str, Any]:
        started = time.perf_counter()
        sources: Counter = Counter()

        scanned = await self.bev_pass(collection, records, sources)

        semaphore = asyncio.Semaphore(max(1, settings.geocode_concurrency))
        failures: List[str] = []

        async def resolve(record: OrganisationRecord) -> None:
            async with semaphore:
                if len(failures) >= settings.photon_failure_limit:
                    return
                try:
                    await self.photon(record, client, sources)
                except (aiohttp.ClientError, asyncio.TimeoutError) as error:
                    failures.append(str(error))
                    record.warnings.append(f"Photon lookup failed for {record.name}: {error}")

        await asyncio.gather(*(resolve(record) for record in records if record.geo is None))

        # Photon hands back the postal code and street that BEV needs, so the register is asked a
        # second time for everything that is still coarser than a rooftop coordinate.
        scanned += await self.bev_pass(collection, records, sources)

        for record in records:
            if record.geo is None and record.centroid:
                latitude, longitude = record.centroid
                if record.set_geo(latitude, longitude, "city", {"source": "ror", "id": record.ror_id}):
                    sources["ror/city"] += 1

        for record in records:
            if record.geo is None and record.is_austrian and record.address["postal_code"]:
                centroid = await bev.load_postal_code_centroid(collection, record.address["postal_code"])
                if centroid and record.set_geo(centroid[0], centroid[1], "city", {"source": "bev", **centroid[2]}):
                    sources["bev/postal_code"] += 1

        return {
            "matched": sum(1 for record in records if record.geo),
            "total": len(records),
            "methods": sources,
            "scanned": scanned,
            "elapsed": time.perf_counter() - started,
            "extra": (
                f"{client.requests} Photon request(s), {client.cache_hits} cache hit(s)"
                + (f", {len(failures)} failure(s)" if failures else "")
            ),
        }

    async def bev_pass(self, collection, records: List[OrganisationRecord], sources: Counter) -> int:
        wanted = [
            record
            for record in records
            if record.is_austrian
            and record.precision != "rooftop"
            and record.address["postal_code"]
            and record.address["street"]
        ]
        streets = await bev.load_streets(
            collection,
            {(record.address["postal_code"], bev.street_key(record.address["street"])) for record in wanted},
        )
        for record in wanted:
            found = streets.get((record.address["postal_code"], bev.street_key(record.address["street"])))
            picked = bev.pick_address(found or [], record.address["house_number"])
            if picked is None:
                continue
            document, precision = picked
            if record.set_geo(document["lat"], document["lon"], precision, {"source": "bev", "id": document["_id"]}):
                record.fill_address(bev.address_of(document), "bev")
                sources[f"bev/{precision}"] += 1
        return sum(len(documents) for documents in streets.values())

    async def photon(self, record: OrganisationRecord, client: PhotonClient, sources: Counter) -> None:
        country_code = record.country_code
        city = (matching.city_keys(record.address["city"]) or [None])[-1]

        if record.address["street"] and (record.address["postal_code"] or city):
            query = PhotonQuery.structured(
                street=record.address["street"],
                housenumber=record.address["house_number"],
                postcode=record.address["postal_code"],
                city=city,
                countrycode=country_code,
            )
            if await self.apply_photon(record, client, query, sources, "structured"):
                return

        if await self.apply_photon(
            record, client, PhotonQuery.search(record.name, country_code, city), sources, "search"
        ):
            return

        # A ROR city centroid is the same precision as a city geocode and costs no request,
        # so Photon is only asked for the city when the organisation has no centroid.
        if city and record.centroid is None:
            await self.apply_photon(
                record, client, PhotonQuery.search(city, country_code, layer="city"), sources, "city"
            )

    async def apply_photon(
        self,
        record: OrganisationRecord,
        client: PhotonClient,
        query: PhotonQuery,
        sources: Counter,
        label: str,
    ) -> bool:
        place = await client.lookup_place(query)
        if place is None:
            return False
        if country_code := record.country_code:
            if place.countrycode and place.countrycode.upper() != country_code:
                return False
        if place.precision == "country":
            return False
        if label == "search" and not self.same_city(record, place):
            record.warnings.append(
                f'Photon put "{record.name}" in {place.city or place.state} rather than '
                f'{record.address["city"]} - discarded'
            )
            return False
        if not record.set_geo(place.lat, place.lon, place.precision, {"source": "photon", "query": query.as_query()}):
            return False
        record.fill_address(
            {
                "country": place.country,
                "country_code": (place.countrycode or "").upper() or None,
                "state": place.state,
                "city": place.city or place.district,
                "postal_code": place.postcode,
                "street": place.street,
                "house_number": place.housenumber,
            },
            "photon",
        )
        sources[f"photon/{label}/{place.precision}"] += 1
        return True

    def same_city(self, record: OrganisationRecord, place) -> bool:
        wanted = {matching.fold(key) for key in matching.city_keys(record.address["city"])}
        found = {matching.fold(value) for value in (place.city, place.district, place.state) if value}
        if not wanted or not found:
            return True
        return any(key in value or value in key for key in wanted for value in found)

    def plausible(self, record: OrganisationRecord, documents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not record.country_code:
            return list(documents)
        return [
            document
            for document in documents
            if not (document.get("location") or {}).get("country_code")
            or (document.get("location") or {}).get("country_code") == record.country_code
        ]

    def describe(self, stage: str, stats: Dict[str, Any]) -> str:
        methods = ", ".join(f"{method} {count}" for method, count in sorted(stats["methods"].items()))
        message = (
            f"{stage}: {stats['matched']}/{stats['total']} resolved"
            f"{f' ({methods})' if methods else ''}"
            f" - {stats['scanned']:,} registry documents scanned in {stats['elapsed']:.1f}s"
        )
        return f"{message} - {stats['extra']}" if stats.get("extra") else message

    def coverage(self, records: List[OrganisationRecord]) -> str:
        total = len(records) or 1
        filled = {
            "ROR": sum(1 for record in records if record.ror_id),
            "Firmenbuchnummer": sum(1 for record in records if record.firmenbuchnummer),
            "street": sum(1 for record in records if record.address["street"]),
            "coordinates": sum(1 for record in records if record.geo),
        }
        precisions = Counter(record.precision for record in records if record.precision)
        summary = ", ".join(f"{name} {count}/{len(records)} ({count / total:.0%})" for name, count in filled.items())
        return f"Coverage: {summary} - precision: " + ", ".join(
            f"{precision} {count}" for precision, count in sorted(precisions.items())
        )

    def settings(self, user_config: Optional[UserStepConfig]) -> Settings:
        values = user_config or {}
        return Settings(
            fuzzy_threshold=float(values.get("FUZZY_THRESHOLD") or 90),
            fuzzy_margin=float(values.get("FUZZY_MARGIN") or 4),
            geocode_concurrency=int(values.get("GEOCODE_CONCURRENCY") or 8),
            photon_url=matching.clean(values.get("PHOTON_URL")),
        )

    def user_config(self) -> List[StepUserConfig]:
        return [
            StepUserConfig(
                "FUZZY_THRESHOLD",
                LocalisationString("Fuzzy threshold", "Fuzzy-Schwellenwert"),
                LocalisationString(
                    "Minimum similarity (0-100) before a registry name counts as the same organisation.",
                    "Minimale Ähnlichkeit (0-100), ab der ein Registername als dieselbe Organisation gilt.",
                ),
                StepUserConfig.StepUserConfigType.INTEGER,
                90,
                required=False,
            ),
            StepUserConfig(
                "FUZZY_MARGIN",
                LocalisationString("Fuzzy margin", "Fuzzy-Abstand"),
                LocalisationString(
                    "How far the best candidate must beat the second one; below it the field stays empty.",
                    "Wie weit der beste Kandidat den zweiten schlagen muss; darunter bleibt das Feld leer.",
                ),
                StepUserConfig.StepUserConfigType.INTEGER,
                4,
                required=False,
            ),
            StepUserConfig(
                "GEOCODE_CONCURRENCY",
                LocalisationString("Geocoding concurrency", "Parallele Geokodierungen"),
                LocalisationString(
                    "Parallel requests against the Photon server.",
                    "Parallele Anfragen an den Photon-Server.",
                ),
                StepUserConfig.StepUserConfigType.INTEGER,
                8,
                required=False,
            ),
            StepUserConfig(
                "PHOTON_URL",
                LocalisationString("Photon URL", "Photon-URL"),
                LocalisationString(
                    "Overrides the PHOTON_URL environment variable.",
                    "Überschreibt die Umgebungsvariable PHOTON_URL.",
                ),
                StepUserConfig.StepUserConfigType.STRING,
                None,
                required=False,
            ),
        ]

    @staticmethod
    def name() -> str:
        return "organisation_enrich"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Enrich Organisation Data", "Organisationen mit Daten anreichern")

    @staticmethod
    def description() -> Optional[LocalisationStringType]:
        return LocalisationString(
            "Resolve every organisation against the ROR, GLEIF and BEV snapshots and the Photon "
            "geocoder to fill in ROR id, Firmenbuchnummer, postal address and coordinates.",
            "Jede Organisation gegen die ROR-, GLEIF- und BEV-Snapshots und den Photon-Geocoder "
            "auflösen, um ROR-ID, Firmenbuchnummer, Adresse und Koordinaten zu ergänzen.",
        )

    def dependencies(self) -> Union[List[str], None]:
        return [OrganisationNormalizeStep.name()]
