import re
from collections import defaultdict
from typing import Optional, Union, List, Dict, Any, Tuple

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
    EventType,
    Event,
)
from pipeline_configs.transform_steps.configs import GetOrganisationTypeMapperConfiguration
from pipeline_configs.transform_steps.organisations_extract import OrganisationExtractStep

SPECIAL_CONVERSION_NEEDED = "__SPECIAL_CONVERSION_NEEDED"

# A name containing one of these words names a university (in any of the languages found in the data).
UNIVERSITY_PATTERN = re.compile(
    r"universit|universidad|universidade|uniwersytet|univerzit|univerza|egyetem",
    re.IGNORECASE,
)

# University-like names that are separate entities (hospitals, universities of applied sciences, ...).
NON_UNIVERSITY_PATTERN = re.compile(
    r"klinikum|hospital|spital|krankenhaus|medical cent|medisch centrum|applied sciences|fachhochschule"
    r"|hochschule|universitäts\w",
    re.IGNORECASE,
)

# A remainder mentioning a hospital is never merged into the university in front of it.
HOSPITAL_PATTERN = re.compile(r"hospital|klinikum|spital|krankenhaus|medical cent", re.IGNORECASE)

# Words that mark the remainder of a name as an organisational unit of the university.
# Matched as whole words and, for German compounds, as word endings ("Forschungsinstitut").
SUB_UNIT_PATTERN = re.compile(
    r"\b(?:"
    r"ins?titut[eo]?|insitute?|department|departement|departamento|departimento|dipartimento|dept"
    r"|faculty|facult[ée]|facultad|faculdade|fakultät|lehrstuhl|professur|chair|fachbereich|fachgebiet"
    r"|abteilung|sektion|section|division|zentrum|center|centre|centro|klinik|klinische|clinic"
    r"|school|college of|laborator(?:y|ies|io)|laboratoire|lab|labor|arbeitsgruppe|ag|forschungsgruppe"
    r"|research (?:group|unit|platform)|group|unit|rektorat|vizerektorat|büro|office|services?"
    r"|core facilit(?:y|ies)|organisationseinheit|facheinheit|koordinationsstelle|transferstelle"
    r"|plattform|platform|bibliothek|library|direktion|zentraler|versuchs\w*|forschungs\w*"
    r"|universitätsklinik|universitätszahnklinik|universitätsbibliothek|biobank|gleichstellung"
    r"|research network|netzwerk"
    r")\b"
    r"|\bdep\."
    r"|\w(?:institut|zentrum|fakultät|klinik|abteilung|labor|gruppe)\b",
    re.IGNORECASE,
)

# Unit words that are clear enough to cut a university name in front of them when the university itself
# has no entry of its own.
STRONG_SUB_UNIT_PATTERN = re.compile(
    r"\b(?:ins?titut[eo]?|insitute?|department|departement|departamento|departimento|dipartimento"
    r"|faculty|facult[ée]|facultad|faculdade|fakultät|lehrstuhl|professur|chair (?:of|for)|fachbereich"
    r"|fachgebiet|abteilung|school of|college of)\b",
    re.IGNORECASE,
)

# A remainder starting like this names a campus or another university, e.g. "University of Texas at Dallas",
# "Warsaw University of Technology", "Université de Paris 11", "University of Wisconsin-Madison".
CAMPUS_PATTERN = re.compile(
    r"^(?:(?:at|in|im|zu|upon|of|de|di|del|du|des|der|la|system)\b|\d|(?:[IVX]+)\b)",
    re.IGNORECASE,
)

# A remainder that only carries noise or an acronym of the university, e.g. "(ELTE)", "(nan)".
NOISE_PATTERN = re.compile(r"^(?:\(nan\)|\(?[A-Z][A-Z0-9&\-]{1,9}\)?)$")

SEPARATORS = " ,-–/:"

DEFAULT_UNIVERSITY_ALIASES = {
    "Johannes Kepler Universität Linz": "Universität Linz",
    "Johannes Keppler Universität Linz": "Universität Linz",
    "Karl-Franzens-Universität Graz": "Universität Graz",
    "Alpen-Adria-Universität Klagenfurt": "Universität Klagenfurt",
    "Universität für Bodenkultur": "Universität für Bodenkultur Wien",
    "TU München": "Technische Universität München",
    "Technical University of Munich": "Technische Universität München",
    "Friedrich-Alexander-Universität Erlangen": "Friedrich-Alexander-Universität Erlangen-Nürnberg",
    "Friedrich-Alexander-Universität Erlangen Nürnberg": "Friedrich-Alexander-Universität Erlangen-Nürnberg",
    "Friedrich Alexander Universität Erlangen-Nürnberg": "Friedrich-Alexander-Universität Erlangen-Nürnberg",
    "Friedrich-Alexander-University Erlangen-Nuremberg": "Friedrich-Alexander-Universität Erlangen-Nürnberg",
    "Universität Erlangen-Nürnberg": "Friedrich-Alexander-Universität Erlangen-Nürnberg",
    "Johannes Gutenberg Universität Mainz": "Johannes Gutenberg-Universität Mainz",
    "Universität Göttingen": "Georg-August-Universität Göttingen",
    "University of Göttingen": "Georg-August-Universität Göttingen",
    "Universität Hannover": "Leibniz Universität Hannover",
}


def clean_name(name: Optional[str]) -> str:
    """Collapses whitespace and removes scraper noise such as a trailing "(nan)"."""
    if not name:
        return ""
    name = re.sub(r"\s+", " ", name).strip()
    name = re.sub(r"(?:\s*\(nan\))+$", "", name, flags=re.IGNORECASE)
    return name.strip(SEPARATORS + " ")


def is_university_name(name: str) -> bool:
    return bool(UNIVERSITY_PATTERN.search(name)) and not NON_UNIVERSITY_PATTERN.search(name)


def sub_unit_start(name: str) -> Optional[int]:
    """Position of the first sub-unit word that comes after the university word, if any."""
    university = UNIVERSITY_PATTERN.search(name)
    if not university:
        return None
    for match in SUB_UNIT_PATTERN.finditer(name, university.end()):
        return match.start()
    return None


class OrganisationNormalizeStep(StepConfig):
    async def run(
        self,
        user_config: Optional[UserStepConfig],
        results: Optional[Dict[str, Any]] = None,
        events: Optional[List[Event]] = None,
        **_,
    ):
        if results is None:
            results = {}
        events = events if events is not None else []
        ORGANISATIONS: Dict[str, Dict[str, Any]] = results.get("organisation_extract")
        ORGANISATION_TYPE_MAPPER = results.get(GetOrganisationTypeMapperConfiguration.name())
        if ORGANISATIONS is None:
            raise FileNotFoundError("No organisation data found")
        TYPE_MAPPING: Dict[str, str] = user_config.get("TYPE_MAPPING")
        UNIVERSITY_ALIASES: Dict[str, str] = user_config.get("UNIVERSITY_ALIASES") or DEFAULT_UNIVERSITY_ALIASES
        yield "Data found", EventType.INFO

        organisations = ORGANISATIONS.copy()

        self.map_type(organisations, TYPE_MAPPING)
        organisations = self.deduplicate_organisations(organisations, events)

        count_before = len(organisations)
        organisations = self.deduplicate_universities(organisations, UNIVERSITY_ALIASES, events)
        merged_universities = sum(1 for organisation in organisations.values() if organisation.get("aliases"))
        yield (
            f"Merged {count_before - len(organisations)} university sub-units and name variants into "
            f"{merged_universities} universities ({count_before} -> {len(organisations)} organisations)",
            EventType.INFO,
        )

        self.map_special_organisations(organisations, ORGANISATION_TYPE_MAPPER, events)

        yield organisations, EventType.RESULT

    def deduplicate_organisations(
        self, organisations: Dict[str, Dict[str, Any]], events: List[Event]
    ) -> Dict[str, Dict[str, Any]]:
        unique_organisations = {}
        for organisation in organisations.values():
            identifier = organisation["name"]
            if identifier not in unique_organisations:
                unique_organisations[identifier] = organisation
            else:
                unique_organisations[identifier] = self.merge_organisation(
                    unique_organisations[identifier], organisation
                )
        return unique_organisations

    def merge_organisation(self, org1: Dict[str, Any], org2: Dict[str, Any]) -> Dict[str, Any]:
        merged_org = org1.copy()
        for key, value in org2.items():
            if key not in merged_org or merged_org[key] is None:
                merged_org[key] = value
        if merged_org["type"] == SPECIAL_CONVERSION_NEEDED:
            merged_org["type"] = org2["type"]
        return merged_org

    # ------------------------------------------------------------------ universities

    def deduplicate_universities(
        self,
        organisations: Dict[str, Dict[str, Any]],
        aliases: Dict[str, str],
        events: List[Event],
    ) -> Dict[str, Dict[str, Any]]:
        """
        Merges institutes, departments and other sub-units of a university, as well as spelling variants,
        into one organisation named after the university. Every original key that was merged away is kept
        in the "aliases" list of the university, so later steps can still resolve it.
        """
        cleaned = {key: clean_name(organisation.get("name") or key) for key, organisation in organisations.items()}
        surfaces = self._university_surfaces(cleaned.values(), aliases)

        targets: Dict[str, str] = {}
        unresolved: List[str] = []
        for key, name in cleaned.items():
            target, reason = self._resolve_university(name, surfaces)
            if target is not None:
                targets[key] = target
            elif reason:
                unresolved.append(f"{name} (left after '{reason}')")
        # An entry named exactly like a university that others are merged into always joins that group.
        target_names = set(targets.values())
        for key, name in cleaned.items():
            if key not in targets and name in target_names:
                targets[key] = name

        groups: Dict[str, List[str]] = defaultdict(list)
        for key, target in targets.items():
            groups[target].append(key)

        result: Dict[str, Dict[str, Any]] = {}
        for key, organisation in organisations.items():
            target = targets.get(key)
            if target is None:
                result[key] = organisation
            elif target not in result:
                result[target] = self._merge_university_group(target, groups[target], organisations, cleaned)

        for target, keys in sorted(groups.items()):
            merged_away = [key for key in keys if key != target]
            if merged_away:
                events.append(
                    Event.now(
                        f"University '{target}': merged {len(merged_away)} entries ({'; '.join(sorted(merged_away))})",
                        EventType.INFO,
                    )
                )
            if all(cleaned[key] != target for key in keys):
                events.append(
                    Event.now(
                        f"University '{target}' had no entry of its own and was created from its sub-units",
                        EventType.INFO,
                    )
                )
        for message in sorted(unresolved):
            events.append(
                Event.now(f"University name with unrecognised suffix not merged: {message}", EventType.INFO)
            )
        return result

    def _university_surfaces(self, names, aliases: Dict[str, str]) -> Dict[str, str]:
        """Maps every known spelling of a university to its canonical name, longest spelling first."""
        names = [name for name in names if is_university_name(name)]
        surfaces: Dict[str, str] = {}
        sub_units: List[str] = []
        for name in names:
            if sub_unit_start(name) is not None:
                sub_units.append(name)
            elif len(name.split()) >= 2:
                surfaces[name] = name
        for alias, canonical in aliases.items():
            surfaces[clean_name(alias)] = clean_name(canonical)

        # Sub-units of a university that has no entry of its own: the university is the part of the name in
        # front of a clear unit word ("Washington University School of Medicine" -> "Washington University").
        known = sorted(surfaces, key=len, reverse=True)
        for name in sub_units:
            if any(name.startswith(surface) and not name[len(surface)].isalnum() for surface in known):
                continue
            prefix = self._university_in_front_of_unit(name)
            if prefix:
                surfaces.setdefault(prefix, prefix)

        for surface, canonical in list(surfaces.items()):
            surfaces[surface] = surfaces.get(canonical, canonical) if canonical != surface else canonical
        return dict(sorted(surfaces.items(), key=lambda item: len(item[0]), reverse=True))

    def _university_in_front_of_unit(self, name: str) -> Optional[str]:
        university = UNIVERSITY_PATTERN.search(name)
        if not university or HOSPITAL_PATTERN.search(name):
            return None
        first_unit = SUB_UNIT_PATTERN.search(name, university.end())
        strong_unit = STRONG_SUB_UNIT_PATTERN.search(name, university.end())
        # Only split at a clear unit word that is also the first unit word ("Public Service" is not a unit).
        if not first_unit or not strong_unit or first_unit.start() != strong_unit.start():
            return None
        prefix = name[: strong_unit.start()]
        dash = prefix.find(" - ", university.end())
        if dash != -1:
            prefix = prefix[:dash]
        prefix = prefix.strip(SEPARATORS)
        words_after_university = len(prefix[university.end():].split())
        if len(prefix.split()) < 2 or words_after_university > 3 or not is_university_name(prefix):
            return None
        return prefix

    def _resolve_university(self, name: str, surfaces: Dict[str, str]) -> Tuple[Optional[str], Optional[str]]:
        """
        Returns (canonical university, None) when the name belongs to a university,
        (None, matched prefix) when it starts with a university but the rest was not recognised,
        and (None, None) when it has nothing to do with a university.
        """
        if surfaces.get(name, name) != name:
            return surfaces[name], None  # alias of another university
        for surface, canonical in surfaces.items():
            if surface == name or not name.startswith(surface):
                continue
            rest = name[len(surface):]
            if rest[0].isalnum():
                continue  # "Universität Wien" must not match "Universität Wiener ..."
            if canonical == name:
                return name, None  # a shorter alias of this very university, e.g. "... Erlangen" -> "... Erlangen-Nürnberg"
            if rest[0] in "-–" or CAMPUS_PATTERN.match(rest.strip(SEPARATORS)):
                return surfaces.get(name), None  # another university or campus, e.g. "Koblenz-Landau", "at Dallas"
            rest = rest.strip(SEPARATORS)
            if not rest or NOISE_PATTERN.match(rest):
                return canonical, None
            if HOSPITAL_PATTERN.search(rest):
                return surfaces.get(name), None
            if SUB_UNIT_PATTERN.search(rest):
                return canonical, None
            return None, surface
        if name in surfaces:
            return surfaces[name], None
        return None, None

    def _merge_university_group(
        self,
        target: str,
        keys: List[str],
        organisations: Dict[str, Dict[str, Any]],
        cleaned: Dict[str, str],
    ) -> Dict[str, Any]:
        # The university's own entries come first so their values win over the ones of the sub-units.
        own = [key for key in keys if cleaned[key] == target]
        sub_units = [key for key in keys if cleaned[key] != target]

        if own:
            merged = organisations[own[0]].copy()
            merged["address"] = dict(merged.get("address") or {})
            rest = own[1:] + sub_units
        else:
            merged = organisations[sub_units[0]].copy()
            # ROR id and website of an institute do not describe the university.
            merged["ror"] = None
            merged["website"] = None
            merged["address"] = dict(merged.get("address") or {})
            rest = sub_units[1:]
        merged["name"] = target

        for key in rest:
            other = organisations[key]
            is_sub_unit = cleaned[key] != target
            for field, value in other.items():
                if field in ("name", "aliases", "address"):
                    continue
                if is_sub_unit and field in ("ror", "website"):
                    continue
                if merged.get(field) is None:
                    merged[field] = value
            for field, value in (other.get("address") or {}).items():
                if merged["address"].get(field) is None:
                    merged["address"][field] = value
            if merged.get("type") == SPECIAL_CONVERSION_NEEDED and other.get("type"):
                merged["type"] = other["type"]

        aliases = set()
        for key in keys:
            aliases.update(organisations[key].get("aliases") or [])
            if key != target:
                aliases.add(key)
        aliases.discard(target)
        merged["aliases"] = sorted(aliases)
        return merged

    # ------------------------------------------------------------------ types

    def map_type(self, organisations: Dict[str, Dict[str, Any]], mapping: Dict[str, str]) -> None:
        for organisation in organisations.values():
            new_type = mapping.get(organisation["type"], None) if "type" in organisation else None
            if not new_type:
                new_type = SPECIAL_CONVERSION_NEEDED
            organisation["type"] = new_type

    def map_special_organisations(
        self, organisations: Dict[str, Dict[str, Any]], organisation_mapper, events: List[Event]
    ) -> None:
        for organisation in organisations.values():
            current_org_type = organisation["type"]
            if current_org_type == SPECIAL_CONVERSION_NEEDED:
                for mapper in organisation_mapper:
                    if any(re.findall(regex, organisation["name"].lower()) for regex in mapper["keywords"]):
                        organisation["type"] = mapper["mapTo"]
                        break

                if organisation["type"] == SPECIAL_CONVERSION_NEEDED:
                    events.append(
                        Event.now(
                            f"No mapping found for organisation {organisation['name']}",
                            EventType.WARNING,
                        )
                    )
                    organisation["type"] = "OTHER"

    def user_config(self) -> List[StepUserConfig]:
        return [
            StepUserConfig(
                "TYPE_MAPPING",
                LocalisationString("Type Mapping", "Typ Zuordnung"),
                LocalisationString(
                    "Mapping of organisation type from dataSource to universal internal enum",
                    "Zuordnung von Organisationstyp aus Datenquelle zu universellem internen Enum",
                ),
                StepUserConfig.StepUserConfigType.MAPPING,
                {
                    "Außeruniversitäre Forschungseinrichtung": "RESEARCH_INSTITUTE",
                    "Bund, Länder, Gemeinden": "PUBLIC_INSTITUTION",
                    "Einzelforscher": "SINGLE_RESEARCHER",
                    "Fachhochschule": "FACHHOCHSCHULE",
                    "Gemeinnützige Organisation": "NON_PROFIT",
                    "Interessensvertretung": "LOBBY",
                    "Privatuniversität": "UNIVERSITY",
                    "Sonstige": "OTHER",
                    "Universität": "UNIVERSITY",
                    "unternehmerisch tätig": "COMPANY",
                    "Projektpartner:in": "OTHER",
                },
            ),
            StepUserConfig(
                "UNIVERSITY_ALIASES",
                LocalisationString("University Aliases", "Universitäts-Aliase"),
                LocalisationString(
                    "Alternative spellings of a university mapped to the name it should be merged into",
                    "Alternative Schreibweisen einer Universität und der Name, unter dem sie zusammengeführt wird",
                ),
                StepUserConfig.StepUserConfigType.MAPPING,
                DEFAULT_UNIVERSITY_ALIASES,
            ),
        ]

    @staticmethod
    def name() -> str:
        return "organisation_normalize"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Normalize Organisation Data", "Organisationen normalisieren")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return [OrganisationExtractStep.name(), GetOrganisationTypeMapperConfiguration.name()]
