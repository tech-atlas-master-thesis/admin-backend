import json
import math
import re
import unicodedata
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Bump when the shape or the rules of a comparison change, so cached comparisons are recomputed.
COMPARISON_VERSION = 1

DEFAULT_SCORE_THRESHOLD = 5.0
BORDERLINE_SHARE = 0.2
GEO_MOVE_KM = 0.01
GEO_FLAG_KM = 5.0
MATCH_FIELDS = ("title", "keywords", "abstract")

IDENTIFIER_ATTRIBUTES = ("ror", "lei", "firmenbuchnummer")
ADDRESS_ATTRIBUTES = ("country_code", "state", "city", "postal_code", "street", "house_number")
ORGANISATION_ATTRIBUTES = (*IDENTIFIER_ATTRIBUTES, "geo", "precision", "website", "type", *ADDRESS_ATTRIBUTES)
STATUS_STAGES = ("ror", "gleif", "geo")
PRECISION_ORDER = ("rooftop", "street", "building", "city", "region", "country", "none")


@dataclass
class DatasetData:
    dataset: Dict[str, Any]
    pipeline: Optional[Dict[str, Any]]
    projects: List[Dict[str, Any]]
    organisations: List[Dict[str, Any]]
    technologies: List[Dict[str, Any]]
    fields: List[Dict[str, Any]]
    grants: List[Dict[str, Any]]
    programmes: List[Dict[str, Any]]


@dataclass
class Comparison:
    summary: Dict[str, Any]
    rows: Dict[str, List[Dict[str, Any]]] = field(default_factory=dict)


def name_key(name: Any) -> str:
    text = unicodedata.normalize("NFKD", str(name or "")).casefold()
    text = "".join(character for character in text if not unicodedata.combining(character))
    return " ".join(re.sub(r"[^0-9a-z]+", " ", text).split())


def _id(value: Any) -> Optional[str]:
    return None if value is None else str(value)


def _ids(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if item is not None]
    return [str(value)]


def _get(document: Optional[Dict[str, Any]], *path: str) -> Any:
    value: Any = document
    for key in path:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def _blank(value: Any) -> bool:
    return value is None or value == "" or (isinstance(value, float) and math.isnan(value))


def _haversine_km(a: List[float], b: List[float]) -> float:
    lon1, lat1, lon2, lat2 = map(math.radians, (a[0], a[1], b[0], b[1]))
    h = math.sin((lat2 - lat1) / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2
    return 2 * 6371.0088 * math.asin(math.sqrt(h))


def _jaccard(a: set, b: set) -> Optional[float]:
    union = a | b
    return round(len(a & b) / len(union), 4) if union else None


def _keyed(documents: Iterable[Dict[str, Any]], key_of) -> Dict[str, Dict[str, Any]]:
    keyed: Dict[str, Dict[str, Any]] = {}
    for document in documents:
        key = key_of(document)
        if not key:
            continue
        unique, suffix = key, 2
        while unique in keyed:
            unique, suffix = f"{key}#{suffix}", suffix + 1
        keyed[unique] = document
    return keyed


def _side(base: Any, candidate: Any) -> Dict[str, Any]:
    return {
        "base": base,
        "candidate": candidate,
        "delta": None if base is None or candidate is None else candidate - base,
    }


def _value_change(base: Any, candidate: Any) -> str:
    if _blank(base) and _blank(candidate):
        return "missing"
    if _blank(base):
        return "gained"
    if _blank(candidate):
        return "lost"
    return "same" if base == candidate else "changed"


# ---------------------------------------------------------------------------------------------- configuration


def _threshold(pipeline: Optional[Dict[str, Any]]) -> float:
    value = _get(pipeline, "userConfig", "projects_filter", "SCORE_THRESHOLD")
    try:
        return float(value) if value is not None else DEFAULT_SCORE_THRESHOLD
    except TypeError, ValueError:
        return DEFAULT_SCORE_THRESHOLD


def _flatten_config(user_config: Optional[Dict[str, Any]]) -> Dict[Tuple[str, str], Any]:
    flat: Dict[Tuple[str, str], Any] = {}
    for step, values in (user_config or {}).items():
        if isinstance(values, dict):
            for key, value in values.items():
                flat[(step, key)] = value
        else:
            flat[(step, "")] = values
    return flat


def _render(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, ensure_ascii=False, default=str)
    return str(value)


def compare_user_config(base: Optional[Dict[str, Any]], candidate: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    flat_base, flat_candidate = _flatten_config(_get(base, "userConfig")), _flatten_config(
        _get(candidate, "userConfig")
    )
    changes = []
    for step, key in sorted(flat_base.keys() | flat_candidate.keys()):
        old, new = flat_base.get((step, key)), flat_candidate.get((step, key))
        if old != new:
            changes.append({"step": step, "key": key, "base": _render(old), "candidate": _render(new)})
    return changes


def _field_shorts(fields: List[Dict[str, Any]]) -> Dict[str, str]:
    return {_id(item.get("_id")): item.get("short") or item.get("label") for item in fields}


def _technology_index(data: DatasetData) -> Dict[str, Dict[str, Any]]:
    field_short = _field_shorts(data.fields)
    index = {}
    for technology in data.technologies:
        short = technology.get("short") or technology.get("label")
        terms = technology.get("searchTerms") or {}
        index[short] = {
            "id": _id(technology.get("_id")),
            "short": short,
            "label": technology.get("label"),
            "field": field_short.get(_id(technology.get("field"))),
            "anyOf": sorted(terms.get("anyOf") or []),
            "excluded": sorted(terms.get("excluded") or []),
        }
    return index


def compare_technology_config(
    base: Dict[str, Dict[str, Any]], candidate: Dict[str, Dict[str, Any]]
) -> List[Dict[str, Any]]:
    changes = []
    for short in sorted(base.keys() | candidate.keys()):
        old, new = base.get(short), candidate.get(short)
        entry = {"short": short, "label": (new or old)["label"], "field": (new or old)["field"]}
        if old is None:
            changes.append({**entry, "change": "added", "anyOf": {"added": new["anyOf"], "removed": []}})
            continue
        if new is None:
            changes.append({**entry, "change": "removed", "anyOf": {"added": [], "removed": old["anyOf"]}})
            continue
        diff = {
            key: {"added": sorted(set(new[key]) - set(old[key])), "removed": sorted(set(old[key]) - set(new[key]))}
            for key in ("anyOf", "excluded")
        }
        relabelled = old["label"] != new["label"]
        moved = old["field"] != new["field"]
        if relabelled or moved or any(part["added"] or part["removed"] for part in diff.values()):
            changes.append(
                {
                    **entry,
                    "change": "changed",
                    **diff,
                    "labelBefore": old["label"] if relabelled else None,
                    "fieldBefore": old["field"] if moved else None,
                }
            )
    return changes


def _snapshots(organisations: List[Dict[str, Any]]) -> Dict[str, Any]:
    versions: Dict[str, Counter] = defaultdict(Counter)
    for organisation in organisations:
        for registry, snapshot in (_get(organisation, "_match", "snapshots") or {}).items():
            version = snapshot.get("version") if isinstance(snapshot, dict) else snapshot
            versions[registry][_render(version)] += 1
    return {registry: counts.most_common(1)[0][0] for registry, counts in versions.items()}


# ---------------------------------------------------------------------------------------------- projects / links


def _project_key(project: Dict[str, Any]) -> Optional[str]:
    external_id = project.get("externalId")
    if _blank(external_id):
        return None
    return f"{project.get('data_source') or '?'}:{external_id}"


def _term_counts(counts: Any) -> List[Dict[str, Any]]:
    # Terms become values, not keys: a term such as "e.g." or "C++" is no safe MongoDB field name.
    if not isinstance(counts, dict):
        return []
    return [{"term": term, "count": count} for term, count in sorted(counts.items())]


def _match_entry(entry: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not entry:
        return None
    return {
        "score": entry.get("score"),
        "passed": entry.get("passed"),
        "matches": {name: _term_counts((entry.get("matches") or {}).get(name)) for name in MATCH_FIELDS},
        "excluded": {name: _term_counts((entry.get("excluded") or {}).get(name)) for name in MATCH_FIELDS},
    }


def _terms(entry: Optional[Dict[str, Any]]) -> set:
    if not entry:
        return set()
    return {hit["term"] for name in MATCH_FIELDS for hit in entry["matches"].get(name, [])}


def _hit_fields(entry: Optional[Dict[str, Any]]) -> List[str]:
    if not entry:
        return []
    return [name for name in MATCH_FIELDS if entry["matches"].get(name)]


def _hit_profile(entry: Optional[Dict[str, Any]]) -> str:
    fields = _hit_fields(entry)
    if not fields:
        return "unknown"
    return f"{fields[0]} only" if len(fields) == 1 else "+".join(fields)


@dataclass
class _ProjectView:
    key: str
    document: Dict[str, Any]
    technologies: set
    organisations: set
    matches: Dict[str, Dict[str, Any]]


def _project_views(
    data: DatasetData, technology_short_by_id: Dict[str, str], organisation_key_by_id: Dict[str, str]
) -> Dict[str, _ProjectView]:
    views = {}
    for key, project in _keyed(data.projects, _project_key).items():
        views[key] = _ProjectView(
            key=key,
            document=project,
            technologies={
                technology_short_by_id[tid]
                for tid in _ids(project.get("keyTechnologies"))
                if tid in technology_short_by_id
            },
            organisations={
                organisation_key_by_id[oid]
                for oid in _ids(project.get("organisations"))
                if oid in organisation_key_by_id
            },
            matches={short: _match_entry(entry) for short, entry in (project.get("_match") or {}).items()},
        )
    return views


def _link_reason(
    short: str,
    added: bool,
    base: Optional[_ProjectView],
    candidate: Optional[_ProjectView],
    technologies: Tuple[Dict[str, Any], Dict[str, Any]],
    thresholds: Tuple[float, float],
) -> str:
    if added and short not in technologies[0]:
        return "technology added"
    if not added and short not in technologies[1]:
        return "technology removed"
    if base is None:
        return "project entered"
    if candidate is None:
        return "project left"
    old, new = base.matches.get(short), candidate.matches.get(short)
    old_score = old["score"] if old else 0
    new_score = new["score"] if new else 0
    if old_score != new_score:
        return "score changed"
    if thresholds[0] != thresholds[1]:
        return "threshold changed"
    return "unknown"


def compare_projects(
    base: Dict[str, _ProjectView],
    candidate: Dict[str, _ProjectView],
    technologies: Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]],
    thresholds: Tuple[float, float],
    organisation_names: Dict[str, str],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    rows = []
    link_reasons: Dict[str, Counter] = {"added": Counter(), "removed": Counter()}
    link_profiles: Dict[str, Counter] = {"added": Counter(), "removed": Counter()}
    replacements: Counter = Counter()
    per_technology: Dict[str, Dict[str, set]] = defaultdict(lambda: {"base": set(), "candidate": set()})
    counts = Counter()

    for key in sorted(base.keys() | candidate.keys()):
        old, new = base.get(key), candidate.get(key)
        old_techs = old.technologies if old else set()
        new_techs = new.technologies if new else set()
        for short in old_techs:
            per_technology[short]["base"].add(key)
        for short in new_techs:
            per_technology[short]["candidate"].add(key)
        added, removed = new_techs - old_techs, old_techs - new_techs
        old_orgs = old.organisations if old else set()
        new_orgs = new.organisations if new else set()
        orgs_changed = bool(old and new and old_orgs != new_orgs)

        if old is None:
            change = "entered"
        elif new is None:
            change = "left"
        elif added or removed:
            change = "changed"
        elif orgs_changed:
            change = "organisations"
        else:
            continue
        counts[change] += 1

        technologies_detail = []
        for short in sorted(old_techs | new_techs | set(old.matches if old else {}) | set(new.matches if new else {})):
            old_match = old.matches.get(short) if old else None
            new_match = new.matches.get(short) if new else None
            if short in added:
                link = "added"
            elif short in removed:
                link = "removed"
            elif short in old_techs:
                link = "unchanged"
            else:
                link = "none"
            old_terms, new_terms = _terms(old_match), _terms(new_match)
            score_changed = (old_match or {}).get("score") != (new_match or {}).get("score")
            if link in ("unchanged", "none") and not score_changed:
                continue
            reason = None
            if link in ("added", "removed"):
                reason = _link_reason(short, link == "added", old, new, technologies, thresholds)
                link_reasons[link][reason] += 1
                link_profiles[link][_hit_profile(new_match if link == "added" else old_match)] += 1
            technologies_detail.append(
                {
                    "short": short,
                    "label": (technologies[1].get(short) or technologies[0].get(short) or {}).get("label", short),
                    "link": link,
                    "reason": reason,
                    "base": old_match,
                    "candidate": new_match,
                    "newTerms": sorted(new_terms - old_terms),
                    "lostTerms": sorted(old_terms - new_terms),
                }
            )
        if old and new:
            for lost in removed:
                for gained in added:
                    replacements[(lost, gained)] += 1

        document = (new or old).document
        score_deltas = [
            abs(((t["candidate"] or {}).get("score") or 0) - ((t["base"] or {}).get("score") or 0))
            for t in technologies_detail
        ]
        title = document.get("title") or document.get("short") or key
        rows.append(
            {
                "key": key,
                "dataSource": document.get("data_source"),
                "externalId": _render(document.get("externalId")),
                "title": title,
                "change": change,
                "added": sorted(added),
                "removed": sorted(removed),
                "touched": sorted(added | removed),
                "technologiesBase": sorted(old_techs),
                "technologiesCandidate": sorted(new_techs),
                "organisationsAdded": sorted(organisation_names.get(o, o) for o in new_orgs - old_orgs) if old else [],
                "organisationsRemoved": (
                    sorted(organisation_names.get(o, o) for o in old_orgs - new_orgs) if new else []
                ),
                "technologies": technologies_detail,
                "maxScoreDelta": max(score_deltas, default=0),
                "search": name_key(f"{key} {title}"),
            }
        )

    borderline = {}
    for side, views, threshold in (("base", base, thresholds[0]), ("candidate", candidate, thresholds[1])):
        margin = abs(threshold) * BORDERLINE_SHARE
        below = above = 0
        for view in views.values():
            for entry in view.matches.values():
                if entry is None or entry["score"] is None:
                    continue
                if threshold - margin <= entry["score"] < threshold:
                    below += 1
                elif threshold <= entry["score"] <= threshold + margin:
                    above += 1
        borderline[side] = {"threshold": threshold, "margin": margin, "below": below, "above": above}

    aggregates = {
        "counts": dict(counts),
        "linkReasons": {side: dict(counter) for side, counter in link_reasons.items()},
        "linkProfiles": {side: dict(counter) for side, counter in link_profiles.items()},
        "replacements": [
            {"from": lost, "to": gained, "count": count} for (lost, gained), count in replacements.most_common()
        ],
        "borderline": borderline,
        "perTechnology": per_technology,
        "projects": _side(len(base), len(candidate)),
        "dataSources": {
            side: dict(Counter(view.document.get("data_source") or "?" for view in views.values()))
            for side, views in (("base", base), ("candidate", candidate))
        },
    }
    return rows, aggregates


def _technology_tables(
    per_technology: Dict[str, Dict[str, set]],
    technologies: Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]],
    base_views: Dict[str, _ProjectView],
    candidate_views: Dict[str, _ProjectView],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    shorts = sorted(set(per_technology) | technologies[0].keys() | technologies[1].keys())
    tech_rows, network_rows = [], []
    per_field: Dict[str, Dict[str, set]] = defaultdict(lambda: {"base": set(), "candidate": set()})
    for short in shorts:
        info = technologies[1].get(short) or technologies[0].get(short) or {"label": short, "field": None}
        base_projects = per_technology.get(short, {}).get("base", set())
        candidate_projects = per_technology.get(short, {}).get("candidate", set())
        field_name = info.get("field") or "—"
        per_field[field_name]["base"] |= {(short, key) for key in base_projects}
        per_field[field_name]["candidate"] |= {(short, key) for key in candidate_projects}
        tech_rows.append(
            {
                "short": short,
                "label": info.get("label"),
                "field": field_name,
                "inBase": short in technologies[0],
                "inCandidate": short in technologies[1],
                "base": len(base_projects),
                "candidate": len(candidate_projects),
                "added": len(candidate_projects - base_projects),
                "removed": len(base_projects - candidate_projects),
                "delta": len(candidate_projects) - len(base_projects),
                "jaccard": _jaccard(base_projects, candidate_projects),
            }
        )
        base_orgs = {org for key in base_projects for org in base_views[key].organisations}
        candidate_orgs = {org for key in candidate_projects for org in candidate_views[key].organisations}
        network_rows.append(
            {
                "short": short,
                "label": info.get("label"),
                "field": field_name,
                "organisations": _side(len(base_orgs), len(candidate_orgs)),
                "organisationsAdded": len(candidate_orgs - base_orgs),
                "organisationsRemoved": len(base_orgs - candidate_orgs),
                "jaccard": _jaccard(base_orgs, candidate_orgs),
            }
        )
    field_rows = [
        {
            "field": name,
            "base": len(sides["base"]),
            "candidate": len(sides["candidate"]),
            "added": len(sides["candidate"] - sides["base"]),
            "removed": len(sides["base"] - sides["candidate"]),
            "delta": len(sides["candidate"]) - len(sides["base"]),
            "jaccard": _jaccard(sides["base"], sides["candidate"]),
        }
        for name, sides in sorted(per_field.items())
    ]
    return tech_rows, field_rows, network_rows


# ---------------------------------------------------------------------------------------------- organisations


def organisation_attributes(organisation: Dict[str, Any]) -> Dict[str, Any]:
    address = organisation.get("address") or {}
    coordinates = _get(organisation, "geo", "coordinates")
    return {
        "ror": organisation.get("ror"),
        "lei": _get(organisation, "_match", "gleif", "lei"),
        "firmenbuchnummer": organisation.get("firmenbuchnummer"),
        "geo": list(coordinates) if isinstance(coordinates, (list, tuple)) and len(coordinates) == 2 else None,
        "precision": _get(organisation, "_match", "geo", "precision") if coordinates else None,
        "website": organisation.get("website"),
        "type": organisation.get("type"),
        **{name: address.get(name) for name in ADDRESS_ATTRIBUTES},
    }


def _statuses(organisation: Optional[Dict[str, Any]]) -> Dict[str, str]:
    if organisation is None:
        return {stage: "absent" for stage in (*STATUS_STAGES, "precision")}
    statuses = {stage: _get(organisation, "_match", stage, "status") or "unknown" for stage in STATUS_STAGES}
    statuses["precision"] = organisation_attributes(organisation)["precision"] or "none"
    return statuses


def _match_summary(organisation: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if organisation is None:
        return None
    match = organisation.get("_match") or {}
    return {
        "ror": match.get("ror"),
        "gleif": match.get("gleif"),
        "geo": match.get("geo"),
        "address": match.get("address"),
        "warnings": match.get("warnings") or [],
    }


def pair_organisations(
    base: List[Dict[str, Any]], candidate: List[Dict[str, Any]]
) -> Tuple[List[Tuple[Optional[str], Optional[str], str]], Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    base_keyed = _keyed(base, lambda o: name_key(o.get("name")))
    candidate_keyed = _keyed(candidate, lambda o: name_key(o.get("name")))
    pairs = [(key, key, "name") for key in sorted(base_keyed.keys() & candidate_keyed.keys())]
    base_left = set(base_keyed) - set(candidate_keyed)
    candidate_left = set(candidate_keyed) - set(base_keyed)

    # Names merged into another organisation (institutes into their university) survive as aliases, so an
    # organisation renamed or merged between the runs is found through them before it counts as added/removed.
    def alias_index(keys, keyed):
        index = {}
        for key in keys:
            for alias in keyed[key].get("aliases") or []:
                index.setdefault(name_key(alias), key)
        return index

    candidate_aliases = alias_index(candidate_left, candidate_keyed)
    base_aliases = alias_index(base_left, base_keyed)
    for key in sorted(base_left):
        target = candidate_aliases.get(key)
        if target is None:
            target = next(
                (
                    candidate_aliases[name_key(alias)]
                    for alias in base_keyed[key].get("aliases") or []
                    if name_key(alias) in candidate_aliases
                ),
                None,
            )
        if target is not None and target in candidate_left:
            pairs.append((key, target, "alias"))
            base_left.discard(key)
            candidate_left.discard(target)
    for key in sorted(candidate_left):
        source = base_aliases.get(key)
        if source is not None and source in base_left:
            pairs.append((source, key, "alias"))
            base_left.discard(source)
            candidate_left.discard(key)
    pairs += [(key, None, "removed") for key in sorted(base_left)]
    pairs += [(None, key, "added") for key in sorted(candidate_left)]
    return pairs, base_keyed, candidate_keyed


def compare_organisations(
    pairs: List[Tuple[Optional[str], Optional[str], str]],
    base_keyed: Dict[str, Dict[str, Any]],
    candidate_keyed: Dict[str, Dict[str, Any]],
    project_counts: Tuple[Counter, Counter],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    rows = []
    coverage = {name: Counter() for name in ORGANISATION_ATTRIBUTES}
    transitions = {stage: Counter() for stage in (*STATUS_STAGES, "precision")}
    counts = Counter()
    geo_moves = []

    for base_key, candidate_key, identity in pairs:
        old = base_keyed.get(base_key) if base_key else None
        new = candidate_keyed.get(candidate_key) if candidate_key else None
        old_attributes = organisation_attributes(old) if old else {}
        new_attributes = organisation_attributes(new) if new else {}
        old_status, new_status = _statuses(old), _statuses(new)
        for stage in transitions:
            transitions[stage][(old_status[stage], new_status[stage])] += 1

        attributes, changed, conflicts = {}, [], []
        distance = None
        for name in ORGANISATION_ATTRIBUTES:
            old_value, new_value = old_attributes.get(name), new_attributes.get(name)
            if old is not None and not _blank(old_value):
                coverage[name]["base"] += 1
            if new is not None and not _blank(new_value):
                coverage[name]["candidate"] += 1
            if old is None or new is None:
                continue
            change = _value_change(old_value, new_value)
            if name == "geo" and change == "changed":
                distance = round(_haversine_km(old_value, new_value), 3)
                if distance < GEO_MOVE_KM:
                    change = "same"
            coverage[name][change] += 1
            if change in ("gained", "lost", "changed"):
                attributes[name] = {"change": change, "base": old_value, "candidate": new_value}
                changed.append(name)
                if name in IDENTIFIER_ATTRIBUTES and change == "changed":
                    conflicts.append(name)
        if distance is not None and "geo" in changed:
            geo_moves.append(distance)

        if old is None:
            change = "added"
        elif new is None:
            change = "removed"
        elif changed:
            change = "conflict" if conflicts else "changed"
        elif identity == "alias":
            change = "renamed"
        else:
            continue
        counts[change] += 1
        name = (new or old).get("name")
        rows.append(
            {
                "key": candidate_key or base_key,
                "name": name,
                "nameBase": old.get("name") if old else None,
                "nameCandidate": new.get("name") if new else None,
                "identity": identity,
                "change": change,
                "changedAttributes": changed,
                "conflicts": conflicts,
                "attributes": attributes,
                "geoDistanceKm": distance,
                "geoFlag": distance is not None and distance >= GEO_FLAG_KM,
                "statusBase": old_status,
                "statusCandidate": new_status,
                "projects": _side(
                    project_counts[0].get(base_key, 0) if base_key else None,
                    project_counts[1].get(candidate_key, 0) if candidate_key else None,
                ),
                "type": (new or old).get("type"),
                "matchBase": _match_summary(old),
                "matchCandidate": _match_summary(new),
                "search": name_key(f"{name} {old.get('name') if old else ''} {(new or old).get('ror') or ''}"),
            }
        )

    coverage_rows = [
        {
            "attribute": name,
            "base": counter["base"],
            "candidate": counter["candidate"],
            "delta": counter["candidate"] - counter["base"],
            "gained": counter["gained"],
            "lost": counter["lost"],
            "changed": counter["changed"],
        }
        for name, counter in coverage.items()
    ]
    transition_tables = {}
    for stage, counter in transitions.items():
        states = sorted({state for pair in counter for state in pair}, key=_state_order)
        transition_tables[stage] = {
            "states": states,
            "cells": [{"base": old, "candidate": new, "count": count} for (old, new), count in sorted(counter.items())],
        }
    aggregates = {
        "counts": dict(counts),
        "coverage": coverage_rows,
        "transitions": transition_tables,
        "organisations": _side(len(base_keyed), len(candidate_keyed)),
        "geoMoves": {
            "moved": len(geo_moves),
            "flagged": sum(1 for distance in geo_moves if distance >= GEO_FLAG_KM),
            "medianKm": round(sorted(geo_moves)[len(geo_moves) // 2], 1) if geo_moves else None,
        },
    }
    return rows, aggregates


def _state_order(state: str) -> Tuple[int, str]:
    order = ("matched", "source_only", "ambiguous", "unmatched", "skipped", *PRECISION_ORDER, "unknown", "absent")
    return (order.index(state), state) if state in order else (len(order), state)


# ---------------------------------------------------------------------------------------------- grants / programmes


def _compare_named(
    kind: str,
    base: Dict[str, Dict[str, Any]],
    candidate: Dict[str, Dict[str, Any]],
    base_attributes,
    candidate_attributes,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    rows, counts, totals = [], Counter(), Counter()
    for key in sorted(base.keys() | candidate.keys()):
        old, new = base.get(key), candidate.get(key)
        old_values = base_attributes(old) if old else {}
        new_values = candidate_attributes(new) if new else {}
        totals["base"] += old_values.get("projects") or 0
        totals["candidate"] += new_values.get("projects") or 0
        diff = {
            name: {"base": old_values.get(name), "candidate": new_values.get(name)}
            for name in sorted(old_values.keys() | new_values.keys())
            if old_values.get(name) != new_values.get(name)
        }
        if old is None:
            change = "added"
        elif new is None:
            change = "removed"
        elif diff:
            change = "changed"
        else:
            continue
        counts[change] += 1
        name = (new or old).get("name") or key
        rows.append(
            {
                "key": key,
                "name": name,
                "change": change,
                "changedAttributes": sorted(diff),
                "attributes": diff,
                "base": old_values or None,
                "candidate": new_values or None,
                "projectsDelta": (new_values.get("projects") or 0) - (old_values.get("projects") or 0),
                "search": name_key(name),
            }
        )
    aggregates = {
        kind: _side(len(base), len(candidate)),
        "counts": dict(counts),
        "projects": _side(totals["base"], totals["candidate"]),
    }
    return rows, aggregates


def _grant_attributes(data: DatasetData):
    names = {_id(programme.get("_id")): programme.get("name") for programme in data.programmes}
    return lambda grant: {"programme": names.get(_id(grant.get("programme"))), "projects": grant.get("projects")}


def _programme_attributes(data: DatasetData):
    per_programme = Counter(_id(grant.get("programme")) for grant in data.grants)
    return lambda programme: {
        "projects": programme.get("projects"),
        "grants": per_programme.get(_id(programme.get("_id")), 0),
    }


def compare_grants_and_programmes(base: DatasetData, candidate: DatasetData):
    grant_rows, grant_aggregates = _compare_named(
        "grants",
        _keyed(base.grants, lambda grant: grant.get("name")),
        _keyed(candidate.grants, lambda grant: grant.get("name")),
        _grant_attributes(base),
        _grant_attributes(candidate),
    )
    grant_aggregates["reassigned"] = sum(
        1 for row in grant_rows if row["change"] == "changed" and "programme" in row["changedAttributes"]
    )
    programme_rows, programme_aggregates = _compare_named(
        "programmes",
        _keyed(base.programmes, lambda programme: programme.get("name")),
        _keyed(candidate.programmes, lambda programme: programme.get("name")),
        _programme_attributes(base),
        _programme_attributes(candidate),
    )
    return grant_rows, grant_aggregates, programme_rows, programme_aggregates


# ---------------------------------------------------------------------------------------------- findings


def _findings(summary: Dict[str, Any]) -> List[str]:
    findings = []
    thresholds = summary["config"]["thresholds"]
    links = summary["links"]
    if thresholds["base"] != thresholds["candidate"]:
        findings.append(f"Score threshold changed from {thresholds['base']:g} to {thresholds['candidate']:g}.")
    for side, verb in (("removed", "removed"), ("added", "added")):
        total = links[side]
        if not total:
            continue
        reasons = summary["technologyLinks"]["linkReasons"].get(side, {})
        profiles = summary["technologyLinks"]["linkProfiles"].get(side, {})
        reason, reason_count = max(reasons.items(), key=lambda item: item[1], default=("unknown", 0))
        sentence = f"{total} links {verb}; {round(100 * reason_count / total)} % because of '{reason}'"
        profile, profile_count = max(profiles.items(), key=lambda item: item[1], default=(None, 0))
        if profile and profile_count:
            sentence += f", {round(100 * profile_count / total)} % with hits in {profile}"
        findings.append(sentence + ".")
    movers = sorted(summary["technologyLinks"]["perTechnology"], key=lambda row: abs(row["delta"]), reverse=True)
    movers = [row for row in movers if row["delta"]][:3]
    if movers:
        findings.append(
            "Largest changes per technology: "
            + ", ".join(f"{row['label']} ({row['delta']:+d})" for row in movers)
            + "."
        )
    for row in summary["organisations"]["coverage"]:
        if row["attribute"] in ("ror", "lei", "geo") and row["delta"]:
            findings.append(
                f"{row['attribute'].upper()} coverage {row['base']} → {row['candidate']} ({row['delta']:+d}); "
                f"{row['gained']} gained, {row['lost']} lost, {row['changed']} changed."
            )
    conflicts = summary["organisations"]["counts"].get("conflict", 0)
    if conflicts:
        findings.append(f"{conflicts} organisations got a different identifier (ROR/LEI/Firmenbuchnummer).")
    if summary["snapshots"]["differs"]:
        findings.append(
            "The datasets used different registry snapshots, so part of the enrichment changes may come from the "
            "registry data rather than from the configuration."
        )
    grants = summary["grants"]
    if grants["counts"]:
        findings.append(
            f"Grants: {grants['counts'].get('added', 0)} added, {grants['counts'].get('removed', 0)} removed, "
            f"{grants['counts'].get('changed', 0)} changed ({grants['reassigned']} moved to another programme)."
        )
    return findings


# ---------------------------------------------------------------------------------------------- entry point


def _dataset_meta(data: DatasetData) -> Dict[str, Any]:
    dataset, pipeline = data.dataset, data.pipeline or {}
    return {
        "id": _id(dataset.get("_id")),
        "pipeline": _id(dataset.get("pipeline")),
        "pipelineType": dataset.get("pipelineType"),
        "pipelineName": dataset.get("pipelineName"),
        "pipelineDescription": pipeline.get("description"),
        "created": dataset.get("created"),
        "projects": len(data.projects),
        "organisations": len(data.organisations),
        "technologies": len(data.technologies),
        "grants": len(data.grants),
        "programmes": len(data.programmes),
    }


def compare_datasets(base: DatasetData, candidate: DatasetData) -> Comparison:
    technologies = (_technology_index(base), _technology_index(candidate))
    thresholds = (_threshold(base.pipeline), _threshold(candidate.pipeline))

    pairs, base_orgs, candidate_orgs = pair_organisations(base.organisations, candidate.organisations)
    # Organisations are compared by their pairing key, so a project link survives a rename between the runs.
    base_org_key = {_id(doc.get("_id")): key for key, doc in base_orgs.items()}
    candidate_org_key = {_id(doc.get("_id")): key for key, doc in candidate_orgs.items()}
    for base_key, candidate_key, _ in pairs:
        if base_key and candidate_key and base_key != candidate_key:
            base_org_key[_id(base_orgs[base_key].get("_id"))] = candidate_key
    organisation_names = {
        **{key: doc.get("name") for key, doc in base_orgs.items()},
        **{key: doc.get("name") for key, doc in candidate_orgs.items()},
    }

    base_views = _project_views(base, {v["id"]: k for k, v in technologies[0].items()}, base_org_key)
    candidate_views = _project_views(candidate, {v["id"]: k for k, v in technologies[1].items()}, candidate_org_key)
    project_rows, project_aggregates = compare_projects(
        base_views, candidate_views, technologies, thresholds, organisation_names
    )
    tech_rows, field_rows, network_rows = _technology_tables(
        project_aggregates.pop("perTechnology"), technologies, base_views, candidate_views
    )

    # Project counts per organisation, keyed by each side's own key so the rows show both sides.
    base_counts, candidate_counts = Counter(), Counter()
    reverse_base = {_id(doc.get("_id")): key for key, doc in base_orgs.items()}
    for project in base.projects:
        for oid in set(_ids(project.get("organisations"))):
            if oid in reverse_base:
                base_counts[reverse_base[oid]] += 1
    for project in candidate.projects:
        for oid in set(_ids(project.get("organisations"))):
            if oid in candidate_org_key:
                candidate_counts[candidate_org_key[oid]] += 1
    organisation_rows, organisation_aggregates = compare_organisations(
        pairs, base_orgs, candidate_orgs, (base_counts, candidate_counts)
    )
    grant_rows, grant_aggregates, programme_rows, programme_aggregates = compare_grants_and_programmes(base, candidate)

    base_links = sum(len(view.technologies) for view in base_views.values())
    candidate_links = sum(len(view.technologies) for view in candidate_views.values())
    snapshots = {"base": _snapshots(base.organisations), "candidate": _snapshots(candidate.organisations)}
    snapshots["differs"] = bool(
        snapshots["base"] and snapshots["candidate"] and snapshots["base"] != snapshots["candidate"]
    )
    edges = {
        side: len(
            {(short, org) for view in views.values() for short in view.technologies for org in view.organisations}
        )
        for side, views in (("base", base_views), ("candidate", candidate_views))
    }

    summary = {
        "version": COMPARISON_VERSION,
        "base": _dataset_meta(base),
        "candidate": _dataset_meta(candidate),
        "config": {
            "thresholds": {"base": thresholds[0], "candidate": thresholds[1]},
            "userConfig": compare_user_config(base.pipeline, candidate.pipeline),
            "technologies": compare_technology_config(*technologies),
        },
        "snapshots": snapshots,
        "projects": {**project_aggregates.pop("projects"), "counts": project_aggregates.pop("counts")},
        "links": {
            **_side(base_links, candidate_links),
            "added": sum(row["added"] for row in tech_rows),
            "removed": sum(row["removed"] for row in tech_rows),
        },
        "technologyLinks": {**project_aggregates, "perTechnology": tech_rows, "perField": field_rows},
        "organisations": organisation_aggregates,
        "grants": grant_aggregates,
        "programmes": programme_aggregates,
        "network": {"perTechnology": network_rows, "edges": _side(edges["base"], edges["candidate"])},
    }
    summary["findings"] = _findings(summary)
    return Comparison(
        summary,
        {
            "projects": project_rows,
            "organisations": organisation_rows,
            "grants": grant_rows,
            "programmes": programme_rows,
        },
    )
