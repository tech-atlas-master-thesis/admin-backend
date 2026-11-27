from bson import ObjectId

from datasets.compare import DatasetData, compare_datasets, name_key


def technology(short, label, field_id, any_of, excluded=()):
    return {
        "_id": ObjectId(),
        "short": short,
        "label": label,
        "field": field_id,
        "searchTerms": {"anyOf": list(any_of), "excluded": list(excluded)},
    }


def match(score, passed, title=None, abstract=None):
    return {
        "score": score,
        "passed": passed,
        "matches": {"title": title or {}, "keywords": {}, "abstract": abstract or {}},
        "excluded": {"title": {}, "keywords": {}, "abstract": {}},
    }


def organisation(name, ror=None, lei=None, geo=None, precision=None, aliases=None, city=None, snapshot="2026-10"):
    return {
        "_id": ObjectId(),
        "name": name,
        "type": "company",
        "ror": ror,
        "firmenbuchnummer": None,
        "website": None,
        "aliases": aliases or [],
        "address": {"city": city},
        "geo": {"type": "Point", "coordinates": geo} if geo else None,
        "_match": {
            "ror": {"status": "matched" if ror else "unmatched"},
            "gleif": {"status": "matched", "lei": lei} if lei else {"status": "unmatched"},
            "geo": {"status": "matched", "precision": precision} if geo else {"status": "unmatched"},
            "snapshots": {"ror": {"version": snapshot}},
        },
    }


def build(threshold, tech_terms, projects, organisations, grants, programmes):
    field_id = ObjectId()
    technologies = [technology(short, short.upper(), field_id, terms) for short, terms in tech_terms.items()]
    by_short = {tech["short"]: tech["_id"] for tech in technologies}
    orgs = {org["name"]: org for org in organisations}
    programme_docs = [{"_id": ObjectId(), "name": name, "projects": count} for name, count in programmes.items()]
    programme_ids = {doc["name"]: doc["_id"] for doc in programme_docs}
    grant_docs = [
        {"_id": ObjectId(), "name": name, "programme": programme_ids.get(programme), "projects": count}
        for name, (programme, count) in grants.items()
    ]
    project_docs = [
        {
            "_id": ObjectId(),
            "externalId": external_id,
            "data_source": "FFG",
            "title": f"Project {external_id}",
            "keyTechnologies": [by_short[short] for short in techs],
            "organisations": [orgs[name]["_id"] for name in org_names],
            "_match": matches,
        }
        for external_id, techs, org_names, matches in projects
    ]
    return DatasetData(
        dataset={"_id": ObjectId(), "pipeline": ObjectId(), "pipelineName": f"run {threshold}"},
        pipeline={"userConfig": {"projects_filter": {"SCORE_THRESHOLD": threshold}}},
        projects=project_docs,
        organisations=organisations,
        technologies=technologies,
        fields=[{"_id": field_id, "short": "ki", "label": "KI"}],
        grants=grant_docs,
        programmes=programme_docs,
    )


def scenario():
    base = build(
        5,
        {"ml": ["Machine Learning"], "h2": ["Wasserstoff"]},
        [
            ("1", ["ml"], ["TU Graz"], {"ml": match(5, True, title={"Machine Learning": 1})}),
            ("2", ["ml", "h2"], ["TU Graz", "AVL"], {"ml": match(6, True), "h2": match(10, True)}),
            ("3", ["h2"], ["AVL"], {"h2": match(5, True, abstract={"Wasserstoff": 5})}),
        ],
        [
            organisation("TU Graz", ror="00d7xrm67", geo=[15.45, 47.06], precision="city"),
            organisation("AVL", lei="LEI1", geo=[15.40, 47.05], precision="rooftop"),
            organisation("Institut für Physik", city="Wien"),
        ],
        {"Basisprogramm": ("General Programme", 2), "Bridge": ("Bridge", 1)},
        {"General Programme": 2, "Bridge": 1},
    )
    candidate = build(
        6,
        {"ml": ["Machine Learning", "Deep Learning"], "h2": ["Wasserstoff"], "saf": ["SAF"]},
        [
            (
                "2",
                ["ml", "saf"],
                ["TU Graz", "AVL"],
                {"ml": match(6, True), "h2": match(4, False), "saf": match(6, True)},
            ),
            ("3", ["h2"], ["AVL"], {"h2": match(7, True, abstract={"Wasserstoff": 7})}),
            ("4", ["ml"], ["Universität Wien"], {"ml": match(8, True, title={"Deep Learning": 1})}),
        ],
        [
            organisation("TU Graz", ror="02jfbm483", geo=[15.45, 47.06], precision="city"),
            organisation("AVL", geo=[16.37, 48.21], precision="street"),
            organisation("Universität Wien", aliases=["Institut für Physik"], ror="03prydq77"),
        ],
        {"Basisprogramm": ("Basis", 1), "Spinoff": ("Basis", 1)},
        {"Basis": 2},
    )
    return compare_datasets(base, candidate)


def test_name_key_folds_case_accents_and_punctuation():
    assert name_key("Technische Universität  Graz!") == name_key("technische universitat graz")


def test_project_link_changes_and_reasons():
    comparison = scenario()
    rows = {row["key"]: row for row in comparison.rows["projects"]}
    assert set(rows) == {"FFG:1", "FFG:2", "FFG:4"}
    assert rows["FFG:1"]["change"] == "left"
    assert rows["FFG:4"]["change"] == "entered"
    assert rows["FFG:2"]["added"] == ["saf"] and rows["FFG:2"]["removed"] == ["h2"]
    reasons = {t["short"]: t["reason"] for t in rows["FFG:2"]["technologies"] if t["reason"]}
    assert reasons == {"saf": "technology added", "h2": "score changed"}
    assert comparison.summary["technologyLinks"]["replacements"] == [{"from": "h2", "to": "saf", "count": 1}]
    assert comparison.summary["links"] == {"base": 4, "candidate": 4, "delta": 0, "added": 2, "removed": 2}


def test_unchanged_project_with_score_change_only_is_not_a_row():
    rows = {row["key"] for row in scenario().rows["projects"]}
    assert "FFG:3" not in rows


def test_technology_config_diff():
    changes = {change["short"]: change for change in scenario().summary["config"]["technologies"]}
    assert changes["saf"]["change"] == "added"
    assert changes["ml"]["anyOf"] == {"added": ["Deep Learning"], "removed": []}
    assert "h2" not in changes


def test_threshold_and_user_config():
    config = scenario().summary["config"]
    assert config["thresholds"] == {"base": 5.0, "candidate": 6.0}
    assert config["userConfig"] == [
        {"step": "projects_filter", "key": "SCORE_THRESHOLD", "base": "5", "candidate": "6"}
    ]


def test_organisation_changes():
    comparison = scenario()
    rows = {row["name"]: row for row in comparison.rows["organisations"]}
    assert rows["TU Graz"]["change"] == "conflict" and rows["TU Graz"]["conflicts"] == ["ror"]
    avl = rows["AVL"]
    assert avl["change"] == "changed"
    assert avl["attributes"]["lei"]["change"] == "lost"
    assert avl["geoFlag"] and avl["geoDistanceKm"] > 100
    vienna = rows["Universität Wien"]
    assert vienna["identity"] == "alias" and vienna["nameBase"] == "Institut für Physik"
    assert vienna["attributes"]["ror"]["change"] == "gained"
    coverage = {row["attribute"]: row for row in comparison.summary["organisations"]["coverage"]}
    assert coverage["ror"]["base"] == 1 and coverage["ror"]["candidate"] == 2
    precision = comparison.summary["organisations"]["transitions"]["precision"]["cells"]
    assert {"base": "rooftop", "candidate": "street", "count": 1} in precision


def test_project_organisation_links_survive_alias_pairing():
    rows = {row["key"]: row for row in scenario().rows["projects"]}
    assert rows["FFG:2"]["organisationsAdded"] == [] and rows["FFG:2"]["organisationsRemoved"] == []


def test_grants_and_programmes():
    comparison = scenario()
    grants = {row["name"]: row for row in comparison.rows["grants"]}
    assert grants["Basisprogramm"]["change"] == "changed"
    assert grants["Basisprogramm"]["attributes"]["programme"] == {"base": "General Programme", "candidate": "Basis"}
    assert grants["Bridge"]["change"] == "removed" and grants["Spinoff"]["change"] == "added"
    assert comparison.summary["grants"]["reassigned"] == 1
    programmes = {row["name"]: row["change"] for row in comparison.rows["programmes"]}
    assert programmes == {"Basis": "added", "Bridge": "removed", "General Programme": "removed"}


def test_network_and_findings():
    summary = scenario().summary
    network = {row["short"]: row for row in summary["network"]["perTechnology"]}
    assert network["ml"]["organisations"]["base"] == 2 and network["ml"]["organisations"]["candidate"] == 3
    assert summary["findings"][0] == "Score threshold changed from 5 to 6."
    assert not summary["snapshots"]["differs"]
