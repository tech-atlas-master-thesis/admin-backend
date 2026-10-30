from pipeline_configs.transform_steps.organisations_normalize import (
    DEFAULT_UNIVERSITY_ALIASES,
    OrganisationNormalizeStep,
    clean_name,
)


def organisation(name, ror=None, website=None, city=None, type_="UNIVERSITY"):
    return {
        "name": name,
        "type": type_,
        "website": website,
        "ror": ror,
        "firmenbuchnummer": None,
        "data_id": "test",
        "address": {"country": "Österreich", "state": None, "city": city, "street": None},
    }


def deduplicate(*organisations, aliases=None):
    events = []
    result = OrganisationNormalizeStep().deduplicate_universities(
        {org["name"]: org for org in organisations},
        DEFAULT_UNIVERSITY_ALIASES if aliases is None else aliases,
        events,
    )
    return result, events


def test_clean_name():
    assert clean_name("  University of Virginia  ") == "University of Virginia"
    assert clean_name("Stanford University (nan)") == "Stanford University"
    assert clean_name("Universite de  Nice Sophia Antipolis") == "Universite de Nice Sophia Antipolis"


def test_institutes_are_merged_into_their_university():
    result, events = deduplicate(
        organisation("Technische Universität Graz", ror="00d7xrm67", website="https://tugraz.at"),
        organisation("Technische Universität Graz Institut für Experimentalphysik", ror="0inst", website="https://x"),
        organisation("Technische Universität Graz - Gebäude und Technik"),
        organisation("Universität Graz Institut für Physik", city="Graz"),
        organisation("Universität Graz"),
    )
    tu_graz = result["Technische Universität Graz"]
    assert tu_graz["aliases"] == ["Technische Universität Graz Institut für Experimentalphysik"]
    assert tu_graz["ror"] == "00d7xrm67"
    assert tu_graz["website"] == "https://tugraz.at"
    # unrecognised suffix: left untouched and reported
    assert "Technische Universität Graz - Gebäude und Technik" in result
    assert any("Gebäude und Technik" in event.message for event in events)
    # the longer university name wins, the address of the institute fills the gap
    assert result["Universität Graz"]["aliases"] == ["Universität Graz Institut für Physik"]
    assert result["Universität Graz"]["address"]["city"] == "Graz"


def test_sub_units_without_institute_word_at_start():
    result, _ = deduplicate(
        organisation("Medizinische Universität Graz"),
        organisation("Medizinische Universität Graz Klinische Abteilung für Kardiologie"),
        organisation("Medizinische Universität Graz Otto Loewi Forschungszentrum für Gefäßbiologie"),
        organisation("Universität Wien"),
        organisation("Universität Wien - Dep. of Behavioral and Cognitive Biology"),
        organisation("Universität Wien Forschungsgruppe Cooperative Systems"),
    )
    assert len(result["Medizinische Universität Graz"]["aliases"]) == 2
    assert len(result["Universität Wien"]["aliases"]) == 2


def test_other_universities_and_campuses_are_not_merged():
    names = [
        "University of California",
        "University of California, Berkeley",
        "University of California at Davis",
        "University of Texas",
        "University of Texas at Dallas",
        "Warsaw University",
        "Warsaw University of Technology",
        "Universität Koblenz",
        "Universität Koblenz-Landau",
        "Uppsala University",
        "Uppsala University Hospital",
        "University Medical Center Groningen",
    ]
    result, _ = deduplicate(*[organisation(name) for name in names])
    assert set(result) == set(names)
    assert not any(org.get("aliases") for org in result.values())


def test_university_without_own_entry_is_created_without_institute_ror():
    result, events = deduplicate(
        organisation("Washington University School of Medicine", ror="0inst", website="https://med.wustl.edu"),
        organisation("Washington University School of Medicine (nan)"),
    )
    assert list(result) == ["Washington University"]
    university = result["Washington University"]
    assert university["ror"] is None and university["website"] is None
    assert university["aliases"] == [
        "Washington University School of Medicine",
        "Washington University School of Medicine (nan)",
    ]
    assert any("created from its sub-units" in event.message for event in events)


def test_aliases_and_whitespace_variants():
    result, _ = deduplicate(
        organisation("Universität Linz"),
        organisation("Universität Linz "),
        organisation("Johannes Kepler Universität Linz Institut für Stochastik"),
        organisation("Friedrich-Alexander-Universität Erlangen-Nürnberg"),
        organisation("Friedrich-Alexander-Universität Erlangen"),
    )
    assert result["Universität Linz"]["aliases"] == [
        "Johannes Kepler Universität Linz Institut für Stochastik",
        "Universität Linz ",
    ]
    assert result["Friedrich-Alexander-Universität Erlangen-Nürnberg"]["aliases"] == [
        "Friedrich-Alexander-Universität Erlangen"
    ]


def test_every_original_name_stays_resolvable():
    names = [
        "Universität Innsbruck",
        "Universität Innsbruck - Rektorat",
        "Universität Innsbruck Institut für Informatik",
        "Universität für Bodenkultur Institut für Landschaftsarchitektur",
        "Universität für Bodenkultur Wien",
        "Robots in Architecture",
    ]
    result, _ = deduplicate(*[organisation(name) for name in names])
    resolvable = set(result) | {alias for org in result.values() for alias in org.get("aliases", [])}
    assert set(names) <= resolvable
    assert "Robots in Architecture" in result
