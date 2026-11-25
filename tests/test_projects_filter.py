import asyncio
import json
import random
import re
import time
from collections import Counter

import pandas as pd
import pytest

from pipelineFramework import EventType
from pipeline_configs.transform_steps.project_extract import ProjectExtractStep
from pipeline_configs.transform_steps.project_normalize import ProjectNormalizeStep
from pipeline_configs.transform_steps.projects_filter import ProjectsFilterStep
from pipeline_configs.transform_steps.technology_matcher import (
    _Text,
    ScoreWeights,
    project_texts,
    score_project,
    technologies_from_config,
)

TECH_CONFIG = [
    {
        "label": "KI",
        "technologies": [
            {
                "label": "Maschinelles Lernen",
                "short": "ml",
                "searchTerms": {
                    "anyOf": ["Machine Learning", "supervised learning", "self-supervised learning", "LLM"],
                    "excluded": [],
                },
            },
        ],
    },
    {
        "label": "Energie",
        "technologies": [
            {
                "label": "Wasserstoff",
                "short": "h2",
                "searchTerms": {"anyOf": ["Wasserstoff"], "excluded": ["Wasserstoffperoxid", "Peroxid"]},
            },
            {"label": "SAF", "short": "saf", "searchTerms": {"anyOf": ["SAF"], "excluded": []}},
        ],
    },
]
TECHS = technologies_from_config(TECH_CONFIG)
FIELDS = {"title": ["title"], "keywords": ["keywords"], "abstract": ["abstract"]}


def score(project, weights=ScoreWeights()):
    return score_project(project_texts(project, FIELDS), TECHS, weights)


async def result_of(step, results, user_config=None):
    async for event, event_type in step.run(user_config=user_config or {}, results=results, events=[]):
        if event_type == EventType.RESULT:
            return event
    return None


def test_field_weights_add_up_and_repeats_count():
    match = score(
        {
            "title": "Machine learning for sensors",
            "keywords": "Machine Learning, Robotik",
            "abstract": "We apply machine learning. Machine Learning again.",
        }
    )
    entry = match["ml"]
    assert entry["matches"]["title"] == {"Machine Learning": 1}
    assert entry["matches"]["abstract"] == {"Machine Learning": 2}
    assert entry["score"] == 5 + 3 + 2


def test_whole_words_only():
    match = score({"title": "Safety of LLMs", "abstract": "Wasserstofftankstelle und LLM-basierte Agenten"})
    assert "saf" not in match
    assert "h2" not in match
    assert match["ml"]["matches"]["abstract"] == {"LLM": 1}


def test_overlapping_terms_count_once():
    match = score({"abstract": "self-supervised learning and unsupervised learning"})
    assert match["ml"]["matches"]["abstract"] == {"self-supervised learning": 1}


def test_phrases_match_across_line_breaks_but_not_across_keywords():
    assert score({"abstract": "machine\nlearning"})["ml"]["score"] == 1
    assert score({"keywords": ["Machine", "Learning"]}) == {}
    assert score({"keywords": "['Machine', 'Learning']"}) == {}


def test_excluded_terms_subtract_with_multiplier():
    project = {"title": "Peroxid", "abstract": "Wasserstoff und Wasserstoffperoxid"}
    assert score(project)["h2"]["score"] == 1 - (5 + 1)
    assert score(project, ScoreWeights(excluded_multiplier=2))["h2"]["score"] == 1 - 2 * (5 + 1)


def test_shorts_must_be_unique():
    config = [{"label": "F", "technologies": [TECH_CONFIG[1]["technologies"][0], TECH_CONFIG[1]["technologies"][0]]}]
    with pytest.raises(ValueError, match="h2"):
        technologies_from_config(config)


def test_projects_without_hits_have_no_entries():
    assert score({"title": "Nothing here", "abstract": None, "keywords": float("nan")}) == {}


def scraper_frame():
    base = {
        "start": "2024-01-15",
        "end": "2025-01-15",
        "status": "laufend",
        "keywords": "a, b",
        "keyTechnologies": json.dumps(["SAF"]),
        "organisations": json.dumps([{"organisationName": "TU Wien", "role_in_project": "LEADER"}]),
        "data_source": "FFG",
        "bidding": "Call 1",
        "programme": "Programme 1",
    }
    return pd.DataFrame(
        [
            {**base, "externalId": "1", "title": "Machine Learning project", "abstract": "Wasserstoff"},
            {**base, "externalId": "2", "title": "Other", "abstract": "Machine Learning"},
            {**base, "externalId": "3", "title": "Other", "title_en": "Machine Learning", "abstract": "x"},
        ]
    )


@pytest.mark.asyncio
async def test_filter_keeps_only_technologies_above_threshold():
    extracted = await result_of(ProjectExtractStep(), {"getScraperResults": scraper_frame()})
    filtered = await result_of(
        ProjectsFilterStep(), {"project_extract": extracted, "getTechnologyConfiguration": TECH_CONFIG}
    )

    assert [p["externalId"] for p in filtered] == ["1", "3"]
    assert json.loads(filtered[0]["keyTechnologies"]) == ["Maschinelles Lernen"]
    assert filtered[0]["_match"]["h2"] == {
        "label": "Wasserstoff",
        "score": 1.0,
        "matches": {"title": {}, "keywords": {}, "abstract": {"Wasserstoff": 1}},
        "excluded": {"title": {}, "keywords": {}, "abstract": {}},
        "passed": False,
    }
    assert filtered[0]["_match"]["ml"]["passed"] is True
    # The English title counts as title.
    assert filtered[1]["_match"]["ml"]["score"] == 5


@pytest.mark.asyncio
async def test_threshold_is_configurable():
    extracted = await result_of(ProjectExtractStep(), {"getScraperResults": scraper_frame()})
    filtered = await result_of(
        ProjectsFilterStep(),
        {"project_extract": extracted, "getTechnologyConfiguration": TECH_CONFIG},
        {"SCORE_THRESHOLD": 1},
    )
    assert [p["externalId"] for p in filtered] == ["1", "2", "3"]
    assert json.loads(filtered[0]["keyTechnologies"]) == ["Maschinelles Lernen", "Wasserstoff"]


@pytest.mark.asyncio
async def test_normalize_output_only_gains_the_match_field():
    frame = scraper_frame()
    extracted = await result_of(ProjectExtractStep(), {"getScraperResults": frame})
    status = {"STATUS_MAPPING": {"laufend": "ONGOING"}}

    # What normalize produced before projects_filter existed: extract output without the new columns, with the
    # keyTechnologies the scraper used to deliver (extract no longer passes it on, projects_filter sets it).
    legacy = [
        {
            **{k: v for k, v in p.items() if k not in ("grant", "title_en", "abstract_en", "keywords_en")},
            "keyTechnologies": None,
        }
        for p in extracted
    ]
    before = await result_of(ProjectNormalizeStep(), {"projects_filter": legacy}, status)

    filtered = await result_of(
        ProjectsFilterStep(), {"project_extract": extracted, "getTechnologyConfiguration": TECH_CONFIG}
    )
    after = await result_of(ProjectNormalizeStep(), {"projects_filter": filtered}, status)

    before_by_id = {p["externalId"]: p for p in before}
    for project in after:
        old = before_by_id[project["externalId"]]
        assert set(project) == set(old) | {"_match"}
        assert {k: v for k, v in project.items() if k not in ("_match", "keyTechnologies")} == {
            k: v for k, v in old.items() if k != "keyTechnologies"
        }
        assert isinstance(project["keyTechnologies"], str)


def scan_reference(terms, text):
    # The straightforward version the word index must agree with: scan the whole text.
    keys = sorted({t.casefold() for t in terms}, key=len, reverse=True)
    pattern = re.compile(
        r"(?<!\w)(?:" + "|".join(r"\s+".join(map(re.escape, k.split())) for k in keys) + r")(?!\w)", re.IGNORECASE
    )
    return Counter(" ".join(found.group(0).split()).casefold() for found in pattern.finditer(text))


def test_word_index_finds_the_same_matches_as_a_full_scan():
    rng = random.Random(5)
    words = ["self", "supervised", "learning", "machine", "LLM", "LLMs", "Wasserstoff", "peroxid", "saf", "x"]
    words += ["self-supervised learning", "supervised  learning", "Wasserstoffperoxid"]
    separators = [" ", "  ", "\n", "-", ", ", "_", ""]
    matched = 0
    for tech in TECHS:
        for _ in range(300):
            text = "".join(rng.choice(words) + rng.choice(separators) for _ in range(rng.randint(1, 12)))
            found = Counter()
            for patterns in (tech.positive_patterns, tech.excluded_patterns):
                found.update({term.casefold(): n for term, n in tech.count(patterns, [_Text(text)]).items()})
            expected = scan_reference(tech.any_of, text)
            if tech.excluded:
                expected += scan_reference(tech.excluded, text)
            assert found == expected, text
            matched += sum(found.values())
    assert matched > 100


@pytest.mark.asyncio
async def test_filter_does_not_stall_the_event_loop():
    abstract = " ".join(["Wir untersuchen Machine Learning und Wasserstoff in der Praxis."] * 40)
    projects = [
        {"externalId": str(i), "title": "Projekt", "abstract": abstract, "data_source": "FFG"} for i in range(3000)
    ]
    step = asyncio.create_task(
        result_of(ProjectsFilterStep(), {"project_extract": projects, "getTechnologyConfiguration": TECH_CONFIG})
    )
    longest = 0.0
    while not step.done():
        started = time.perf_counter()
        await asyncio.sleep(0.005)
        longest = max(longest, time.perf_counter() - started)

    assert len(step.result()) == 3000
    assert longest < 0.25
