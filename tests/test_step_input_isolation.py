import copy
import json

import pandas as pd
import pytest

from pipelineFramework import EventType
from pipeline_configs.transform_steps.grant_extract import GrantExtractStep
from pipeline_configs.transform_steps.project_enrich import ProjectEnrichStep
from pipeline_configs.transform_steps.project_extract import ProjectExtractStep
from pipeline_configs.transform_steps.project_normalize import ProjectNormalizeStep
from pipeline_configs.transform_steps.projects_filter import ProjectsFilterStep

TECH_CONFIG = [
    {
        "label": "Field",
        "technologies": [{"label": "AI", "short": "ai", "searchTerms": {"anyOf": ["AI"], "excluded": []}}],
    }
]


async def result_of(step, results, user_config=None):
    async for event, event_type in step.run(user_config=user_config or {}, results=results, events=[]):
        if event_type == EventType.RESULT:
            return event
    return None


def scraper_frame():
    return pd.DataFrame(
        [
            {
                "externalId": "1",
                "title": "AI Project",
                "start": "2024-01-15",
                "end": "2025-01-15",
                "status": "laufend",
                "keywords": "a, b",
                "keyTechnologies": json.dumps(["AI"]),
                "organisations": json.dumps([{"organisationName": "TU Wien", "role_in_project": "LEADER"}]),
                "data_source": "FFG",
                "bidding": "Call 1",
                "programme": "Programme 1",
            }
        ]
    )


@pytest.mark.asyncio
async def test_project_extract_leaves_the_shared_scraper_frame_untouched():
    frame = scraper_frame().drop(columns=["programme"])
    before = frame.copy(deep=True)

    projects = await result_of(ProjectExtractStep(), {"getScraperResults": frame})

    pd.testing.assert_frame_equal(frame, before)
    assert projects[0]["programme"] is None
    assert projects[0]["start"] == pd.Timestamp("2024-01-15", tz="UTC")


@pytest.mark.asyncio
async def test_grant_extract_skips_projects_without_funding_data():
    # project_extract fills missing columns with None; grant_extract must not turn those into grants.
    frame = scraper_frame().drop(columns=["bidding", "programme"])
    projects = await result_of(ProjectExtractStep(), {"getScraperResults": frame})
    filtered = await result_of(
        ProjectsFilterStep(), {"project_extract": projects, "getTechnologyConfiguration": TECH_CONFIG}
    )

    assert await result_of(GrantExtractStep(), {"projects_filter": filtered}) == {}


@pytest.mark.asyncio
async def test_project_steps_work_when_each_receives_its_own_copy():
    results = {"getScraperResults": scraper_frame()}
    results["project_extract"] = await result_of(ProjectExtractStep(), results)
    results["getTechnologyConfiguration"] = TECH_CONFIG
    results["projects_filter"] = await result_of(ProjectsFilterStep(), copy.deepcopy(results))
    results["project_normalize"] = await result_of(
        ProjectNormalizeStep(), copy.deepcopy(results), {"STATUS_MAPPING": {"laufend": "ONGOING"}}
    )
    results.update(
        {
            "technologies": {"AI": "tech-id"},
            "organisation_database": {"TU Wien": "org-id"},
            "grant_database": {"Call 1": "grant-id"},
        }
    )

    projects = await result_of(ProjectEnrichStep(), copy.deepcopy(results))

    assert projects[0]["grant"] == "grant-id"
    assert projects[0]["keyTechnologies"] == ["tech-id"]
    assert projects[0]["organisations"] == ["org-id"]
    assert projects[0]["projectLeader"] == "org-id"
    assert results["project_extract"][0]["grant"] is None
    assert "_match" not in results["project_extract"][0]
    assert results["projects_filter"][0]["grant"] is None
