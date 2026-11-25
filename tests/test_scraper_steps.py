"""Offline tests for the scraper pipeline steps and the data source filter of the transformer.

Nothing here talks to FFG or FWF: the network parts are fed with small, hand-written responses.
"""

import io
import json

import pandas as pd
import pytest

from pipelineFramework import EventType
from pipelineFramework.tests.test_helper import get_default_user_config
from pipeline_configs.scraper_steps import combine as combine_module
from pipeline_configs.scraper_steps.combine import CombineScraperResults, DEFAULT_COLUMNS, combine_frames
from pipeline_configs.scraper_steps.ffg_fetch import FetchFFG, _FFGConfig, group_export
from pipeline_configs.scraper_steps.fwf_fetch import FetchFWF, _FWFConfig, transform_documents
from pipeline_configs.transform_steps import scraper as scraper_module
from pipeline_configs.transform_steps.scraper import GetScraperResults


async def collect(generator):
    return [event async for event in generator]


def result_of(events):
    results = [event for event, event_type in events if event_type == EventType.RESULT]
    assert len(results) == 1
    return results[0]


# --- FFG -------------------------------------------------------------------------------------------------------


def ffg_export():
    """Two projects as the Excel export delivers them: one row per project and organisation."""
    return pd.DataFrame(
        [
            {
                "Projekt-ID": 1,
                "Kurztitel": "AI",
                "Langtitel": "Artificial Intelligence",
                "Abstract": "About AI",
                "Programm": "P1",
                "Ausschreibung": "B1",
                "Projektstart": "2020-01-01",
                "Projektende": "2021-01-01",
                "Projektstatus": "beendet",
                "Keywords": "AI, ML",
                "Rolle im Projekt": "Koordinator",
                "Organisationsname": "Org A",
                "Organisationsart": "Unternehmen",
                "Website": None,
                "Staat": "Österreich",
                "Bundesland": "Wien",
                "Stadt": "Wien",
                "Adresse (Office)": "Street 1",
            },
            {
                "Projekt-ID": 1,
                "Kurztitel": "AI",
                "Langtitel": "Artificial Intelligence",
                "Abstract": None,
                "Programm": "P1",
                "Ausschreibung": "B1",
                "Projektstart": "2020-01-01",
                "Projektende": "2021-01-01",
                "Projektstatus": "beendet",
                "Keywords": "AI, ML",
                "Rolle im Projekt": "Partner",
                "Organisationsname": "Org B",
                "Organisationsart": "Hochschule",
                "Website": "https://b.at",
                "Staat": "Österreich",
                "Bundesland": "Steiermark",
                "Stadt": "Graz",
                "Adresse (Office)": "Street 2",
            },
            {
                "Projekt-ID": 2,
                "Kurztitel": "H2",
                "Langtitel": "Hydrogen",
                "Abstract": "About hydrogen",
                "Programm": "P2",
                "Ausschreibung": "B2",
                "Projektstart": "2022-01-01",
                "Projektende": "2023-01-01",
                "Projektstatus": "laufend",
                "Keywords": None,
                "Rolle im Projekt": "Koordinator",
                "Organisationsname": "Org C",
                "Organisationsart": "Unternehmen",
                "Website": None,
                "Staat": "Österreich",
                "Bundesland": "Tirol",
                "Stadt": "Innsbruck",
                "Adresse (Office)": "Street 3",
            },
        ]
    )


def test_ffg_group_export_one_row_per_project():
    config = _FFGConfig(get_default_user_config(FetchFFG()))
    grouped = group_export(ffg_export(), config).set_index("externalId")

    # Project 2 has no keywords and must still be kept.
    assert sorted(grouped.index) == [1, 2]
    assert "keyTechnologies" not in grouped.columns
    assert grouped.loc[1, "abstract"] == "About AI"

    organisations = json.loads(grouped.loc[1, "organisations"])
    assert [org["organisationName"] for org in organisations] == ["Org A", "Org B"]
    assert organisations[0]["organisationWebsite"] is None
    assert organisations[1]["role_in_project"] == "Partner"
    assert pd.isna(grouped.loc[2, "keywords"])


# --- FWF -------------------------------------------------------------------------------------------------------


def fwf_documents():
    return pd.DataFrame.from_records(
        [
            {
                "id": "P-1",
                "_str.url": "https://fwf.ac.at/P-1",
                "_str.projecttitle.de": "Maschinelles Lernen",
                "_str.projecttitle.en": "Machine Learning",
                "_str.grantdoi": "10.55776/P1",
                "_date.startdate": "2020-01-01",
                "_date.enddate": "2023-01-01",
                "_str.status.en": "Ended",
                "_long.approvedamount": 100000,
                "_str.prproposalsummary.de": "Zusammenfassung",
                "_str.prfinalreport.de": None,
                "_str.prproposalsummary.en": "Summary",
                "_list.researchareas.de": ["Informatik"],
                "_list.keywords.split": ["KI"],
                "_list.researchareas.en": ["Computer Science"],
                "_list.principalinvestigator.researchinstitute.name": "Uni Wien",
                "_list.researchinstitutes": ["Universität Wien (https://ror.org/03prydq77) - 100%"],
                "_list.nationalprojectparticipants.de": ["Max Muster, TU Graz, Universität"],
                "_list.internationalprojectparticipants.de": ["Jane Doe, MIT - USA, Research", "!!invalid!!"],
            },
            {
                "id": "P-2",
                "_str.projecttitle.de": "Wasserstoff",
            },
        ]
    )


def test_fwf_transform_documents():
    config = _FWFConfig(get_default_user_config(FetchFWF()))
    frame, invalid = transform_documents(fwf_documents(), config)
    frame = frame.set_index("externalId")

    assert list(frame.index) == ["P-1", "P-2"]
    assert "keyTechnologies" not in frame.columns
    assert frame.loc["P-1", "title"] == "Maschinelles Lernen"
    assert frame.loc["P-1", "title_en"] == "Machine Learning"
    assert frame.loc["P-1", "abstract"] == "Zusammenfassung"
    assert frame.loc["P-1", "keywords"] == ["Informatik", "KI"]
    assert frame.loc["P-1", "keywords_en"] == ["Computer Science"]
    assert frame.loc["P-2", "keywords"] == []

    organisations = json.loads(frame.loc["P-1", "organisations"])
    assert [org["organisationName"] for org in organisations] == ["Universität Wien", "TU Graz", "MIT"]
    assert organisations[0]["role_in_project"] == "LEADER"
    assert organisations[0]["organisationRor"] == "03prydq77"
    assert organisations[2]["organisationCountry"] == "USA"
    assert json.loads(frame.loc["P-2", "organisations"]) == []
    assert invalid == ['"!!invalid!!" (fwf_international_partner)']


class FakeResponse:
    def __init__(self, body):
        self._body = body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    def raise_for_status(self):
        pass

    async def json(self, **_):
        return self._body


class FakeSession:
    def __init__(self, documents, *_, **__):
        self.documents = documents
        self.requests = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    def get(self, _url, params=None, headers=None):
        self.requests.append(params)
        offset, limit = params["offset"], params["limit"]
        return FakeResponse({"results": self.documents[offset : offset + limit], "total": len(self.documents)})


@pytest.mark.asyncio
async def test_fwf_fetch_pages_through_all_documents(monkeypatch):
    documents = fwf_documents().to_dict("records") * 3
    for index, document in enumerate(documents):
        document["id"] = f"P-{index}"
    session = FakeSession(documents)
    monkeypatch.setattr("pipeline_configs.scraper_steps.fwf_fetch.aiohttp.ClientSession", lambda *_, **__: session)

    step = FetchFWF()
    config = {**get_default_user_config(step), "PAGE_SIZE": 4}
    frame = result_of(await collect(step.run(config)))

    assert len(frame) == 6
    assert [request["offset"] for request in session.requests] == [0, 4]


# --- Combine ---------------------------------------------------------------------------------------------------


def test_combine_frames():
    ffg = pd.DataFrame(
        [
            {"externalId": 1, "title": "AI", "bidding": "B1", "keywords": "AI, ML", "organisations": "[]"},
            {"externalId": 1, "title": "AI duplicate", "bidding": "B1", "keywords": "AI", "organisations": "[]"},
        ]
    )
    fwf = pd.DataFrame(
        [
            {
                "externalId": "P-1",
                "title": "ML",
                "grant": "10.55776/P1",
                "keywords": ["Informatik", "KI"],
                "keywords_en": [],
                "organisations": "[]",
                "_list.principalinvestigator.researchinstitute.name": "Uni Wien",
            }
        ]
    )
    combined = combine_frames({"FFG": ffg, "FWF": fwf}, DEFAULT_COLUMNS)

    assert list(combined.columns) == DEFAULT_COLUMNS
    assert "keyTechnologies" not in combined.columns
    assert list(combined["data_source"]) == ["FFG", "FWF"]
    assert combined.loc[0, "title"] == "AI"
    assert combined.loc[1, "keywords"] == "Informatik, KI"
    assert pd.isna(combined.loc[1, "keywords_en"])
    assert pd.isna(combined.loc[0, "grant"]) and pd.isna(combined.loc[1, "bidding"])


@pytest.mark.asyncio
async def test_combine_step_requires_both_sources():
    step = CombineScraperResults()
    with pytest.raises(FileNotFoundError, match="FWF"):
        await collect(step.run(get_default_user_config(step), {FetchFFG.name(): pd.DataFrame()}))


@pytest.mark.asyncio
async def test_combine_step_result():
    step = CombineScraperResults()
    results = {
        FetchFFG.name(): pd.DataFrame([{"externalId": 1, "title": "AI"}]),
        FetchFWF.name(): pd.DataFrame([{"externalId": "P-1", "title": "ML"}]),
    }
    combined = result_of(await collect(step.run(get_default_user_config(step), results)))
    assert list(combined["data_source"]) == ["FFG", "FWF"]
    assert combine_module.DATA_SOURCE_COLUMN in combined.columns


# --- Transformer: data source filter ---------------------------------------------------------------------------


def combined_results():
    return pd.DataFrame(
        [
            {"externalId": 1, "data_source": "FFG"},
            {"externalId": 2, "data_source": "FFG"},
            {"externalId": "P-1", "data_source": "FWF"},
        ]
    )


@pytest.fixture
def scraper_results(monkeypatch):
    async def fake_get_pipeline_results(_pipeline, _step):
        return combined_results()

    monkeypatch.setattr(scraper_module, "get_pipeline_results", fake_get_pipeline_results)


@pytest.mark.asyncio
@pytest.mark.parametrize("sources, expected", [(["FFG", "FWF"], 3), (["FFG"], 2), (["FWF"], 1)])
async def test_get_scraper_results_filters_data_sources(scraper_results, sources, expected):
    step = GetScraperResults()
    config = {**get_default_user_config(step), "DATA_SOURCES": sources}
    frame = result_of(await collect(step.run(config)))
    assert len(frame) == expected
    assert set(frame["data_source"]) == set(sources)


@pytest.mark.asyncio
async def test_get_scraper_results_default_selects_both(scraper_results):
    step = GetScraperResults()
    assert get_default_user_config(step)["DATA_SOURCES"] == ["FFG", "FWF"]


@pytest.mark.asyncio
@pytest.mark.parametrize("sources", [[], None])
async def test_get_scraper_results_requires_a_data_source(scraper_results, sources):
    step = GetScraperResults()
    config = {**get_default_user_config(step), "DATA_SOURCES": sources}
    with pytest.raises(ValueError, match="No data source selected"):
        await collect(step.run(config))


class FakeFFGResponse:
    def __init__(self, text=None, content=None):
        self._text, self._content = text, content

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    def raise_for_status(self):
        pass

    async def text(self):
        return self._text

    async def read(self):
        return self._content


class FakeFFGSession:
    """Serves search pages of project links and an Excel export for the requested ids."""

    def __init__(self, project_ids, ignore_offset=False):
        self.project_ids = project_ids
        self.ignore_offset = ignore_offset
        self.exports = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False

    def get(self, uri, ssl=None):
        start = 0 if self.ignore_offset else int(uri.split("&start=")[1].split("&")[0])
        links = "".join(f'<a href="/projekt/{i}">{i}</a>' for i in self.project_ids[start : start + 100])
        return FakeFFGResponse(text=f'<div id="searchresults">{links}</div>')

    def post(self, _uri, ssl=None, data=None):
        ids = data["id[]"]
        self.exports.append(ids)
        export = ffg_export()
        rows = [
            {**export.iloc[0].to_dict(), "Projekt-ID": project_id, "Kurztitel": f"P{project_id}"} for project_id in ids
        ]
        buffer = io.BytesIO()
        # The real export has four title rows above the header.
        pd.DataFrame(rows).to_excel(buffer, index=False, startrow=4)
        return FakeFFGResponse(content=buffer.getvalue())


@pytest.mark.asyncio
@pytest.mark.parametrize("ignore_offset", [False, True])
async def test_ffg_fetch_collects_all_ids_and_exports_in_batches(monkeypatch, ignore_offset):
    pytest.importorskip("openpyxl", reason="writing the fake Excel export needs openpyxl")
    project_ids = [str(i) for i in range(250)]
    session = FakeFFGSession(project_ids, ignore_offset)
    monkeypatch.setattr("pipeline_configs.scraper_steps.ffg_fetch.aiohttp.ClientSession", lambda *_, **__: session)

    step = FetchFFG()
    config = {**get_default_user_config(step), "EXPORT_BATCH_SIZE": 100, "SEARCH_CONCURRENCY": 2}
    frame = result_of(await collect(step.run(config)))

    # If the site ignored the offset, the guard stops after the first page instead of looping forever.
    expected = project_ids[:100] if ignore_offset else project_ids
    assert sorted(frame["externalId"].astype(str)) == sorted(expected)
    assert sum(len(batch) for batch in session.exports) == len(expected)
    assert max(len(batch) for batch in session.exports) <= 100
