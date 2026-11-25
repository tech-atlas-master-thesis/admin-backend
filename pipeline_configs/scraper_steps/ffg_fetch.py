import asyncio
import datetime
import json
import math
import re
from io import BytesIO
from typing import Optional, List, Union, Dict, Any, Set

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    StepUserConfig,
    LocalisationString,
    LocalisationStringType,
    EventType,
)

SEARCH_PAGE_SIZE = 100


class FetchFFG(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if user_config is None:
            raise FileNotFoundError("User Config not provided")
        config = _FFGConfig(user_config)

        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, sock_read=300)) as session:
            ids: List[str] = []
            async for event in self._collect_ids(session, config, ids):
                yield event
            yield f"Found {len(ids)} FFG projects", EventType.INFO
            if not ids:
                yield "FFG returned no projects", EventType.WARNING
                yield pd.DataFrame(columns=[*config.project_columns, *config.first_instance_columns]), EventType.RESULT
                return

            batches = [ids[i : i + config.export_batch_size] for i in range(0, len(ids), config.export_batch_size)]
            semaphore = asyncio.Semaphore(config.export_concurrency)
            tasks = [asyncio.create_task(self._export(session, config, batch, semaphore)) for batch in batches]
            frames = []
            for done, task in enumerate(asyncio.as_completed(tasks), start=1):
                frames.append(await task)
                yield f"Downloaded export {done}/{len(batches)}", EventType.INFO

        frame = group_export(pd.concat(frames, ignore_index=True), config)
        yield f"Grouped export into {len(frame)} FFG projects", EventType.INFO
        yield frame, EventType.RESULT

    async def _collect_ids(self, session: aiohttp.ClientSession, config: "_FFGConfig", ids: List[str]):
        seen: Set[str] = set()
        page = 0
        while True:
            starts = [(page + i) * SEARCH_PAGE_SIZE for i in range(config.search_concurrency)]
            pages = await asyncio.gather(*(self._search_page(session, config, start) for start in starts))
            finished = False
            for page_ids in pages:
                new_ids = [project_id for project_id in page_ids if project_id not in seen]
                if not new_ids:
                    finished = True
                    break
                seen.update(new_ids)
                ids.extend(new_ids)
            page += config.search_concurrency
            if finished:
                return
            if page % (config.search_concurrency * 25) == 0:
                yield f"Collected {len(ids)} FFG project ids", EventType.INFO

    async def _search_page(self, session: aiohttp.ClientSession, config: "_FFGConfig", start: int) -> List[str]:
        uri = config.search_request_uri.format(
            query="",
            page=start,
            title="",
            dateFrom=_search_date(config.search_date_from),
            dateUntil=_search_date(config.search_date_until),
            program="",
            grant="",
            status="",
        )
        async with session.get(uri, ssl=False) as response:
            response.raise_for_status()
            soup = BeautifulSoup(await response.text(), "html.parser")
        results = soup.find(id="searchresults")
        if results is None:
            return []
        page_ids = []
        for link in results.find_all("a", href=config.id_href_regex):
            match = config.id_href_regex.search(link.get("href", ""))
            if match:
                page_ids.append(match.group(1))
        return list(dict.fromkeys(page_ids))

    async def _export(
        self, session: aiohttp.ClientSession, config: "_FFGConfig", ids: List[str], semaphore: asyncio.Semaphore
    ) -> pd.DataFrame:
        async with semaphore:
            async with session.post(
                config.excel_request_uri,
                ssl=False,
                data={"id[]": ids, "projects_selected": str(len(ids)), "projects_total": str(len(ids))},
            ) as response:
                response.raise_for_status()
                content = await response.read()
        return pd.read_excel(BytesIO(content), engine="calamine", skiprows=4)

    def user_config(self) -> List[StepUserConfig]:
        return [
            StepUserConfig(
                "EXCEL_REQUEST_URI",
                LocalisationString("URI to request Excel file", "URI um Excel File zu beantragen"),
                None,
                StepUserConfig.StepUserConfigType.STRING,
                "https://projekte.ffg.at/projekt/excel",
            ),
            StepUserConfig(
                "SEARCH_REQUEST_URI",
                LocalisationString("Project Search Request URI", "Projektsuche URI"),
                None,
                StepUserConfig.StepUserConfigType.STRING,
                "https://projekte.ffg.at/projekt?advanced_search=1"
                "&go=1"
                "&q={query}"
                "&start={page}"
                "&titel={title}"
                "&projektbeginn={dateFrom}"
                "&projektende={dateUntil}"
                "&foerderung={program}"
                "&foerdernehmer={grant}"
                "&status={status}",
            ),
            StepUserConfig(
                "SEARCH_DATE_FROM",
                LocalisationString("Search Project Start Date From", "Suche Projektbeginn von"),
                None,
                StepUserConfig.StepUserConfigType.DATE,
                None,
                required=False,
                format="dd.mm.yy",
            ),
            StepUserConfig(
                "SEARCH_DATE_UNTIL",
                LocalisationString("Search Project End Date Until", "Suche Projektende bis"),
                None,
                StepUserConfig.StepUserConfigType.DATE,
                None,
                required=False,
                format="dd.mm.yy",
            ),
            StepUserConfig(
                "SEARCH_CONCURRENCY",
                LocalisationString("Parallel search requests", "Parallele Suchanfragen"),
                LocalisationString(
                    "Search result pages requested at the same time while collecting project ids",
                    "Gleichzeitig angefragte Suchergebnisseiten beim Sammeln der Projekt-IDs",
                ),
                StepUserConfig.StepUserConfigType.INTEGER,
                4,
            ),
            StepUserConfig(
                "EXPORT_BATCH_SIZE",
                LocalisationString("Projects per Excel export", "Projekte pro Excel Export"),
                LocalisationString(
                    "Project ids sent in one Excel export request",
                    "Projekt-IDs, die in einer Excel Export Anfrage gesendet werden",
                ),
                StepUserConfig.StepUserConfigType.INTEGER,
                500,
            ),
            StepUserConfig(
                "EXPORT_CONCURRENCY",
                LocalisationString("Parallel Excel exports", "Parallele Excel Exporte"),
                LocalisationString("Excel exports requested at the same time", "Gleichzeitig angefragte Excel Exporte"),
                StepUserConfig.StepUserConfigType.INTEGER,
                2,
            ),
            StepUserConfig(
                "ID_HREF_REGEX",
                LocalisationString("RegExp to find href with ID", "RegExp um href mit ID zu finden"),
                None,
                StepUserConfig.StepUserConfigType.STRING,
                "/projekt/([a-z0-9-]+)",
            ),
            StepUserConfig(
                "COLUMN_TRANSLATIONS",
                LocalisationString("Columns renaming", "Spalten Umbenennung"),
                None,
                StepUserConfig.StepUserConfigType.MAPPING,
                {
                    "Projekt-ID": "externalId",
                    "Kurztitel": "short",
                    "Langtitel": "title",
                    "Abstract": "abstract",
                    "Programm": "programme",
                    "Ausschreibung": "bidding",
                    "Projektstart": "start",
                    "Projektende": "end",
                    "Projektstatus": "status",
                    "Keywords": "keywords",
                    "Rolle im Projekt": "role_in_project",
                    "Organisationsname": "organisationName",
                    "Organisationsart": "organisationType",
                    "Website": "organisationWebsite",
                    "Staat": "organisationCountry",
                    "Bundesland": "organisationState",
                    "Stadt": "organisationCity",
                    "Adresse (Office)": "organisationStreet",
                },
            ),
            StepUserConfig(
                "PROJECT_COLUMNS",
                LocalisationString("Columns containing project data", "Spalten mit Projektdaten"),
                None,
                StepUserConfig.StepUserConfigType.LIST,
                [
                    "externalId",
                    "short",
                    "title",
                    "programme",
                    "bidding",
                    "start",
                    "end",
                    "status",
                    "keywords",
                ],
            ),
            StepUserConfig(
                "PROJECT_COLUMNS_ONLY_ON_FIRST_INSTANCE",
                LocalisationString(
                    "Columns containing project data only once", "Spalten mit Prjektdaten nur einmal vorhanden"
                ),
                None,
                StepUserConfig.StepUserConfigType.LIST,
                ["abstract"],
            ),
            StepUserConfig(
                "ORGANISATION_COLUMNS",
                LocalisationString("Columns with organisation data", "Zeilen mit Organisationsdaten"),
                None,
                StepUserConfig.StepUserConfigType.LIST,
                [
                    "role_in_project",
                    "organisationName",
                    "organisationType",
                    "organisationWebsite",
                    "organisationCountry",
                    "organisationState",
                    "organisationCity",
                    "organisationStreet",
                ],
            ),
            StepUserConfig(
                "ORGANISATIONS_COLUMN_NAME",
                LocalisationString("Organisations column", "Organistionsspalte"),
                None,
                StepUserConfig.StepUserConfigType.STRING,
                "organisations",
            ),
        ]

    @staticmethod
    def name() -> str:
        return "getDataFFG"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Get Data from FFG", "Daten von FFG laden")

    def description(self) -> LocalisationStringType:
        return LocalisationString(
            "Fetches every project of the FFG project database",
            "Lädt alle Projekte der FFG Projektdatenbank",
        )

    def dependencies(self) -> Union[List[str], None]:
        return None


class _FFGConfig:
    def __init__(self, user_config: UserStepConfig):
        self.excel_request_uri: str = user_config.get("EXCEL_REQUEST_URI")
        self.search_request_uri: str = user_config.get("SEARCH_REQUEST_URI")
        self.search_date_from = user_config.get("SEARCH_DATE_FROM")
        self.search_date_until = user_config.get("SEARCH_DATE_UNTIL")
        self.search_concurrency: int = max(1, int(user_config.get("SEARCH_CONCURRENCY") or 4))
        self.export_batch_size: int = max(1, int(user_config.get("EXPORT_BATCH_SIZE") or 500))
        self.export_concurrency: int = max(1, int(user_config.get("EXPORT_CONCURRENCY") or 2))
        self.id_href_regex = re.compile(user_config.get("ID_HREF_REGEX"))
        self.column_translations: Dict[str, str] = user_config.get("COLUMN_TRANSLATIONS")
        self.project_columns: List[str] = user_config.get("PROJECT_COLUMNS")
        self.first_instance_columns: List[str] = user_config.get("PROJECT_COLUMNS_ONLY_ON_FIRST_INSTANCE") or []
        self.organisation_columns: List[str] = user_config.get("ORGANISATION_COLUMNS")
        self.organisations_column: str = user_config.get("ORGANISATIONS_COLUMN_NAME")


def group_export(frame: pd.DataFrame, config: _FFGConfig) -> pd.DataFrame:
    frame = frame.rename(columns=config.column_translations)
    for column in [*config.project_columns, *config.first_instance_columns, *config.organisation_columns]:
        if column not in frame.columns:
            frame[column] = None
    frame[config.organisations_column] = [
        {key: _clean(value) for key, value in record.items()}
        for record in frame[config.organisation_columns].to_dict("records")
    ]
    return frame.groupby(config.project_columns, as_index=False, dropna=False, sort=False).agg(
        {
            config.organisations_column: lambda organisations: json.dumps(organisations.to_list()),
            **dict.fromkeys(config.first_instance_columns, "first"),
        }
    )


def _clean(value: Any) -> Any:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if isinstance(value, (pd.Timestamp, datetime.datetime, datetime.date)):
        return value.isoformat()
    return value


def _search_date(value: datetime.datetime | str | None) -> str:
    if not value:
        return ""
    value = datetime.datetime.fromisoformat(value) if isinstance(value, str) else value
    return value.strftime("%d.%m.%Y")
