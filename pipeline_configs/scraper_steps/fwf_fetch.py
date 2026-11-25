import json
import re
from typing import Optional, List, Union, Dict, Any, Tuple

import aiohttp
import numpy as np
import pandas as pd

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    StepUserConfig,
    LocalisationString,
    LocalisationStringType,
    EventType,
)

ABSTRACT_COLUMN = "abstract"
KEYWORD_COLUMN = "keywords"
TITLE_EN_COLUMN = "title_en"
ABSTRACT_EN_COLUMN = "abstract_en"
KEYWORD_EN_COLUMN = "keywords_en"
ORGANISATION_COLUMN = "organisations"

RESEARCH_INSTITUTIONS_IDENTIFIER = "fwf_research_institution"
NATIONAL_PARTNERS_IDENTIFIER = "fwf_national_partner"
INTERNATIONAL_PARTNERS_IDENTIFIER = "fwf_international_partner"

MAX_REGEX_WARNING_EXAMPLES = 20


class FetchFWF(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if user_config is None:
            raise FileNotFoundError("User Config not provided")
        config = _FWFConfig(user_config)

        documents: List[Dict[str, Any]] = []
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, sock_read=120)) as session:
            offset = 0
            while True:
                page, total = await self._fetch_page(session, config, offset)
                documents.extend(page)
                if total is not None and offset == 0:
                    yield f"FWF reports {total} projects", EventType.INFO
                offset += len(page)
                if not page or len(page) < config.page_size or (total is not None and offset >= total):
                    break
                if (offset // config.page_size) % 10 == 0:
                    yield f"Fetched {offset} FWF projects", EventType.INFO

        yield f"Fetched {len(documents)} FWF projects", EventType.INFO
        if not documents:
            yield "FWF returned no projects", EventType.WARNING
            yield pd.DataFrame(columns=config.relevant_columns), EventType.RESULT
            return

        frame, invalid_entries = transform_documents(pd.DataFrame.from_records(documents), config)
        if invalid_entries and config.output_regex_warnings:
            examples = "\n".join(invalid_entries[:MAX_REGEX_WARNING_EXAMPLES])
            yield (
                f"{len(invalid_entries)} organisation entries did not match their regex and were skipped. "
                f"Examples:\n{examples}",
                EventType.WARNING,
            )
        yield frame, EventType.RESULT

    async def _fetch_page(
        self, session: aiohttp.ClientSession, config: "_FWFConfig", offset: int
    ) -> Tuple[List[Dict[str, Any]], Optional[int]]:
        params = {"offset": offset, "limit": config.page_size, "fields": ",".join(config.query_required_fields)}
        async with session.get(config.documents_endpoint, params=params, headers=config.headers) as response:
            response.raise_for_status()
            body = await response.json(content_type=None)
        return body.get(config.items_field) or [], body.get("total")

    def user_config(self) -> List[StepUserConfig]:
        return [
            StepUserConfig(
                "DOCUMENTS_ENDPOINT",
                LocalisationString("Documents URI", "Dokumente URI"),
                LocalisationString(
                    "URI of the Meilisearch documents endpoint of the projects index. Pages through every project.",
                    "URI des Meilisearch Dokumente Endpunktes des Projekt-Index. Lädt alle Projekte seitenweise.",
                ),
                StepUserConfig.StepUserConfigType.STRING,
                "https://openapi.fwf.ac.at/indexes/projects/documents",
            ),
            StepUserConfig(
                "ENDPOINT_HEADERS",
                LocalisationString("Endpoint Headers", "Endpunkt Header"),
                LocalisationString(
                    "Additional Headers required by the endpoint, f.e.: Auth",
                    "Zusätzliche Header für den Endpunkt benötigt, z.B.: Auth",
                ),
                StepUserConfig.StepUserConfigType.MAPPING,
                {
                    "Authorization": "Bearer 3a03f2f39cc8a99ea0775270adb4946c425469aa7f291e7ca9f2d8424337c1af",
                },
            ),
            StepUserConfig(
                "ITEMS_FIELD",
                LocalisationString("Result field", "Resultatfeld"),
                LocalisationString("JSON field holding the documents", "JSON Feld mit den Dokumenten"),
                StepUserConfig.StepUserConfigType.STRING,
                "results",
            ),
            StepUserConfig(
                "PAGE_SIZE",
                LocalisationString("Page Size", "Seitengröße"),
                LocalisationString("Documents fetched per request", "Dokumente pro Anfrage"),
                StepUserConfig.StepUserConfigType.INTEGER,
                1000,
            ),
            StepUserConfig(
                "COLUMN_TRANSLATIONS",
                LocalisationString("Columns renaming", "Spalten Umbenennung"),
                None,
                StepUserConfig.StepUserConfigType.MAPPING,
                {
                    "id": "externalId",
                    "_str.url": "uri",
                    "_str.projecttitle.de": "title",
                    "_str.grantdoi": "grant",
                    "_date.startdate": "start",
                    "_date.enddate": "end",
                    "_str.status.en": "status",
                    "_long.approvedamount": "funding_amount",
                },
            ),
            StepUserConfig(
                "KEYWORD_CONCAT",
                LocalisationString("Keyword fields", "Keyword Felder"),
                LocalisationString(
                    "Field to concatenate to generate the keywords column",
                    "Zu konkatenierende Felder um die keyword Spalte zu erstellen",
                ),
                StepUserConfig.StepUserConfigType.LIST,
                [
                    "_list.researchareas.de",
                    "_list.researchfields.de",
                    "_list.researchdisciplines.de",
                    "_list.keywords.split",
                ],
            ),
            StepUserConfig(
                "ABSTRACT_CONCAT",
                LocalisationString("Abstract fields", "Abstract Felder"),
                LocalisationString(
                    "Field to concatenate to generate the abstract column",
                    "Zu konkatenierende Felder um die Abstract Spalte zu erstellen",
                ),
                StepUserConfig.StepUserConfigType.LIST,
                ["_str.prproposalsummary.de", "_str.prfinalreport.de"],
            ),
            StepUserConfig(
                "TITLE_EN_FIELD",
                LocalisationString("English title field", "Englisches Titelfeld"),
                LocalisationString(
                    "Field with the English project title. Only used to match the technologies.",
                    "Feld mit dem englischen Projekttitel. Wird nur zur Zuordnung der Technologien verwendet.",
                ),
                StepUserConfig.StepUserConfigType.STRING,
                "_str.projecttitle.en",
            ),
            StepUserConfig(
                "ABSTRACT_EN_CONCAT",
                LocalisationString("English abstract fields", "Englische Abstract Felder"),
                LocalisationString(
                    "Fields concatenated to the English abstract. Only used to match the technologies.",
                    "Zu konkatenierende Felder für den englischen Abstract. Wird nur zur Zuordnung der "
                    "Technologien verwendet.",
                ),
                StepUserConfig.StepUserConfigType.LIST,
                ["_str.prproposalsummary.en", "_str.prfinalreport.en"],
            ),
            StepUserConfig(
                "KEYWORD_EN_CONCAT",
                LocalisationString("English keyword fields", "Englische Keyword Felder"),
                LocalisationString(
                    "Fields concatenated to the English keywords. Only used to match the technologies.",
                    "Zu konkatenierende Felder für die englischen Keywords. Wird nur zur Zuordnung der "
                    "Technologien verwendet.",
                ),
                StepUserConfig.StepUserConfigType.LIST,
                ["_list.researchareas.en", "_list.researchfields.en", "_list.researchdisciplines.en"],
            ),
            StepUserConfig(
                "ORGANIZATION_PROJECT_LEADER",
                LocalisationString("Project Leader Field", "Projektleiterfeld"),
                LocalisationString(
                    "Field containing the project leader",
                    "Feld das den Projektleiter beinhaltet",
                ),
                StepUserConfig.StepUserConfigType.STRING,
                "_list.principalinvestigator.researchinstitute.name",
            ),
            StepUserConfig(
                "ORGANISATION_RESEARCH_INSTITUTIONS",
                LocalisationString("Research institutes Field", "Forschungsinstitutefeld"),
                LocalisationString(
                    "Field containing the research institutions",
                    "Feld das die Forschungsinstitute beinhaltet",
                ),
                StepUserConfig.StepUserConfigType.STRING,
                "_list.researchinstitutes",
            ),
            StepUserConfig(
                "ORGANISATION_RESEARCH_INSTITUTIONS_ADDITIONAL_DATA",
                LocalisationString("Research Institutions Additional Data", "Forschungsinstitute Zusatzdate"),
                LocalisationString(
                    "Additional data to be added to the research institutions",
                    "Zusatzdaten die Forschungsinstituten hinzugefügt werden soll",
                ),
                StepUserConfig.StepUserConfigType.MAPPING,
                {
                    "role_in_project": "LEADER",
                },
            ),
            StepUserConfig(
                "ORGANISATION_RESEARCH_INSTITUTIONS_REGEX",
                LocalisationString("Research institutes Regex Extractor", "Forschungsinstitute Regex Extraktor"),
                LocalisationString(
                    "Regex for extracting research institutions data",
                    "Regex das die Daten der Forschungsinstitute extrahiert",
                ),
                StepUserConfig.StepUserConfigType.STRING,
                r"^(?P<organisationName>[A-Za-zŽžÀ-ÿ0-9 .\-,()]+)( \(https:\/\/ror\.org\/(?P<organisationRor>[a-z0-9]+)\))?( - [0-9]+\%)?$",
            ),
            StepUserConfig(
                "ORGANISATION_NATIONAL_PARTNERS",
                LocalisationString("National Partners Field", "Nationale Partner Feld"),
                LocalisationString(
                    "Field containing the national project partners",
                    "Feld das die nationalen Projektpartner beinhaltet",
                ),
                StepUserConfig.StepUserConfigType.STRING,
                "_list.nationalprojectparticipants.de",
            ),
            StepUserConfig(
                "ORGANISATION_NATIONAL_PARTNERS_ADDITIONAL_DATA",
                LocalisationString("National Partners Additional Data", "Nationale Partner Zusatzdate"),
                LocalisationString(
                    "Additional data to be added to the national project partners",
                    "Zusatzdaten die nationalen Projektpartnern hinzugefügt werden soll",
                ),
                StepUserConfig.StepUserConfigType.MAPPING,
                {},
            ),
            StepUserConfig(
                "ORGANISATION_NATIONAL_PARTNERS_REGEX",
                LocalisationString("National Partners Regex Extractor", "Nationale Partner Regex Extraktor"),
                LocalisationString(
                    "Regex for extracting national partners data",
                    "Regex das die Daten der nationalen Partner extrahiert",
                ),
                StepUserConfig.StepUserConfigType.STRING,
                r"^([A-Za-zŽžÀ-ÿ .\-]+, )(?P<organisationName>[A-Za-zŽžÀ-ÿ0-9 .\-,()]+)( \(https:\/\/ror\.org\/(?P<organisationRor>[a-z0-9]+)\))?, (?P<organisationType>[A-Za-zŽžÀ-ÿ: ]+)",
            ),
            StepUserConfig(
                "ORGANISATION_INTERNATIONAL_PARTNERS",
                LocalisationString("International Partners Field", "Internationale Partner Feld"),
                LocalisationString(
                    "Field containing the international project partners",
                    "Feld das die internationalen Projektpartner beinhaltet",
                ),
                StepUserConfig.StepUserConfigType.STRING,
                "_list.internationalprojectparticipants.de",
            ),
            StepUserConfig(
                "ORGANISATION_INTERNATIONAL_PARTNERS_ADDITIONAL_DATA",
                LocalisationString("International Partners Additional Data", "Internationale Partner Zusatzdate"),
                LocalisationString(
                    "Additional data to be added to the international project partners",
                    "Zusatzdaten die internationalen Projektpartnern hinzugefügt werden soll",
                ),
                StepUserConfig.StepUserConfigType.MAPPING,
                {},
            ),
            StepUserConfig(
                "ORGANISATION_INTERNATIONAL_PARTNERS_REGEX",
                LocalisationString("International Partners Regex Extractor", "Internationale Partner Regex Extraktor"),
                LocalisationString(
                    "Regex for extracting international project data",
                    "Regex das die Daten der internationalen Projektpartner extrahiert",
                ),
                StepUserConfig.StepUserConfigType.STRING,
                r"^([A-Za-zŽžÀ-ÿ .\-]+, )(?P<organisationName>[A-Za-zŽžÀ-ÿ0-9 .\-,()]+)( \(https:\/\/ror\.org\/(?P<organisationRor>[a-z0-9]+)\))? - (?P<organisationCountry>[A-Za-zŽžÀ-ÿ: ]+)(, (?P<organisationType>[A-Za-zŽžÀ-ÿ: ]+))?",
            ),
            StepUserConfig(
                "OUTPUT_REGEX_WARNINGS",
                LocalisationString("Regex Warnings", "Regex Warnungen"),
                LocalisationString(
                    "Toggle if a warning about unparseable organisation entries should be output",
                    "Umschalten, ob eine Warnung zu nicht lesbaren Organisationseinträgen ausgegeben werden soll",
                ),
                StepUserConfig.StepUserConfigType.BOOLEAN,
                True,
            ),
        ]

    @staticmethod
    def name() -> str:
        return "getDataFWF"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Get Data from FWF", "Daten von FWF laden")

    def description(self) -> LocalisationStringType:
        return LocalisationString(
            "Fetches every project of the FWF Open API",
            "Lädt alle Projekte der FWF Open API",
        )

    def dependencies(self) -> Union[List[str], None]:
        return None


class _FWFConfig:
    def __init__(self, user_config: UserStepConfig):
        self.documents_endpoint: str = user_config.get("DOCUMENTS_ENDPOINT")
        self.headers: Dict[str, str] = user_config.get("ENDPOINT_HEADERS") or {}
        self.items_field: str = user_config.get("ITEMS_FIELD") or "results"
        self.page_size: int = int(user_config.get("PAGE_SIZE") or 1000)
        self.column_translations: Dict[str, str] = user_config.get("COLUMN_TRANSLATIONS")
        self.keyword_concat: List[str] = user_config.get("KEYWORD_CONCAT")
        self.abstract_concat: List[str] = user_config.get("ABSTRACT_CONCAT")
        self.title_en_field: str = user_config.get("TITLE_EN_FIELD")
        self.abstract_en_concat: List[str] = user_config.get("ABSTRACT_EN_CONCAT")
        self.keyword_en_concat: List[str] = user_config.get("KEYWORD_EN_CONCAT")
        self.project_leader: str = user_config.get("ORGANIZATION_PROJECT_LEADER")
        self.organisation_sources = [
            (
                user_config.get("ORGANISATION_RESEARCH_INSTITUTIONS"),
                user_config.get("ORGANISATION_RESEARCH_INSTITUTIONS_REGEX"),
                RESEARCH_INSTITUTIONS_IDENTIFIER,
                user_config.get("ORGANISATION_RESEARCH_INSTITUTIONS_ADDITIONAL_DATA") or {},
            ),
            (
                user_config.get("ORGANISATION_NATIONAL_PARTNERS"),
                user_config.get("ORGANISATION_NATIONAL_PARTNERS_REGEX"),
                NATIONAL_PARTNERS_IDENTIFIER,
                user_config.get("ORGANISATION_NATIONAL_PARTNERS_ADDITIONAL_DATA") or {},
            ),
            (
                user_config.get("ORGANISATION_INTERNATIONAL_PARTNERS"),
                user_config.get("ORGANISATION_INTERNATIONAL_PARTNERS_REGEX"),
                INTERNATIONAL_PARTNERS_IDENTIFIER,
                user_config.get("ORGANISATION_INTERNATIONAL_PARTNERS_ADDITIONAL_DATA") or {},
            ),
        ]
        self.output_regex_warnings: bool = bool(user_config.get("OUTPUT_REGEX_WARNINGS"))

        self.relevant_columns = list(
            dict.fromkeys(
                [
                    *self.column_translations.values(),
                    ABSTRACT_COLUMN,
                    KEYWORD_COLUMN,
                    ORGANISATION_COLUMN,
                    self.project_leader,
                    TITLE_EN_COLUMN,
                    ABSTRACT_EN_COLUMN,
                    KEYWORD_EN_COLUMN,
                ]
            )
        )
        self.query_required_fields = list(
            dict.fromkeys(
                [
                    *self.column_translations.keys(),
                    *self.abstract_concat,
                    *self.keyword_concat,
                    self.title_en_field,
                    *self.abstract_en_concat,
                    *self.keyword_en_concat,
                    self.project_leader,
                    *(source[0] for source in self.organisation_sources),
                ]
            )
        )


def transform_documents(df: pd.DataFrame, config: _FWFConfig) -> Tuple[pd.DataFrame, List[str]]:
    """Turns raw FWF documents into the scraper format. Returns the frame and unparseable organisation entries."""
    for column in config.query_required_fields:
        if column not in df.columns:
            df[column] = None
    df[ABSTRACT_COLUMN] = _concat_columns(df, config.abstract_concat, "")
    df[KEYWORD_COLUMN] = _concat_columns(df, config.keyword_concat, [])
    df[TITLE_EN_COLUMN] = df[config.title_en_field]
    df[ABSTRACT_EN_COLUMN] = _concat_columns(df, config.abstract_en_concat, "")
    df[KEYWORD_EN_COLUMN] = _concat_columns(df, config.keyword_en_concat, [])
    invalid_entries: List[str] = []
    df[ORGANISATION_COLUMN] = _transform_organisations(df, config, invalid_entries)
    return df.rename(columns=config.column_translations)[config.relevant_columns], invalid_entries


def _concat_columns(df: pd.DataFrame, columns: List[str], empty_cells_fill: Any) -> pd.Series:
    filled = [_fill_null_values(df[column], empty_cells_fill) for column in columns]
    if not filled:
        return pd.Series([empty_cells_fill] * len(df), index=df.index, dtype=object)
    result = filled[0]
    for series in filled[1:]:
        result = result + series
    return result


def _fill_null_values(series: pd.Series, empty_cells_fill: Any) -> pd.Series:
    return series.apply(
        lambda x: (
            x
            if x is not None and (not isinstance(x, float) or not np.isnan(x))
            else (list(empty_cells_fill) if isinstance(empty_cells_fill, list) else empty_cells_fill)
        )
    ).astype(object)


def _transform_organisations(df: pd.DataFrame, config: _FWFConfig, invalid_entries: List[str]) -> pd.Series:
    per_source = [
        _extract_by_regex(df[field], regex, data_id, additional_data, invalid_entries)
        for field, regex, data_id, additional_data in config.organisation_sources
    ]
    combined = per_source[0]
    for series in per_source[1:]:
        combined = combined + series
    return combined.apply(json.dumps)


def _extract_by_regex(
    series: pd.Series, regex: str, data_id: str, additional_data: dict, invalid_entries: List[str]
) -> pd.Series:
    matcher = re.compile(regex)

    def extract(value: Any) -> List[Dict[str, Any]]:
        if not isinstance(value, list):
            return []
        organisations = []
        for entry in value:
            match = matcher.match(entry) if isinstance(entry, str) else None
            if not match or not match.groupdict():
                invalid_entries.append(f'"{entry}" ({data_id})')
                continue
            organisations.append({**additional_data, **match.groupdict(), "data_id": data_id})
        return organisations

    return series.apply(extract).astype(object)
