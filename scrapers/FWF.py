import datetime
import itertools
import json
import re
from typing import List, Dict, Any, Optional, Tuple

import aiohttp
import numpy as np
import pandas as pd
from aiohttp import BasicAuth
from pandas import StringDtype
from six import StringIO

from pipelineFramework import UserStepConfig
from scrapers.Scraper import Scraper


class FWF(Scraper):
    def __init__(self, user_config: UserStepConfig, results: Dict[str, Any]):
        super().__init__()

        self.warnings = []

        self.TECHNOLOGIES = results.get("getTechnologyConfiguration")

        self.PROJECT_SEARCH_ENDPOINT = user_config.get("PROJECT_SEARCH_ENDPOINT")
        self.PROJECT_SEARCH_ENDPOINT_HEADERS = user_config.get("PROJECT_SEARCH_ENDPOINT_HEADERS")
        self.PROJECT_SEARCH_ITEMS_FIELD = user_config.get("PROJECT_SEARCH_ITEMS_FIELD")
        self.PROJECT_SEARCH_MATCHING_STRATEGY = user_config.get("PROJECT_SEARCH_MATCHING_STRATEGY")
        self.PROJECT_SEARCH_LOCALES = user_config.get("PROJECT_SEARCH_LOCALES")
        self.SEARCH_DATE_FROM = user_config.get("SEARCH_DATE_FROM")
        self.SEARCH_DATE_UNTIL = user_config.get("SEARCH_DATE_UNTIL")
        self.FOUND_KEYWORD_COLUMN = "keyTechnologies"
        self.COLUMN_TRANSLATIONS = user_config.get("COLUMN_TRANSLATIONS")
        self.KEYWORD_CONCAT = user_config.get("KEYWORD_CONCAT")
        self.KEYWORD_COLUMN = "keywords"
        self.ABSTRACT_CONCAT = user_config.get("ABSTRACT_CONCAT")
        self.ABSTRACT_COLUMN = "abstract"
        self.ORGANIZATION_COLUMN = "organisations"
        self.ORGANIZATION_PROJECT_LEADER = user_config.get("ORGANIZATION_PROJECT_LEADER")
        self.ORGANISATION_RESEARCH_INSTITUTIONS = user_config.get("ORGANISATION_RESEARCH_INSTITUTIONS")
        self.ORGANISATION_RESEARCH_INSTITUTIONS_ADDITIONAL_DATA = user_config.get(
            "ORGANISATION_RESEARCH_INSTITUTIONS_ADDITIONAL_DATA"
        )
        self.ORGANISATION_RESEARCH_INSTITUTIONS_REGEX = user_config.get("ORGANISATION_RESEARCH_INSTITUTIONS_REGEX")
        self.ORGANISATION_RESEARCH_INSTITUTIONS_IDENTIFIER = "fwf_research_institution"
        self.ORGANISATION_NATIONAL_PARTNERS = user_config.get("ORGANISATION_NATIONAL_PARTNERS")
        self.ORGANISATION_NATIONAL_PARTNERS_ADDITIONAL_DATA = user_config.get(
            "ORGANISATION_NATIONAL_PARTNERS_ADDITIONAL_DATA"
        )
        self.ORGANISATION_NATIONAL_PARTNERS_REGEX = user_config.get("ORGANISATION_NATIONAL_PARTNERS_REGEX")
        self.ORGANISATION_NATIONAL_PARTNERS_IDENTIFIER = "fwf_national_partner"
        self.ORGANISATION_INTERNATIONAL_PARTNERS = user_config.get("ORGANISATION_INTERNATIONAL_PARTNERS")
        self.ORGANISATION_INTERNATIONAL_PARTNERS_ADDITIONAL_DATA = user_config.get(
            "ORGANISATION_INTERNATIONAL_PARTNERS_ADDITIONAL_DATA"
        )
        self.ORGANISATION_INTERNATIONAL_PARTNERS_REGEX = user_config.get("ORGANISATION_INTERNATIONAL_PARTNERS_REGEX")
        self.ORGANISATION_INTERNATIONAL_PARTNERS_IDENTIFIER = "fwf_international_partner"
        self.OUTPUT_REGEX_WARNINGS = user_config.get("OUTPUT_REGEX_WARNINGS")

        self.RELEVANT_COLUMNS = [
            *self.COLUMN_TRANSLATIONS.values(),
            self.ABSTRACT_COLUMN,
            self.KEYWORD_COLUMN,
            self.ORGANIZATION_COLUMN,
            self.ORGANIZATION_PROJECT_LEADER,
        ]

        self.QUERY_REQUIRED_FIELDS = [
            *self.COLUMN_TRANSLATIONS.keys(),
            *self.ABSTRACT_CONCAT,
            *self.KEYWORD_CONCAT,
            self.ORGANIZATION_PROJECT_LEADER,
            self.ORGANISATION_RESEARCH_INSTITUTIONS,
            self.ORGANISATION_NATIONAL_PARTNERS,
            self.ORGANISATION_INTERNATIONAL_PARTNERS,
        ]

    async def get_results_for_keyword(self, keyword: super().Keyword) -> Tuple[pd.DataFrame | None, List[str]]:
        self.warnings = []
        async with aiohttp.ClientSession() as session:
            df = pd.read_json(await self._get_query_results(session, keyword), orient="records")
        df = self._add_missing_columns(df)
        df = self._transform_dataframe(df)
        df[self.FOUND_KEYWORD_COLUMN] = keyword.name
        return df, self.warnings

    async def _get_query_results(self, session: aiohttp.ClientSession, keyword: super().Keyword) -> StringIO | None:
        async with session.get(
            self.PROJECT_SEARCH_ENDPOINT,
            json={},
            params={
                "limit": 999999,
                "attributesToRetrieve": ",".join(self.QUERY_REQUIRED_FIELDS),
                "matchingStrategy": self.PROJECT_SEARCH_MATCHING_STRATEGY,
                "locales": ",".join(self.PROJECT_SEARCH_LOCALES),
                "showRankingScoreDetails": "true",
                "filter": self._get_search_filters(),
                "q": self.get_query_string(keyword),
            },
            headers={**self.PROJECT_SEARCH_ENDPOINT_HEADERS, "Content-Type": "application/json"},
        ) as response:
            if not response.ok:
                raise response.raise_for_status()
            hits = json.loads(await response.content.read())[self.PROJECT_SEARCH_ITEMS_FIELD]
            return StringIO(json.dumps(hits))

    def get_query_string(self, keyword: super().Keyword) -> str:
        any_of = " ".join(keyword.any_of)
        excluded = " ".join(f"-{term}" for term in keyword.exclude)
        return f"{any_of} {excluded}"

    def _add_missing_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        for column in self.QUERY_REQUIRED_FIELDS:
            if column not in df.columns:
                df[column] = None
        return df

    def _transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        df[self.ABSTRACT_COLUMN] = self._concat_columns(df, self.ABSTRACT_CONCAT, "")
        df[self.KEYWORD_COLUMN] = self._concat_columns(df, self.KEYWORD_CONCAT, [])
        df[self.ORGANIZATION_COLUMN] = self._transform_organisations(df)
        return df.rename(columns=self.COLUMN_TRANSLATIONS)[self.RELEVANT_COLUMNS]

    def _concat_columns(self, df: pd.DataFrame, columns: List[str], empty_cells_fill: Any) -> pd.Series:
        for column in columns:
            df[column] = self._fill_null_values(df[column], empty_cells_fill)
        return df[columns].sum(axis=1)

    def _fill_null_values(self, series: pd.Series, empty_cells_fill: Any) -> pd.Series:
        if series.dtypes.type == str:
            return series.fillna(empty_cells_fill)
        return series.apply(
            lambda x: x if x is not None and (not isinstance(x, float) or not np.isnan(x)) else empty_cells_fill
        )

    def _transform_organisations(self, df: pd.DataFrame) -> pd.Series:
        return (
            pd.concat(
                [
                    self._extract_by_regex(
                        df[self.ORGANISATION_RESEARCH_INSTITUTIONS],
                        self.ORGANISATION_RESEARCH_INSTITUTIONS_REGEX,
                        self.ORGANISATION_RESEARCH_INSTITUTIONS_IDENTIFIER,
                        self.ORGANISATION_RESEARCH_INSTITUTIONS_ADDITIONAL_DATA,
                    ),
                    self._extract_by_regex(
                        df[self.ORGANISATION_NATIONAL_PARTNERS],
                        self.ORGANISATION_NATIONAL_PARTNERS_REGEX,
                        self.ORGANISATION_NATIONAL_PARTNERS_IDENTIFIER,
                        self.ORGANISATION_NATIONAL_PARTNERS_ADDITIONAL_DATA,
                    ),
                    self._extract_by_regex(
                        df[self.ORGANISATION_INTERNATIONAL_PARTNERS],
                        self.ORGANISATION_INTERNATIONAL_PARTNERS_REGEX,
                        self.ORGANISATION_INTERNATIONAL_PARTNERS_IDENTIFIER,
                        self.ORGANISATION_INTERNATIONAL_PARTNERS_ADDITIONAL_DATA,
                    ),
                ],
                axis=1,
            )
            .sum(axis=1)
            .apply(lambda x: json.dumps(x))
        )

    def _extract_by_regex(self, series: pd.Series, regex: str, data_id: str, additional_data: dict) -> pd.Series:
        matcher = re.compile(regex)
        return series.apply(
            lambda x: (
                [
                    {**additional_data, **matches, "data_id": data_id}
                    for entry in x
                    if (matches := self._get_extract_matches(matcher, entry)) is not None
                ]
                if isinstance(x, list)
                else []
            )
        )

    def _get_extract_matches(self, matcher: re.Pattern[str], entry: str) -> Optional[dict]:
        result = matcher.match(entry)
        if not result or not result.groupdict():
            if self.OUTPUT_REGEX_WARNINGS:
                self.warnings.append(f'"{entry}" is not a valid regex for {matcher.pattern}')
            return None
        return result.groupdict()

    def _get_search_filters(self) -> str:
        return ""

    async def aggregate_dataframes(self, dataframes: List[pd.DataFrame]) -> pd.DataFrame:
        return (
            pd.concat(dataframes)
            .groupby("externalId", as_index=False)
            .agg(
                {
                    self.FOUND_KEYWORD_COLUMN: lambda x: json.dumps(x.to_list()),
                    **dict.fromkeys(self.RELEVANT_COLUMNS, "first"),
                }
            )
        )

    def get_keywords(self) -> List[super().Keyword]:
        return [
            super().Keyword.keyword_from_config(tech)
            for field in self.TECHNOLOGIES
            for tech in field.get("technologies")
        ]

    def get_data_identifier(self) -> str:
        return "FWF"

    def get_search_datetime_format(self, dt: datetime.datetime | str | None) -> str:
        if dt is None:
            return ""
        dt = datetime.datetime.fromisoformat(dt) if isinstance(dt, str) else dt
        return dt.strftime("%d.%m.%Y")
