import datetime
import math
from typing import Optional, Union, List, Dict, Any

import pandas as pd

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
    EventType,
)
from pipeline_configs.transform_steps.scraper import GetScraperResults


class ProjectExtractStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        SCRAPER_DATA: pd.DataFrame = results.get("getScraperResults")
        if SCRAPER_DATA is None or not isinstance(SCRAPER_DATA, pd.DataFrame):
            raise FileNotFoundError("No scraper data found")
        yield "Data found", EventType.INFO

        START_DATE_FROM = (
            pd.to_datetime(user_config.get("START_DATE_FROM"), format="ISO8601")
            if user_config.get("START_DATE_FROM")
            else None
        )
        START_DATE_UNTIL = (
            pd.to_datetime(user_config.get("START_DATE_UNTIL"), format="ISO8601")
            if user_config.get("START_DATE_UNTIL")
            else None
        )
        END_DATE_FROM = (
            pd.to_datetime(user_config.get("END_DATE_FROM"), format="ISO8601")
            if user_config.get("END_DATE_FROM")
            else None
        )
        END_DATE_UNTIL = (
            pd.to_datetime(user_config.get("END_DATE_UNTIL"), format="ISO8601")
            if user_config.get("END_DATE_UNTIL")
            else None
        )

        RELEVANT_COLUMNS = [
            "externalId",
            "uri",
            "short",
            "title",
            "abstract",
            "bidding",
            "programme",
            "start",
            "end",
            "status",
            "keywords",
            "keyTechnologies",
            "organisations",
            "data_source",
        ]

        for column in RELEVANT_COLUMNS:
            if column not in SCRAPER_DATA.columns:
                SCRAPER_DATA[column] = None

        # TODO: convert this in the scraper pipeline
        SCRAPER_DATA["start"] = self._convert_to_date(SCRAPER_DATA["start"])
        SCRAPER_DATA["end"] = self._convert_to_date(SCRAPER_DATA["end"])
        projects = SCRAPER_DATA[RELEVANT_COLUMNS].to_dict("records")
        yield [
            self.map(project, RELEVANT_COLUMNS)
            for project in projects
            if self._is_in_date_range(project, START_DATE_FROM, START_DATE_UNTIL, END_DATE_FROM, END_DATE_UNTIL)
        ], EventType.RESULT

    def map(self, project: Dict[str, Any], columns: List[str]) -> Dict[str, Any]:
        new_project = {}
        for column in columns:
            value = project[column]
            new_project[column] = value if not isinstance(value, float) or not math.isnan(value) else None
        return new_project

    def _convert_to_date(self, series: pd.Series) -> pd.Series:
        return pd.to_datetime(series, format="mixed", utc=True).replace({pd.NaT: None})

    def _is_in_date_range(
        self,
        project: Dict[str, Any],
        start_date_from: datetime.datetime,
        start_date_until: datetime.datetime,
        end_date_from: datetime.datetime,
        end_date_until: datetime.datetime,
    ) -> bool:
        if (start_date_from or start_date_until) and ("start" not in project or not project["start"]):
            return False
        if (end_date_from or end_date_until) and ("end" not in project or not project["end"]):
            return False

        if start_date_from and project["start"] < start_date_from:
            return False
        if start_date_until and project["start"] > start_date_until:
            return False
        if end_date_from and project["end"] < end_date_from:
            return False
        if end_date_until and project["end"] > end_date_until:
            return False

        return True

    def user_config(self) -> List[StepUserConfig]:
        return [
            StepUserConfig(
                "START_DATE_FROM",
                LocalisationString("Project Start Date From", "Projektbeginn von"),
                None,
                StepUserConfig.StepUserConfigType.DATE,
                None,
                required=False,
                format="dd.mm.yy",
            ),
            StepUserConfig(
                "START_DATE_UNTIL",
                LocalisationString("Project Start Date Until", "Projektstart bis"),
                None,
                StepUserConfig.StepUserConfigType.DATE,
                None,
                required=False,
                format="dd.mm.yy",
            ),
            StepUserConfig(
                "END_DATE_FROM",
                LocalisationString("Project End Date From", "Projektende von"),
                None,
                StepUserConfig.StepUserConfigType.DATE,
                None,
                required=False,
                format="dd.mm.yy",
            ),
            StepUserConfig(
                "END_DATE_UNTIL",
                LocalisationString("Project End Date Until", "Projektende bis"),
                None,
                StepUserConfig.StepUserConfigType.DATE,
                None,
                required=False,
                format="dd.mm.yy",
            ),
        ]

    @staticmethod
    def name() -> str:
        return "project_extract"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Extract Project Data", "Projekt Daten Extrahieren")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return [GetScraperResults.name()]
