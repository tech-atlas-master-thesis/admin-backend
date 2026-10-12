import json
from typing import Optional, Union, List, Dict, Any

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
    EventType,
    Event,
)
from pipeline_configs.transform_steps.grant_database import GrantDatabaseStep
from pipeline_configs.transform_steps.organisations_database import OrganisationDatabaseStep
from pipeline_configs.transform_steps.project_extract import ProjectExtractStep


class ProjectNormalizeStep(StepConfig):
    async def run(
        self,
        user_config: Optional[UserStepConfig],
        results: Optional[Dict[str, Any]] = None,
        warnings: Optional[List[Event]] = None,
        **_,
    ):
        if results is None:
            results = {}
        PROJECTS: List[Dict[str, Any]] = results.get("project_extract")
        if PROJECTS is None:
            raise FileNotFoundError("No scraper data found")
        STATUS_MAPPING: Dict[str, str] = user_config.get("STATUS_MAPPING")
        yield "Data found", EventType.INFO

        self.parse_keywords(PROJECTS)
        self.map_status(PROJECTS, STATUS_MAPPING, warnings if warnings is not None else [])
        self.normalize_grant(PROJECTS)

        yield PROJECTS, EventType.RESULT

    def map_status(self, projects: List[Dict[str, Any]], mapping: Dict[str, str], warnings: List[Event]) -> None:
        for project in projects:
            new_type = mapping.get(project["status"].lower(), None)
            if not new_type:
                warnings.append(
                    Event.now(
                        f"No mapping found for status {project['status']} in project {project['title']}",
                        EventType.WARNING,
                    )
                )
            project["status"] = new_type

    def parse_keywords(self, projects: List[Dict[str, Any]]) -> None:
        for project in projects:
            project["keywords"] = project["keywords"].split(", ")

    def normalize_grant(self, projects: List[Dict[str, Any]]) -> None:
        for project in projects:
            if "bidding" in project:
                project["grant"] = project["bidding"]
                del project["bidding"]
            if "programme" in project:
                del project["programme"]

    def user_config(self) -> List[StepUserConfig]:
        return [
            StepUserConfig(
                "STATUS_MAPPING",
                LocalisationString("Status Mapping", "Status Zuordnung"),
                LocalisationString(
                    "Mapping of status from dataSource to universal internal enum",
                    "Zuordnung von Status aus Datenquelle zu universellem internen Enum",
                ),
                StepUserConfig.StepUserConfigType.MAPPING,
                {
                    "abgeschlossen": "ENDED",
                    "laufend": "ONGOING",
                    "ended": "ENDED",
                    "ongoing": "ONGOING",
                    "pending": "PENDING",
                    "prematurely terminated": "TERMINATED",
                    "starting date pending": "PENDING",
                    "withdrawn": "TERMINATED",
                },
            ),
        ]

    @staticmethod
    def name() -> str:
        return "project_normalize"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Normalize Project Data", "Projekt Daten Normalisieren")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return [ProjectExtractStep.name(), OrganisationDatabaseStep.name(), GrantDatabaseStep.name()]
