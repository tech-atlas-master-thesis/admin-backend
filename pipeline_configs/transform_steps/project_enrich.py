import json
from typing import Optional, Union, List, Dict, Any

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
    EventType,
)


class ProjectEnrichStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        PROJECTS = results.get("project_normalize")
        if PROJECTS is None:
            raise FileNotFoundError("No scraper data found")
        yield "Data found", EventType.INFO

        yield PROJECTS, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    @staticmethod
    def name() -> str:
        return "project_enrich"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Enrich Project Data", "Projekt Daten Anreichern")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return ["project_normalize"]
