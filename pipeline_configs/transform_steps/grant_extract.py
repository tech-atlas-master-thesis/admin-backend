import json
import math
from typing import Optional, Union, List, Dict, Any

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
    EventType,
)
from pipeline_configs.transform_steps.projects_filter import ProjectsFilterStep


class GrantExtractStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        PROJECTS: List[Dict[str, Any]] = results.get(ProjectsFilterStep.name())
        if PROJECTS is None:
            raise FileNotFoundError("No grant data found")
        grants = {}
        for grant_data in PROJECTS:
            # TODO: fix for FWF data ("grant")
            new_grant = None
            if (
                "data_source" in grant_data
                and grant_data["data_source"] == "FFG"
                and grant_data.get("bidding") is not None
                and grant_data.get("programme") is not None
            ):
                new_grant = {
                    "name": grant_data["bidding"],
                    "programme": grant_data["programme"],
                    "projects": 0,
                }
            if grant_data.get("data_source") == "FWF" and grant_data.get("grant") is not None:
                new_grant = {
                    "name": grant_data["grant"],
                    "programme": grant_data["grant"],
                    "projects": 0,
                }
            if new_grant:
                if any(new_grant == grant for grant in grants):
                    continue
                grants[new_grant["name"]] = new_grant
        yield f"Extracted {len(grants)} unique grants", EventType.INFO
        yield grants, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    @staticmethod
    def name() -> str:
        return "grant_extract"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Extract Grant Data", "Förderdaten extrahieren")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return [ProjectsFilterStep.name()]
