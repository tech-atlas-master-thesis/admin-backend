from typing import Optional, Union, List, Dict, Any

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
    EventType,
)
from pipeline_configs.transform_steps.grant_extract import GrantExtractStep


class GrantNormalizeStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        GRANTS = results.get("grant_extract")
        if GRANTS is None:
            raise FileNotFoundError("No organisation data found")
        yield "Data found", EventType.INFO
        yield GRANTS, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    @staticmethod
    def name() -> str:
        return "grant_normalize"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Normalize Grant Data", "Förderdaten normalisieren")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return [GrantExtractStep.name()]
