from typing import Optional, Union, List, Dict, Any

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
    EventType,
)
from pipeline_configs.transform_steps.grant_normalize import GrantNormalizeStep
from pipeline_configs.transform_steps.programmes import ProgrammesDatabaseStep


class GrantEnrichStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        GRANTS = results.get(GrantNormalizeStep.name())
        PROGRAMMES = results.get(ProgrammesDatabaseStep.name())
        if GRANTS is None:
            raise FileNotFoundError("No organisation data found")
        yield "Data found", EventType.INFO

        for grant in GRANTS:
            grant["programme"] = PROGRAMMES.get(grant["programme"])

        yield GRANTS, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    @staticmethod
    def name() -> str:
        return "grant_enrich"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Enrich Grant data", "Förderdaten anreichern")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return [GrantNormalizeStep.name(), ProgrammesDatabaseStep.name()]
