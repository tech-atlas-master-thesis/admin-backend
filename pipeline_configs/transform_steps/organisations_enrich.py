from typing import Optional, Union, List, Dict, Any

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
    EventType,
)
from pipeline_configs.transform_steps.organisations_normalize import OrganisationNormalizeStep


class OrganisationEnrichStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        ORGANISATIONS = results.get("organisation_normalize")
        if ORGANISATIONS is None:
            raise FileNotFoundError("No organisation data found")
        yield "Data found", EventType.INFO
        yield ORGANISATIONS, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    @staticmethod
    def name() -> str:
        return "organisation_enrich"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Enrich Organisation Data", "Organisationen mit Daten anreichern")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return [OrganisationNormalizeStep.name()]
