from typing import Optional, Union, List, Dict, Any

from bson import ObjectId

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
    EventType,
)
from pipelineFramework.server.db.helper import get_fe_db_client
from pipeline_configs.transform_steps.create_dataset import CreateDataSetStep
from pipeline_configs.transform_steps.organisations_enrich import OrganisationEnrichStep


class OrganisationDatabaseStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        ORGANISATIONS = results.get("organisation_enrich")
        DATASET = results.get("create_dataset")
        if ORGANISATIONS is None:
            raise FileNotFoundError("No organisation data found")
        yield "Data found", EventType.INFO

        project_db = get_fe_db_client().organisations

        ids = await project_db.insert_many([{**item, "dataset": DATASET} for item in ORGANISATIONS.values()])
        yield dict(zip(ORGANISATIONS.keys(), ids.inserted_ids)), EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    @staticmethod
    def name() -> str:
        return "organisation_database"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Save Organisation Data to Database", "Organisationen Daten in Datenbank speichern")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return [OrganisationEnrichStep.name(), CreateDataSetStep.name()]
