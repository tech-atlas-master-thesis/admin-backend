from typing import Optional, Union, List, Dict, Any

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
    EventType,
    get_fe_db_client,
)
from pipeline_configs.transform_steps.create_dataset import CreateDataSetStep
from pipeline_configs.transform_steps.grant_enrich import GrantEnrichStep


class GrantDatabaseStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        GRANTS = results.get("grant_enrich")
        DATASET = results.get("create_dataset")
        if GRANTS is None:
            raise FileNotFoundError("No grant data found")
        yield "Data found", EventType.INFO

        project_db = get_fe_db_client().grants

        ids = await project_db.insert_many([{**item, "dataset": DATASET} for item in GRANTS])
        yield dict(zip((grant["name"] for grant in GRANTS), ids.inserted_ids)), EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    @staticmethod
    def name() -> str:
        return "grant_database"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Save Grant Data to Database", "Förderdaten in Datenbank speichern")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return [GrantEnrichStep.name(), CreateDataSetStep.name()]
