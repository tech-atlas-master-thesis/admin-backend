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
from pipelineFramework.server.db.helper import get_fe_db_client


class ProjectDatabaseStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        PROJECTS: pd.DataFrame = results.get("project_enrich")
        DATASET = results.get("create_dataset")
        if PROJECTS is None:
            raise FileNotFoundError("No organisation data found")
        yield "Data found", EventType.INFO

        project_db = get_fe_db_client().get_collection("projects")

        project_db.insert_many([{**item, "dataset": DATASET} for item in PROJECTS])

    def user_config(self) -> List[StepUserConfig]:
        return []

    @staticmethod
    def name() -> str:
        return "project_database"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Save Project Data to Database", "Projekt Daten in Datenbank speichern")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return ["project_enrich", "create_dataset"]
