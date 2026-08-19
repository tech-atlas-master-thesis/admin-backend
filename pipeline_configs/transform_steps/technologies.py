from copy import deepcopy
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
from pipeline_configs.transform_steps.scraper import GetTechnologyConfiguration


class TechnologiesStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        TECH_CONFIG = results.get("getTechnologyConfiguration")
        DATASET = results.get("create_dataset")
        if DATASET is None:
            raise FileNotFoundError("No dataset found")
        if TECH_CONFIG is None:
            raise FileNotFoundError("No organisation data found")
        yield "Data found", EventType.INFO

        field_db = get_fe_db_client().fields
        tech_db = get_fe_db_client().technologies

        techs_flat = [tech for field in TECH_CONFIG for tech in field["technologies"]]

        tech_ids = await tech_db.insert_many([{**item, "projects": 0, "dataset": DATASET} for item in techs_flat])
        tech_id_map = {tech["label"]: tech_id for tech, tech_id in zip(techs_flat, tech_ids.inserted_ids)}

        fields = deepcopy(TECH_CONFIG)

        for field in fields:
            field["technologies"] = [tech_id_map[tech["label"]] for tech in field["technologies"]]

        field_ids = await field_db.insert_many([{**item, "projects": 0, "dataset": DATASET} for item in fields])

        for field, field_id in zip(fields, field_ids.inserted_ids):
            await tech_db.update_many(
                {"dataset": DATASET, "_id": {"$in": field["technologies"]}}, {"$set": {"field": field_id}}
            )

        yield tech_id_map, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    @staticmethod
    def name() -> str:
        return "technologies"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Save Technologies to Database", "Technologies in Datenbank speichern")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return [GetTechnologyConfiguration.name(), CreateDataSetStep.name()]
