import datetime
from typing import Union, List

from bson import ObjectId

from pipelineFramework import (
    StepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
    EventType,
    Pipeline,
)
from pipelineFramework.server.dto import AuditInfoDto, UserDto
from pipelineFramework.server.db.helper import get_fe_db_client
from pipeline_configs.transform_steps.create_dataset import CreateDataSetStep
from pipeline_configs.transform_steps.grant_database import GrantDatabaseStep
from pipeline_configs.transform_steps.organisations_database import OrganisationDatabaseStep
from pipeline_configs.transform_steps.prefill_counts import PrefillProgrammeCounts
from pipeline_configs.transform_steps.project_database import ProjectDatabaseStep


class PublishDataSetStep(StepConfig):
    async def run(self, pipeline: Pipeline, results, **_):
        datasets = get_fe_db_client().get_collection("datasets")
        DATASET_ID: ObjectId = results.get(CreateDataSetStep.name())
        yield f"Publishing DataSet with ID {DATASET_ID}", EventType.INFO
        dataset = datasets.insert_one(
            {
                "_id": DATASET_ID,
                "pipelineType": pipeline.type,
                "pipeline": pipeline.id,
                "pipelineName": pipeline.name,
                "active": True,
                "created": AuditInfoDto(pipeline.created.by, datetime.datetime.now(datetime.UTC)).serialize(),
            }
        )
        yield f"DataSet {DATASET_ID} created", EventType.INFO
        yield dataset.inserted_id, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    @staticmethod
    def name() -> str:
        return "publish_dataset"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Publish DataSet", "Datenset veröffentlichen")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return [
            CreateDataSetStep.name(),
            PrefillProgrammeCounts.name(),
            ProjectDatabaseStep.name(),
            OrganisationDatabaseStep.name(),
            GrantDatabaseStep.name(),
        ]
