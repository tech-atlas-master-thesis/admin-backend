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


class CreateDataSetStep(StepConfig):
    async def run(self, pipeline: Pipeline, **_):
        yield f"Creating new ID for data for Pipeline {str(pipeline)}", EventType.INFO
        data_set_id = ObjectId()
        yield f"DataSet ID {data_set_id} created", EventType.INFO
        yield data_set_id, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    @staticmethod
    def name() -> str:
        return "create_dataset"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Create DataSet", "Datenset erstellen")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return []
