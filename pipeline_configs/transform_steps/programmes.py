from typing import Optional, Union, List, Dict, Any

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
    EventType,
)
from pipelineFramework import (
    get_fe_db_client,
)
from pipeline_configs.transform_steps.create_dataset import CreateDataSetStep
from pipeline_configs.transform_steps.grant_extract import GrantExtractStep
from pipeline_configs.transform_steps.scraper import GetTechnologyConfiguration


class ProgrammeExtractStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        GRANTS_DATA = results.get(GrantExtractStep.name())
        if GRANTS_DATA is None:
            raise FileNotFoundError("No grant data found")
        programmes = set()
        for grant in GRANTS_DATA:
            programmes.add(grant["programme"])
        yield f"Extracted {len(programmes)} unique programmes", EventType.INFO
        yield [{"name": programme, "projects": 0} for programme in programmes], EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    @staticmethod
    def name() -> str:
        return "programme_extract"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Extract Grant Programme Data", "Förderprogrammdaten extrahieren")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return [GrantExtractStep.name()]


class ProgrammeNormalizeStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        PROGRAMMES = results.get("programme_extract")
        if PROGRAMMES is None:
            raise FileNotFoundError("No organisation data found")
        yield "Data found", EventType.INFO
        yield PROGRAMMES, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    @staticmethod
    def name() -> str:
        return "programme_normalize"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Normalize Grant Programme Data", "Förderprogrammdaten normalisieren")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return [ProgrammeExtractStep.name()]


class ProgrammeEnrichStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        PROGRAMMES = results.get("programme_normalize")
        if PROGRAMMES is None:
            raise FileNotFoundError("No organisation data found")
        yield "Data found", EventType.INFO

        # TODO: add relevant technologies (fields) to programmes

        yield PROGRAMMES, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    @staticmethod
    def name() -> str:
        return "programme_enrich"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Enrich Grant Programme data", "Förderprogrammdaten anreichern")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return [ProgrammeNormalizeStep.name(), GetTechnologyConfiguration.name()]


class ProgrammesDatabaseStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        PROGRAMMES = results.get("programme_enrich")
        DATASET = results.get("create_dataset")
        if PROGRAMMES is None:
            raise FileNotFoundError("No programme data found")
        yield "Data found", EventType.INFO

        project_db = get_fe_db_client().programmes

        ids = await project_db.insert_many([{**item, "dataset": DATASET} for item in PROGRAMMES])
        yield dict(zip((programme["name"] for programme in PROGRAMMES), ids.inserted_ids)), EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    @staticmethod
    def name() -> str:
        return "programme_database"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Save Grant Programme Data to Database", "Förderprogramme in Datenbank speichern")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return [ProgrammeEnrichStep.name(), CreateDataSetStep.name()]


ProgrammeStepCollection = [
    ProgrammeExtractStep(),
    ProgrammeNormalizeStep(),
    ProgrammeEnrichStep(),
    ProgrammesDatabaseStep(),
]
