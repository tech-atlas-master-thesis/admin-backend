from collections import defaultdict
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
from pipeline_configs.transform_steps.grant_database import GrantDatabaseStep
from pipeline_configs.transform_steps.programmes import ProgrammesDatabaseStep
from pipeline_configs.transform_steps.project_enrich import ProjectEnrichStep
from pipeline_configs.transform_steps.scraper import GetTechnologyConfiguration


class PrefillTechnologyCounts(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        PROJECTS: List[Dict[str, Any]] = results.get(ProjectEnrichStep.name())
        if PROJECTS is None:
            raise FileNotFoundError("No scraper data found")
        yield "Data found", EventType.INFO

        tech_counts = defaultdict(lambda: 0)
        for project in PROJECTS:
            for tech in project["keyTechnologies"]:
                tech_counts[tech] += 1

        tech_db = get_fe_db_client().get_collection("technologies")

        for tech_id, count in tech_counts.items():
            tech_db.update_one({"_id": tech_id}, {"$set": {"projects": count}})

        yield {str(tech_id): tech_count for tech_id, tech_count in tech_counts.items()}, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    @staticmethod
    def name() -> str:
        return "prefill_technology_counts"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Prefill Technology Counts", "Technologie Zählungen vorausfüllen")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return [ProjectEnrichStep.name(), GetTechnologyConfiguration.name()]


class PrefillFieldCounts(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        DATASET = results.get(CreateDataSetStep.name())
        if DATASET is None:
            raise FileNotFoundError("No dataset found")
        yield "Data found", EventType.INFO

        techs = get_fe_db_client().get_collection("technologies").find({"dataset": DATASET})

        fields_count = defaultdict(lambda: 0)
        for tech in techs:
            if "field" not in tech:
                yield f"Not field found for Technology {tech['label']}. Maybe the Technology has been added twice?", EventType.WARNING
                continue
            field = tech["field"]
            projects = tech["projects"]
            if projects is not None:
                fields_count[field] += projects

        field_db = get_fe_db_client().get_collection("fields")

        for field_id, count in fields_count.items():
            field_db.update_one({"_id": field_id}, {"$set": {"projects": count}})

        yield {str(field_id): field_count for field_id, field_count in fields_count.items()}, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    @staticmethod
    def name() -> str:
        return "prefill_field_counts"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Prefill Technology Field Counts", "Technologiefelder Zählungen vorausfüllen")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return [PrefillTechnologyCounts.name(), GetTechnologyConfiguration.name()]


class PrefillGrantCounts(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        PROJECTS: List[Dict[str, Any]] = results.get(ProjectEnrichStep.name())
        if PROJECTS is None:
            raise FileNotFoundError("No scraper data found")
        yield "Data found", EventType.INFO

        grant_count = defaultdict(lambda: 0)
        for project in PROJECTS:
            grant = project["grant"]
            if grant is not None:
                grant_count[grant] += 1

        grant_db = get_fe_db_client().get_collection("grants")

        for grant_id, count in grant_count.items():
            grant_db.update_one({"_id": grant_id}, {"$set": {"projects": count}})

        yield {str(grant_id): grant_count for grant_id, grant_count in grant_count.items()}, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    @staticmethod
    def name() -> str:
        return "prefill_grant_counts"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Prefill Grants Counts", "Technologie Förderungen vorausfüllen")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return [ProjectEnrichStep.name(), GrantDatabaseStep.name()]


class PrefillProgrammeCounts(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        DATASET = results.get(CreateDataSetStep.name())
        if DATASET is None:
            raise FileNotFoundError("No dataset found")
        yield "Data found", EventType.INFO

        grants = get_fe_db_client().get_collection("grants").find({"dataset": DATASET})

        programmes_count = defaultdict(lambda: 0)
        for grant in grants:
            programme = grant["programme"]
            projects = grant["projects"]
            if projects is not None:
                programmes_count[programme] += projects

        programme_db = get_fe_db_client().get_collection("programmes")

        for programme_id, count in programmes_count.items():
            programme_db.update_one({"_id": programme_id}, {"$set": {"projects": count}})

        yield {
            str(programme_id): programme_count for programme_id, programme_count in programmes_count.items()
        }, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    @staticmethod
    def name() -> str:
        return "prefill_programme_counts"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Prefill Grant Programme Counts", "Förderprogramme Zählungen vorausfüllen")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return [PrefillGrantCounts.name(), ProgrammesDatabaseStep.name()]


PrefillStepCollection = [
    PrefillTechnologyCounts(),
    PrefillFieldCounts(),
    PrefillGrantCounts(),
    PrefillProgrammeCounts(),
]
