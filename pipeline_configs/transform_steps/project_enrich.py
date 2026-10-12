import json
from typing import Optional, Union, List, Dict, Any

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
    EventType,
    Event,
)
from pipeline_configs.transform_steps.grant_database import GrantDatabaseStep
from pipeline_configs.transform_steps.organisations_database import OrganisationDatabaseStep
from pipeline_configs.transform_steps.project_extract import ProjectExtractStep
from pipeline_configs.transform_steps.project_normalize import ProjectNormalizeStep


class ProjectEnrichStep(StepConfig):
    async def run(
        self,
        user_config: Optional[UserStepConfig],
        results: Optional[Dict[str, Any]] = None,
        warnings: Optional[List[Event]] = None,
        **_,
    ):
        if results is None:
            results = {}
        PROJECTS: List[Dict[str, Any]] = results.get(ProjectExtractStep.name())
        TECHNOLOGIES: Dict[str, List[str, str]] = results.get("technologies")
        if PROJECTS is None:
            raise FileNotFoundError("No scraper data found")
        ORGANISATION_MAPPING: Dict[str, str] = results.get(OrganisationDatabaseStep.name())
        GRANT_MAPPING: Dict[str, str] = results.get(GrantDatabaseStep.name())
        yield "Data found", EventType.INFO

        self.add_technology_ids(PROJECTS, TECHNOLOGIES)
        self.add_organisations(PROJECTS, ORGANISATION_MAPPING, warnings if warnings is not None else [])
        self.add_grants(PROJECTS, GRANT_MAPPING)

        yield PROJECTS, EventType.RESULT

    def add_organisations(
        self, projects: List[Dict[str, Any]], organisation_mapping: Dict[str, str], warnings: List[Event]
    ):
        for project in projects:
            orgs = json.loads(project["organisations"])
            project["organisations"] = [organisation_mapping.get(org["organisationName"]) for org in orgs]
            project_leaders = [
                organisation_mapping.get(org["organisationName"])
                for org in orgs
                if "role_in_project" in org
                and org["role_in_project"] in ["Konsortialführer", "Einzelantragsteller", "LEADER"]
            ]
            if not project_leaders:
                warnings.append(
                    Event.now(
                        f"No project leader found for project {project['title']} ({project['externalId']} from {project['data_source']})",
                        EventType.WARNING,
                    )
                )
            project["projectLeader"] = project_leaders[0] if len(project_leaders) == 1 else project_leaders

    def add_technology_ids(self, projects: List[Dict[str, Any]], technologies: Dict[str, List[str, str]]) -> None:
        for project in projects:
            tech_ids = []
            for tech in json.loads(project["keyTechnologies"]):
                tech_id = technologies[tech]
                tech_ids.append(tech_id)
            project["keyTechnologies"] = tech_ids

    def add_grants(self, projects: List[Dict[str, Any]], grants: Dict[str, str]) -> None:
        for project in projects:
            project["grant"] = grants.get(project["grant"])

    def user_config(self) -> List[StepUserConfig]:
        return []

    @staticmethod
    def name() -> str:
        return "project_enrich"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Enrich Project Data", "Projekt Daten Anreichern")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return [ProjectNormalizeStep.name()]
