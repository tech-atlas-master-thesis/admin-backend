import re
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
from pipeline_configs.transform_steps.configs import GetOrganisationTypeMapperConfiguration
from pipeline_configs.transform_steps.organisations_extract import OrganisationExtractStep


class OrganisationNormalizeStep(StepConfig):
    async def run(
        self,
        user_config: Optional[UserStepConfig],
        results: Optional[Dict[str, Any]] = None,
        warnings: List[Event] = None,
        **_,
    ):
        if results is None:
            results = {}
        ORGANISATIONS: Dict[str, Dict[str, Any]] = results.get("organisation_extract")
        ORGANISATION_TYPE_MAPPER = results.get(GetOrganisationTypeMapperConfiguration.name())
        if ORGANISATIONS is None:
            raise FileNotFoundError("No organisation data found")
        TYPE_MAPPING: Dict[str, str] = user_config.get("TYPE_MAPPING")
        yield "Data found", EventType.INFO

        organisations = ORGANISATIONS.copy()

        self.map_type(organisations, TYPE_MAPPING)
        organisations = self.deduplicate_organisations(organisations, warnings if warnings else [])
        self.map_special_organisations(organisations, ORGANISATION_TYPE_MAPPER, warnings if warnings else [])

        yield organisations, EventType.RESULT

    def deduplicate_organisations(
        self, organisations: Dict[str, Dict[str, Any]], warnings: List[Event]
    ) -> Dict[str, Dict[str, Any]]:
        unique_organisations = {}
        for organisation in organisations.values():
            identifier = organisation["name"]
            if identifier not in unique_organisations:
                unique_organisations[identifier] = organisation
            else:
                unique_organisations[identifier] = self.merge_organisation(
                    unique_organisations[identifier], organisation
                )
        return unique_organisations

    def merge_organisation(self, org1: Dict[str, Any], org2: Dict[str, Any]) -> Dict[str, Any]:
        merged_org = org1.copy()
        for key, value in org2.items():
            if key not in merged_org or merged_org[key] is None:
                merged_org[key] = value
        if merged_org["type"] == "__SPECIAL_CONVERSION_NEEDED":
            merged_org["type"] = org2["type"]
        return merged_org

    def map_type(self, organisations: Dict[str, Dict[str, Any]], mapping: Dict[str, str]) -> None:
        for organisation in organisations.values():
            new_type = mapping.get(organisation["type"], None) if "type" in organisation else None
            if not new_type:
                new_type = "__SPECIAL_CONVERSION_NEEDED"
            organisation["type"] = new_type

    def map_special_organisations(
        self, organisations: Dict[str, Dict[str, Any]], organisation_mapper, warnings: List[Event]
    ) -> None:
        for organisation in organisations.values():
            current_org_type = organisation["type"]
            if current_org_type == "__SPECIAL_CONVERSION_NEEDED":
                for mapper in organisation_mapper:
                    if any(re.findall(regex, organisation["name"].lower()) for regex in mapper["keywords"]):
                        organisation["type"] = mapper["mapTo"]
                        break

                if organisation["type"] == "__SPECIAL_CONVERSION_NEEDED":
                    warnings.append(
                        Event.now(
                            f"No mapping found for organisation {organisation['name']}",
                            EventType.WARNING,
                        )
                    )
                    organisation["type"] = "OTHER"

    def user_config(self) -> List[StepUserConfig]:
        return [
            StepUserConfig(
                "TYPE_MAPPING",
                LocalisationString("Type Mapping", "Typ Zuordnung"),
                LocalisationString(
                    "Mapping of organisation type from dataSource to universal internal enum",
                    "Zuordnung von Organisationstyp aus Datenquelle zu universellem internen Enum",
                ),
                StepUserConfig.StepUserConfigType.MAPPING,
                {
                    "Außeruniversitäre Forschungseinrichtung": "RESEARCH_INSTITUTE",
                    "Bund, Länder, Gemeinden": "PUBLIC_INSTITUTION",
                    "Einzelforscher": "SINGLE_RESEARCHER",
                    "Fachhochschule": "FACHHOCHSCHULE",
                    "Gemeinnützige Organisation": "NON_PROFIT",
                    "Interessensvertretung": "LOBBY",
                    "Privatuniversität": "UNIVERSITY",
                    "Sonstige": "OTHER",
                    "Universität": "UNIVERSITY",
                    "unternehmerisch tätig": "COMPANY",
                    "Projektpartner:in": "OTHER",
                },
            ),
        ]

    @staticmethod
    def name() -> str:
        return "organisation_normalize"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Normalize Organisation Data", "Organisationen normalisieren")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return [OrganisationExtractStep.name(), GetOrganisationTypeMapperConfiguration.name()]
