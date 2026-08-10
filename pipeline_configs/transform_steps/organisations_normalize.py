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
        if ORGANISATIONS is None:
            raise FileNotFoundError("No organisation data found")
        TYPE_MAPPING: Dict[str, str] = user_config.get("TYPE_MAPPING")
        yield "Data found", EventType.INFO

        organisations = self.deduplicate_organisations(ORGANISATIONS, warnings if warnings else [])
        self.map_type(organisations, TYPE_MAPPING)
        self.map_special_organisations(organisations, warnings if warnings else [])

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
                # TODO: merge orgs
                pass
        return unique_organisations

    def map_type(self, organisations: Dict[str, Dict[str, Any]], mapping: Dict[str, str]) -> None:
        for organisation in organisations.values():
            new_type = mapping.get(organisation["type"], None) if "type" in organisation else None
            if not new_type:
                new_type = "__SPECIAL_CONVERSION_NEEDED"
            organisation["type"] = new_type

    def map_special_organisations(self, organisations: Dict[str, Dict[str, Any]], warnings: List[Event]) -> None:
        # TODO: find good strategy to categorise FWF orgs
        for organisation in organisations.values():
            if organisation["type"] == "__SPECIAL_CONVERSION_NEEDED":
                warnings.append(
                    Event.now(
                        f"No mapping found for type {organisation['type']} in organisation {organisation['name']}",
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
                    "Nationale Forschungseinrichtung": "OTHER",
                    "assoziierte:r Forschungspartner:in": "OTHER",
                    "nationale:r Kooperationspartner:in": "OTHER",
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
        return [OrganisationExtractStep.name()]
