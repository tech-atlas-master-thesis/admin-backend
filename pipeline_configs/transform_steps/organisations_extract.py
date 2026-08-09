import json
import math
from typing import Optional, Union, List, Dict, Any

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
    EventType,
)
from pipeline_configs.transform_steps.scraper import GetScraperResults


def get_non_null_value(collection: dict, key: str):
    if key not in collection:
        return None
    value = collection[key]
    if isinstance(value, float) and math.isnan(value):
        return None
    return value


class OrganisationExtractStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        SCRAPER_DATA = results.get("getScraperResults")
        if SCRAPER_DATA is None:
            raise FileNotFoundError("No organisation data found")
        organisations = {}
        available_fields = set()
        for collection in SCRAPER_DATA["organisations"]:
            for organisation in json.loads(collection):
                org_name = get_non_null_value(organisation, "organisationName")
                if not org_name:
                    yield f"Organisation {str(organisation)} has no organisation name. Will be skipped", EventType.WARNING
                    continue
                for key in organisation.keys():
                    available_fields.add(key)
                organisation = {
                    "name": get_non_null_value(organisation, "organisationName"),
                    "type": get_non_null_value(organisation, "organisationType"),
                    "website": get_non_null_value(organisation, "organisationWebsite"),
                    "ror": get_non_null_value(organisation, "organisationRor"),
                    "firmenbuchnummer": None,
                    "data_id": get_non_null_value(organisation, "data_id"),
                    "address": {
                        "country": get_non_null_value(organisation, "organisationCountry"),
                        "state": get_non_null_value(organisation, "organisationState"),
                        "city": get_non_null_value(organisation, "organisationCity"),
                        "street": get_non_null_value(organisation, "organisationStreet"),
                    },
                }
                if org_name in organisations:
                    organisation = {**organisations[org_name], **organisation}
                organisations[org_name] = organisation
        yield f"Extracted {len(organisations)} unique organisations", EventType.INFO
        yield organisations, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return []

    @staticmethod
    def name() -> str:
        return "organisation_extract"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Extract Organisation Data", "Organisationen extrahieren")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return [GetScraperResults.name()]
