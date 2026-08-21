from config_definitions.organisation_type_map_config import ORGANISATION_TYPE_MAP_CONFIG
from pipelineFramework import LocalisationString, GetConfiguration

GetOrganisationTypeMapperConfiguration = GetConfiguration(
    ORGANISATION_TYPE_MAP_CONFIG.type,
    "getOrganisationTypeMapperConfiguration",
    LocalisationString("Get Organisation Type Mapper Configuration", "Organisationstyp Mapper Konfiguration Laden"),
    None,
    LocalisationString("Get Organisation Type Mapper Configuration", "Organisationstyp Mapper Konfiguration Laden"),
)
