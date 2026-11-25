from config_definitions.organisation_type_map_config import ORGANISATION_TYPE_MAP_CONFIG
from config_definitions.technologies_config import TECHNOLOGIES_CONFIG
from pipelineFramework import LocalisationString, GetConfiguration

GetOrganisationTypeMapperConfiguration = GetConfiguration(
    ORGANISATION_TYPE_MAP_CONFIG.type,
    "getOrganisationTypeMapperConfiguration",
    LocalisationString("Get Organisation Type Mapper Configuration", "Organisationstyp Mapper Konfiguration Laden"),
    None,
    LocalisationString("Get Organisation Type Mapper Configuration", "Organisationstyp Mapper Konfiguration Laden"),
)

GetTechnologyConfiguration = GetConfiguration(
    TECHNOLOGIES_CONFIG.type,
    "getTechnologyConfiguration",
    LocalisationString("Get Technology Configuration", "Technologie Konfiguration Laden"),
    None,
    LocalisationString("Technology Configuration", "Technologie Konfiguration"),
)
