from typing import Optional, List, Union, Dict, Any

from pipelineFramework import StepConfig, UserStepConfig, StepUserConfig, LocalisationString, LocalisationStringType
from pipelineFramework.server.config.config import UserConfigEnumDto
from scrapers.FWF import FWF


class FWFScraper(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]], **_):
        if user_config is None or results is None:
            raise FileNotFoundError("User Config or Results not provided")
        fwf = FWF(user_config, results)
        async for event in fwf.get_results():
            yield event

    def user_config(self) -> List[StepUserConfig]:
        return [
            StepUserConfig(
                "MULTI_SEARCH_ENDPOINT",
                LocalisationString("Multi Search URI", "Multi-Suche URI"),
                LocalisationString(
                    "URI of the Meilisearch multi-search endpoint. All search terms of a technology are sent "
                    "as one federated request.",
                    "URI des Meilisearch Multi-Search Endpunktes. Alle Suchbegriffe einer Technologie werden "
                    "als eine föderierte Anfrage gesendet.",
                ),
                StepUserConfig.StepUserConfigType.STRING,
                "https://openapi.fwf.ac.at/multi-search",
            ),
            StepUserConfig(
                "PROJECT_SEARCH_INDEX",
                LocalisationString("Project Search Index", "Projektsuche Index"),
                LocalisationString("Meilisearch index uid to search", "Meilisearch Index-UID der Suche"),
                StepUserConfig.StepUserConfigType.STRING,
                "projects",
            ),
            StepUserConfig(
                "PROJECT_SEARCH_ENDPOINT_HEADERS",
                LocalisationString("Project Search Endpoint Headers", "Projektsuche Endpunkt Header"),
                LocalisationString(
                    "Additional Headers required by the project search endpoint, f.e.: Auth",
                    "Zusätzliche Header für den Projektsuche Endpunkt benötigt, z.B.: Auth",
                ),
                StepUserConfig.StepUserConfigType.MAPPING,
                {
                    "Authorization": "Bearer 3a03f2f39cc8a99ea0775270adb4946c425469aa7f291e7ca9f2d8424337c1af",
                    "Content-Type": "application/json",
                },
            ),
            StepUserConfig(
                "PROJECT_SEARCH_ITEMS_FIELD",
                LocalisationString("Query result field", "Queryresultatfeld"),
                LocalisationString(
                    "JSON field for the query result items", "JSON Feld für die Resultatelemente der Query"
                ),
                StepUserConfig.StepUserConfigType.STRING,
                "hits",
            ),
            StepUserConfig(
                "PROJECT_SEARCH_MATCHING_STRATEGY",
                LocalisationString("Query Matching Strategy", "Query Matching Strategie"),
                LocalisationString(
                    "How Meilisearch handles a query whose terms cannot all be matched. Each query now holds a "
                    "single quoted phrase, which is mandatory in any case, so 'all' keeps results deterministic.",
                    "Wie Meilisearch mit einer Query umgeht, deren Begriffe nicht alle gefunden werden. Jede Query "
                    "enthält nur noch eine Phrase in Anführungszeichen, die ohnehin verpflichtend ist, daher hält "
                    "'all' die Ergebnisse deterministisch.",
                ),
                StepUserConfig.StepUserConfigType.STRING,
                "all",
                enumValues=[
                    UserConfigEnumDto(
                        "all", "all", LocalisationString("Every term must match", "Alle Begriffe müssen passen")
                    ),
                    UserConfigEnumDto(
                        "last",
                        "last",
                        LocalisationString(
                            "Drop terms from the end until results are found",
                            "Begriffe vom Ende her weglassen, bis Ergebnisse gefunden werden",
                        ),
                    ),
                    UserConfigEnumDto(
                        "frequency",
                        "frequency",
                        LocalisationString(
                            "Drop the most common terms first", "Die häufigsten Begriffe zuerst weglassen"
                        ),
                    ),
                ],
            ),
            StepUserConfig(
                "PROJECT_SEARCH_LOCALES",
                LocalisationString("Project Search Locales", "Projectsuche Sprache"),
                LocalisationString(
                    "Overrides Meilisearch's language auto-detection. FWF stores German and English side by side, "
                    "so restricting this to one language applies the wrong tokenizer to the other.",
                    "Überschreibt die automatische Spracherkennung von Meilisearch. FWF speichert Deutsch und "
                    "Englisch nebeneinander, eine Einschränkung auf eine Sprache wendet daher den falschen "
                    "Tokenizer auf die andere an.",
                ),
                StepUserConfig.StepUserConfigType.LIST,
                ["de", "en"],
                enumValues=[
                    UserConfigEnumDto("en", "en"),
                    UserConfigEnumDto("de", "de"),
                ],
            ),
            StepUserConfig(
                "PROJECT_SEARCH_ATTRIBUTES_TO_SEARCH_ON",
                LocalisationString("Searched Attributes", "Durchsuchte Attribute"),
                LocalisationString(
                    "Restrict matching to these fields. Empty means every searchable attribute of the index.",
                    "Sucht nur in diesen Feldern. Leer bedeutet alle durchsuchbaren Attribute des Index.",
                ),
                StepUserConfig.StepUserConfigType.LIST,
                [],
                required=False,
            ),
            StepUserConfig(
                "PROJECT_SEARCH_PAGE_SIZE",
                LocalisationString("Result Page Size", "Ergebnis-Seitengröße"),
                LocalisationString(
                    "Documents fetched per federated request while paginating",
                    "Dokumente pro föderierter Anfrage beim Paginieren",
                ),
                StepUserConfig.StepUserConfigType.INTEGER,
                1000,
            ),
            StepUserConfig(
                "PROJECT_SEARCH_MAX_TOTAL_HITS",
                LocalisationString("Maximum Total Hits", "Maximale Gesamttreffer"),
                LocalisationString(
                    "Max amount of projects to return per technology.",
                    "Maximale Nummer an Projekten die pro Technologie zurückgegeben werden.",
                ),
                StepUserConfig.StepUserConfigType.INTEGER,
                10000,
                required=False,
            ),
            StepUserConfig(
                "SEARCH_DATE_FROM",
                LocalisationString("Search Project Start Date From", "Suche Projektbeginn von"),
                None,
                StepUserConfig.StepUserConfigType.DATE,
                None,
                required=False,
                format="dd.mm.yy",
            ),
            StepUserConfig(
                "SEARCH_DATE_UNTIL",
                LocalisationString("Search Project End Date Until", "Suche Projektende bis"),
                None,
                StepUserConfig.StepUserConfigType.DATE,
                None,
                required=False,
                format="dd.mm.yy",
            ),
            StepUserConfig(
                "COLUMN_TRANSLATIONS",
                LocalisationString("Columns renaming", "Spalten Umbenennung"),
                None,
                StepUserConfig.StepUserConfigType.MAPPING,
                {
                    "id": "externalId",
                    "_str.url": "uri",
                    "_str.projecttitle.de": "title",
                    "_str.grantdoi": "grant",
                    "_date.startdate": "start",
                    "_date.enddate": "end",
                    "_str.status.en": "status",
                    "_long.approvedamount": "funding_amount",
                },
            ),
            StepUserConfig(
                "KEYWORD_CONCAT",
                LocalisationString("Keyword fields", "Keyword Felder"),
                LocalisationString(
                    "Field to concatenate to generate the keywords column",
                    "Zu konkatenierende Felder um die keyword Spalte zu erstellen",
                ),
                StepUserConfig.StepUserConfigType.LIST,
                [
                    "_list.researchareas.de",
                    "_list.researchfields.de",
                    "_list.researchdisciplines.de",
                    "_list.keywords.split",
                ],
            ),
            StepUserConfig(
                "ABSTRACT_CONCAT",
                LocalisationString("Abstract fields", "Abstract Felder"),
                LocalisationString(
                    "Field to concatenate to generate the abstract column",
                    "Zu konkatenierende Felder um die Abstract Spalte zu erstellen",
                ),
                StepUserConfig.StepUserConfigType.LIST,
                ["_str.prproposalsummary.de", "_str.prfinalreport.de"],
            ),
            StepUserConfig(
                "ORGANIZATION_PROJECT_LEADER",
                LocalisationString("Project Leader Field", "Projektleiterfeld"),
                LocalisationString(
                    "Field containing the project leader",
                    "Feld das den Projektleiter beinhaltet",
                ),
                StepUserConfig.StepUserConfigType.STRING,
                "_list.principalinvestigator.researchinstitute.name",
            ),
            StepUserConfig(
                "ORGANISATION_RESEARCH_INSTITUTIONS",
                LocalisationString("Research institutes Field", "Forschungsinstitutefeld"),
                LocalisationString(
                    "Field containing the research institutions",
                    "Feld das die Forschungsinstitute beinhaltet",
                ),
                StepUserConfig.StepUserConfigType.STRING,
                "_list.researchinstitutes",
            ),
            StepUserConfig(
                "ORGANISATION_RESEARCH_INSTITUTIONS_ADDITIONAL_DATA",
                LocalisationString("Research Institutions Additional Data", "Forschungsinstitute Zusatzdate"),
                LocalisationString(
                    "Additional data to be added to the research institutions",
                    "Zusatzdaten die Forschungsinstituten hinzugefügt werden soll",
                ),
                StepUserConfig.StepUserConfigType.MAPPING,
                {
                    "role_in_project": "LEADER",
                    "organisationCountry": "Österreich",
                    "organisationType": "Nationale Forschungseinrichtung",
                },
            ),
            StepUserConfig(
                "ORGANISATION_RESEARCH_INSTITUTIONS_REGEX",
                LocalisationString("Research institutes Regex Extractor", "Forschungsinstitute Regex Extraktor"),
                LocalisationString(
                    "Regex for extracting research institutions data",
                    "Regex das die Daten der Forschungsinstitute extrahiert",
                ),
                StepUserConfig.StepUserConfigType.STRING,
                "^(?P<organisationName>[A-Za-zŽžÀ-ÿ0-9 .\-,()]+)( \(https:\/\/ror\.org\/(?P<organisationRor>[a-z0-9]+)\))?( - [0-9]+\%)?$",
            ),
            StepUserConfig(
                "ORGANISATION_NATIONAL_PARTNERS",
                LocalisationString("National Partners Field", "Nationale Partner Feld"),
                LocalisationString(
                    "Field containing the national project partners",
                    "Feld das die nationalen Projektpartner beinhaltet",
                ),
                StepUserConfig.StepUserConfigType.STRING,
                "_list.nationalprojectparticipants.de",
            ),
            StepUserConfig(
                "ORGANISATION_NATIONAL_PARTNERS_ADDITIONAL_DATA",
                LocalisationString("National Partners Additional Data", "Nationale Partner Zusatzdate"),
                LocalisationString(
                    "Additional data to be added to the national project partners",
                    "Zusatzdaten die nationalen Projektpartnern hinzugefügt werden soll",
                ),
                StepUserConfig.StepUserConfigType.MAPPING,
                {"organisationCountry": "Österreich"},
            ),
            StepUserConfig(
                "ORGANISATION_NATIONAL_PARTNERS_REGEX",
                LocalisationString("National Partners Regex Extractor", "Nationale Partner Regex Extraktor"),
                LocalisationString(
                    "Regex for extracting national partners data",
                    "Regex das die Daten der nationalen Partner extrahiert",
                ),
                StepUserConfig.StepUserConfigType.STRING,
                "^([A-Za-zŽžÀ-ÿ .\-]+, )(?P<organisationName>[A-Za-zŽžÀ-ÿ0-9 .\-,()]+)( \(https:\/\/ror\.org\/(?P<organisationRor>[a-z0-9]+)\))?, (?P<organisationType>[A-Za-zŽžÀ-ÿ: ]+)",
            ),
            StepUserConfig(
                "ORGANISATION_INTERNATIONAL_PARTNERS",
                LocalisationString("International Partners Field", "Internationale Partner Feld"),
                LocalisationString(
                    "Field containing the international project partners",
                    "Feld das die internationalen Projektpartner beinhaltet",
                ),
                StepUserConfig.StepUserConfigType.STRING,
                "_list.internationalprojectparticipants.de",
            ),
            StepUserConfig(
                "ORGANISATION_INTERNATIONAL_PARTNERS_ADDITIONAL_DATA",
                LocalisationString("International Partners Additional Data", "Internationale Partner Zusatzdate"),
                LocalisationString(
                    "Additional data to be added to the international project partners",
                    "Zusatzdaten die internationalen Projektpartnern hinzugefügt werden soll",
                ),
                StepUserConfig.StepUserConfigType.MAPPING,
                {},
            ),
            StepUserConfig(
                "ORGANISATION_INTERNATIONAL_PARTNERS_REGEX",
                LocalisationString("International Partners Regex Extractor", "Internationale Partner Regex Extraktor"),
                LocalisationString(
                    "Regex for extracting international project data",
                    "Regex das die Daten der internationalen Projektpartner extrahiert",
                ),
                StepUserConfig.StepUserConfigType.STRING,
                "^([A-Za-zŽžÀ-ÿ .\-]+, )(?P<organisationName>[A-Za-zŽžÀ-ÿ0-9 .\-,()]+)( \(https:\/\/ror\.org\/(?P<organisationRor>[a-z0-9]+)\))? - (?P<organisationCountry>[A-Za-zŽžÀ-ÿ: ]+)(, (?P<organisationType>[A-Za-zŽžÀ-ÿ: ]+))?",
            ),
            StepUserConfig(
                "OUTPUT_REGEX_WARNINGS",
                LocalisationString("Regex Warnings", "Regex Warnungen"),
                LocalisationString(
                    "Toggle if regex warnings should be output",
                    "Umschalten, ob Regex-Warnungen ausgegeben werden sollen",
                ),
                StepUserConfig.StepUserConfigType.BOOLEAN,
                True,
            ),
        ]

    def name(self) -> str:
        return "getDataFWF"

    def display_name(self) -> LocalisationStringType:
        return LocalisationString("Get Data from FWF", "Daten von FWF laden")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return ["getTechnologyConfiguration"]
