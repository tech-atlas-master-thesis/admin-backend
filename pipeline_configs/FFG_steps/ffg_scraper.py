from typing import Optional, List, Union, Dict, Any

from pipelineFramework import StepConfig, UserStepConfig, StepUserConfig, LocalisationString, LocalisationStringType
from scrapers.FFG import FFG


class FFPScraper(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]], **_):
        if user_config is None or results is None:
            raise FileNotFoundError("User Config or Results not provided")
        ffg = FFG(user_config, results)
        async for event in ffg.get_results():
            yield event

    def user_config(self) -> List[StepUserConfig]:
        return [
            StepUserConfig(
                "EXCEL_REQUEST_URI",
                LocalisationString("URI to request Excel file", "URI um Excel File zu beantragen"),
                None,
                StepUserConfig.StepUserConfigType.STRING,
                "https://projekte.ffg.at/projekt/excel",
            ),
            StepUserConfig(
                "SEARCH_REQUEST_URI",
                LocalisationString("Project Search Request URI", "Projektsuche URI"),
                None,
                StepUserConfig.StepUserConfigType.STRING,
                "https://projekte.ffg.at/projekt?advanced_search=1"
                "&go=1"
                "&q={query}"
                "&start={page}"
                "&titel={title}"
                "&projektbeginn={dateFrom}"
                "&projektende={dateUntil}"
                "&foerderung={program}"
                "&foerdernehmer={grant}"
                "&status={status}",
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
                "ID_HREF_REGEX",
                LocalisationString("RegExp to find href with ID", "RegExp um href mit ID zu finden"),
                None,
                StepUserConfig.StepUserConfigType.STRING,
                "/projekt/([a-z0-9-]+)",
            ),
            StepUserConfig(
                "FOUND_KEYWORD_COLUMN",
                LocalisationString("Column for used keyword", "Spalte für benutztes keyword"),
                None,
                StepUserConfig.StepUserConfigType.STRING,
                "keyTechnologies",
            ),
            StepUserConfig(
                "COLUMN_TRANSLATIONS",
                LocalisationString("Columns renaming", "Spalten Umbenennung"),
                None,
                StepUserConfig.StepUserConfigType.MAPPING,
                {
                    "Projekt-ID": "externalId",
                    "Kurztitel": "short",
                    "Langtitel": "title",
                    "Abstract": "abstract",
                    "Programm": "programme",
                    "Ausschreibung": "bidding",
                    "Projektstart": "start",
                    "Projektende": "end",
                    "Projektstatus": "status",
                    "Keywords": "keywords",
                    "Rolle im Projekt": "role_in_project",
                    "Organisationsname": "organisationName",
                    "Organisationsart": "organisationType",
                    "Website": "organisationWebsite",
                    "Staat": "organisationCountry",
                    "Bundesland": "organisationState",
                    "Stadt": "organisationCity",
                    "Adresse (Office)": "organisationStreet",
                },
            ),
            StepUserConfig(
                "PROJECT_COLUMNS",
                LocalisationString("Columns containing project data", "Spalten mit Projektdaten"),
                None,
                StepUserConfig.StepUserConfigType.LIST,
                [
                    "externalId",
                    "short",
                    "title",
                    "programme",
                    "bidding",
                    "start",
                    "end",
                    "status",
                    "keywords",
                ],
            ),
            StepUserConfig(
                "PROJECT_COLUMNS_ONLY_ON_FIRST_INSTANCE",
                LocalisationString(
                    "Columns containing project data only once", "Spalten mit Prjektdaten nur einmal vorhanden"
                ),
                None,
                StepUserConfig.StepUserConfigType.LIST,
                ["abstract"],
            ),
            StepUserConfig(
                "ORGANISATION_COLUMNS",
                LocalisationString("Columns with organisation data", "Zeilen mit Organisationsdaten"),
                None,
                StepUserConfig.StepUserConfigType.LIST,
                [
                    "role_in_project",
                    "organisationName",
                    "organisationType",
                    "organisationWebsite",
                    "organisationCountry",
                    "organisationState",
                    "organisationCity",
                    "organisationStreet",
                ],
            ),
            StepUserConfig(
                "ORGANISATIONS_COLUMN_NAME",
                LocalisationString("Organisations column", "Organistionsspalte"),
                None,
                StepUserConfig.StepUserConfigType.STRING,
                "organisations",
            ),
        ]

    def name(self) -> str:
        return "getDataFFG"

    def display_name(self) -> LocalisationStringType:
        return LocalisationString("Get Data from FFG", "Daten von FFG laden")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return ["getTechnologyConfiguration"]
