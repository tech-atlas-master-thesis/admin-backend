from typing import Optional, Dict, Any, List

import pandas as pd

from pipelineFramework import (
    LocalisationString,
    StepConfig,
    UserStepConfig,
    PipelineDummy,
    StepDummy,
    StepUserConfig,
    EventType,
)
from pipelineFramework.server.common_steps.get_results_from_latest_pipeline import get_pipeline_results
from pipelineFramework.server.config.config import UserConfigEnumDto


class GetScraperResults(StepConfig):
    DEFAULT_PIPELINE_NAME = "Scraper Pipeline"
    DEFAULT_COMBINED_STEP_NAME = "combineScraperResults"
    DATA_SOURCE_COLUMN = "data_source"
    DATA_SOURCES = ["FFG", "FWF"]

    async def run(
        self,
        user_config: Optional[UserStepConfig] = None,
        results: Optional[Dict[str, Any]] = None,
        pipeline: PipelineDummy = None,
        step: StepDummy = None,
        **_,
    ):
        PIPELINE_NAME = user_config.get("PIPELINE_NAME")
        COMBINED_STEP = user_config.get("COMBINED_STEP")
        DATA_SOURCES = user_config.get("DATA_SOURCES") or []

        if not DATA_SOURCES:
            raise ValueError("No data source selected. Select at least one data source.")

        scraper_results = await get_pipeline_results(PIPELINE_NAME, COMBINED_STEP)
        if scraper_results is None:
            raise FileNotFoundError(
                f'No result returned for step "{COMBINED_STEP}" in pipeline "{PIPELINE_NAME}" found'
            )
        if not isinstance(scraper_results, pd.DataFrame):
            raise TypeError(f'Result of step "{COMBINED_STEP}" in pipeline "{PIPELINE_NAME}" is not a table')
        if self.DATA_SOURCE_COLUMN not in scraper_results.columns:
            raise KeyError(
                f'Result of step "{COMBINED_STEP}" in pipeline "{PIPELINE_NAME}" has no '
                f'"{self.DATA_SOURCE_COLUMN}" column'
            )

        filtered = scraper_results[scraper_results[self.DATA_SOURCE_COLUMN].isin(DATA_SOURCES)]
        counts = filtered[self.DATA_SOURCE_COLUMN].value_counts().to_dict()
        yield (
            f"Kept {len(filtered)} of {len(scraper_results)} projects for data sources "
            f"[{', '.join(DATA_SOURCES)}] ({counts})",
            EventType.INFO,
        )
        missing = [source for source in DATA_SOURCES if source not in counts]
        if missing:
            yield f"No projects found for data sources [{', '.join(missing)}]", EventType.WARNING
        yield filtered.reset_index(drop=True), EventType.RESULT

    @staticmethod
    def name() -> str:
        return "getScraperResults"

    @staticmethod
    def display_name() -> LocalisationString:
        return LocalisationString("Get results from Scraper Pipeline", "Ergebnisse der Scraper Pipeline laden")

    def description(self) -> Optional[LocalisationString]:
        return None

    def user_config(self) -> List[StepUserConfig]:
        return [
            StepUserConfig(
                "PIPELINE_NAME",
                LocalisationString("Scraper Pipeline Name", "Scraper Pipeline Name"),
                LocalisationString("", ""),
                StepUserConfig.StepUserConfigType.PIPELINE,
                self.DEFAULT_PIPELINE_NAME,
                pipelineType="scraper_main",
            ),
            StepUserConfig(
                "COMBINED_STEP",
                LocalisationString("Combined Scraper Results Step", "Step mit kombinierten Scraper Ergebnissen"),
                LocalisationString("", ""),
                StepUserConfig.StepUserConfigType.STEP,
                self.DEFAULT_COMBINED_STEP_NAME,
                pipelineType="scraper_main",
            ),
            StepUserConfig(
                "DATA_SOURCES",
                LocalisationString("Data Sources", "Datenquellen"),
                LocalisationString(
                    "Only projects from the selected data sources are used. At least one is required.",
                    "Nur Projekte aus den ausgewählten Datenquellen werden verwendet. Mindestens eine ist nötig.",
                ),
                StepUserConfig.StepUserConfigType.LIST,
                list(self.DATA_SOURCES),
                enumValues=[UserConfigEnumDto(source, source) for source in self.DATA_SOURCES],
            ),
        ]

    def dependencies(self) -> List[str]:
        return []
