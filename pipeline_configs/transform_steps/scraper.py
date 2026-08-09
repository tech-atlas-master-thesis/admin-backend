from typing import Optional, Dict, Any, List

import pandas as pd

from pipelineFramework import (
    GetResultFromLatestPipeline,
    LocalisationString,
    StepConfig,
    UserStepConfig,
    PipelineDummy,
    StepDummy,
    StepUserConfig,
    EventType,
)
from pipelineFramework.server.common_steps.get_results_from_latest_pipeline import get_pipeline_results

GetTechnologyConfiguration = GetResultFromLatestPipeline(
    "getTechnologyConfiguration",
    LocalisationString("Get Technology Configuration", "Technologie Konfiguration Laden"),
    None,
    "Scraper Pipeline",
    "getTechnologyConfiguration",
)


class GetScraperResults(StepConfig):
    DEFAULT_PIPELINE_NAME = "Scraper Pipeline"
    DEFAULT_FFG_STEP_NAME = "getDataFFG"
    DEFAULT_FWF_STEP_NAME = "getDataFWF"

    async def run(
        self,
        user_config: Optional[UserStepConfig] = None,
        results: Optional[Dict[str, Any]] = None,
        pipeline: PipelineDummy = None,
        step: StepDummy = None,
        **_,
    ):
        PIPELINE_NAME = user_config.get("PIPELINE_NAME")
        FFG_STEP = user_config.get("FFG_STEP")
        FWF_STEP = user_config.get("FWF_STEP")

        ffg_results = (await get_pipeline_results(PIPELINE_NAME, FFG_STEP)) if FFG_STEP else pd.DataFrame()
        fwf_results = (await get_pipeline_results(PIPELINE_NAME, FWF_STEP)) if FWF_STEP else pd.DataFrame()
        if ffg_results is None:
            raise FileNotFoundError(f'No result returned for step "{FFG_STEP}" in pipeline "{PIPELINE_NAME}" found')
        if fwf_results is None:
            raise FileNotFoundError(f'No result returned for step "{FWF_STEP}" in pipeline "{PIPELINE_NAME}" found')
        yield pd.concat([fwf_results, ffg_results]), EventType.RESULT

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
                "FFG_STEP",
                LocalisationString("FFG Scraper Pipeline Step", "FFG Scraper Pipeline Step"),
                LocalisationString("", ""),
                StepUserConfig.StepUserConfigType.STEP,
                self.DEFAULT_FFG_STEP_NAME,
                pipelineType="scraper_main",
                required=False,
            ),
            StepUserConfig(
                "FWF_STEP",
                LocalisationString("FWF Scraper Pipeline Step", "FWF Scraper Pipeline Step"),
                LocalisationString("", ""),
                StepUserConfig.StepUserConfigType.STEP,
                self.DEFAULT_FWF_STEP_NAME,
                pipelineType="scraper_main",
                required=False,
            ),
        ]

    def dependencies(self) -> List[str]:
        return []
