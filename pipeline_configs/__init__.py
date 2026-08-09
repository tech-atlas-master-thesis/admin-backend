from typing import List

from pipelineFramework import PipelineConfig
from .test import TEST_PIPELINE
from .scraper_main import DEMO_PIPELINE

PIPELINE_CONFIGS: List[PipelineConfig] = [TEST_PIPELINE, DEMO_PIPELINE]
