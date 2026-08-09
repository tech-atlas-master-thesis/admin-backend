from typing import List

from pipelineFramework import PipelineConfig
from .test import TEST_PIPELINE
from .scraper_main import SCRAPER_PIPELINE
from .transform_main import TRANSFORMER_PIPELINE

PIPELINE_CONFIGS: List[PipelineConfig] = [TEST_PIPELINE, SCRAPER_PIPELINE, TRANSFORMER_PIPELINE]
