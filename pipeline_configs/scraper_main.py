from pipelineFramework import LocalisationString, PipelineConfig
from pipeline_configs.scraper_steps import CombineScraperResults, FetchFFG, FetchFWF

SCRAPER_PIPELINE = PipelineConfig(
    type="scraper_main",
    display_name=LocalisationString("Scraper Pipeline", "Scraper Pipeline"),
    steps=[
        FetchFFG(),
        FetchFWF(),
        CombineScraperResults(),
    ],
    parallelize=True,
)
