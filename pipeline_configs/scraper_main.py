from pipelineFramework import LocalisationString, PipelineConfig, GetConfiguration, get_dummy_step
from pipeline_configs.FFG_steps.ffg_scraper import FFPScraper
from pipeline_configs.FWF_steps.fwf_scraper import FWFScraper

SCRAPER_PIPELINE = PipelineConfig(
    type="scraper_main",
    display_name=LocalisationString("Scraper Pipeline", "Scraper Pipeline"),
    steps=[
        GetConfiguration(
            "technologies",
            "getTechnologyConfiguration",
            LocalisationString("Get Technology Configuration", "Technologie Konfiguration Laden"),
            LocalisationString("Desc", "Desc"),
            LocalisationString("Technology Configuration", "Technologie Konfiguration"),
        ),
        FFPScraper(),
        FWFScraper(),
    ],
    parallelize=True,
)
