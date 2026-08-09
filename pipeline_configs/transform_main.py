from pipelineFramework import LocalisationString, PipelineConfig
from pipeline_configs.transform_steps.create_dataset import CreateDataSetStep
from pipeline_configs.transform_steps.grant_database import GrantDatabaseStep
from pipeline_configs.transform_steps.grant_enrich import GrantEnrichStep
from pipeline_configs.transform_steps.grant_extract import GrantExtractStep
from pipeline_configs.transform_steps.grant_normalize import GrantNormalizeStep
from pipeline_configs.transform_steps.organisations_database import OrganisationDatabaseStep
from pipeline_configs.transform_steps.organisations_enrich import OrganisationEnrichStep
from pipeline_configs.transform_steps.organisations_extract import OrganisationExtractStep
from pipeline_configs.transform_steps.organisations_normalize import OrganisationNormalizeStep
from pipeline_configs.transform_steps.prefill_counts import (
    PrefillStepCollection,
)
from pipeline_configs.transform_steps.programmes import (
    ProgrammeStepCollection,
)
from pipeline_configs.transform_steps.project_database import ProjectDatabaseStep
from pipeline_configs.transform_steps.project_enrich import ProjectEnrichStep
from pipeline_configs.transform_steps.project_extract import ProjectExtractStep
from pipeline_configs.transform_steps.project_normalize import ProjectNormalizeStep
from pipeline_configs.transform_steps.publish_dataset import PublishDataSetStep
from pipeline_configs.transform_steps.scraper import GetScraperResults, GetTechnologyConfiguration
from pipeline_configs.transform_steps.technologies import TechnologiesStep

TRANSFORMER_PIPELINE = PipelineConfig(
    type="transform_main",
    display_name=LocalisationString("Transformer Pipeline", "Transformer Pipeline"),
    steps=[
        CreateDataSetStep(),
        GetScraperResults(),
        GetTechnologyConfiguration,
        TechnologiesStep(),
        OrganisationExtractStep(),
        OrganisationNormalizeStep(),
        OrganisationEnrichStep(),
        OrganisationDatabaseStep(),
        GrantExtractStep(),
        *ProgrammeStepCollection,
        GrantNormalizeStep(),
        GrantEnrichStep(),
        GrantDatabaseStep(),
        ProjectExtractStep(),
        ProjectNormalizeStep(),
        ProjectEnrichStep(),
        ProjectDatabaseStep(),
        *PrefillStepCollection,
        PublishDataSetStep(),
    ],
    parallelize=True,
)
