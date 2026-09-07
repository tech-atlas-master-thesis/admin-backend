from pipelineFramework import LocalisationString, PipelineConfig, SnapshotSummaryStep
from pipeline_configs.registry_steps import (
    BevDownloadStep,
    BevLoadStep,
    GleifDownloadStep,
    GleifLoadStep,
    RorDownloadStep,
    RorLoadStep,
)

REGISTRY_PIPELINE = PipelineConfig(
    type="registry_main",
    display_name=LocalisationString("Registry Data", "Registerdaten"),
    description=LocalisationString(
        "Download and activate versioned snapshots of ROR, GLEIF and the BEV address register.",
        "Versionierte Snapshots von ROR, GLEIF und dem BEV-Adressregister herunterladen und aktivieren.",
    ),
    steps=[
        RorDownloadStep(),
        RorLoadStep(),
        GleifDownloadStep(),
        GleifLoadStep(),
        BevDownloadStep(),
        BevLoadStep(),
        SnapshotSummaryStep(depends_on=[RorLoadStep, GleifLoadStep, BevLoadStep]),
    ],
    parallelize=True,
)
