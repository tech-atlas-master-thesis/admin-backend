from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import aiohttp
from pymongo import IndexModel

from pipelineFramework import (
    LocalisationString,
    LocalisationStringType,
    RemoteFile,
    SnapshotLoadStep,
    StepUserConfig,
    StreamingDownloadStep,
    UserStepConfig,
)
from registries import gleif
from user_agent import USER_AGENT


class GleifDownloadStep(StreamingDownloadStep):
    source = gleif.SOURCE
    user_agent = USER_AGENT

    async def resolve_remote_file(
        self, session: aiohttp.ClientSession, user_config: Optional[UserStepConfig]
    ) -> RemoteFile:
        api = str((user_config or {}).get("GOLDEN_COPY_API") or gleif.GOLDEN_COPY_API)
        async with session.get(api) as response:
            response.raise_for_status()
            payload = await response.json(content_type=None)

        version, url = gleif.parse_golden_copy_publish(payload)
        return RemoteFile(url=url, version=version, filename=f"gleif-lei2-{version}.zip")

    def user_config(self) -> List[StepUserConfig]:
        return [
            *super().user_config(),
            StepUserConfig(
                "GOLDEN_COPY_API",
                LocalisationString("Golden copy API", "Golden-Copy-API"),
                LocalisationString(
                    "GLEIF endpoint listing the concatenated file publishes.",
                    "GLEIF-Endpunkt mit den Veröffentlichungen der konkatenierten Dateien.",
                ),
                StepUserConfig.StepUserConfigType.STRING,
                gleif.GOLDEN_COPY_API,
                required=False,
            ),
        ]

    @staticmethod
    def name() -> str:
        return "gleif_download"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Download GLEIF LEI data", "GLEIF-LEI-Daten herunterladen")

    @staticmethod
    def description() -> Optional[LocalisationStringType]:
        return LocalisationString(
            "Fetch the newest Level 1 concatenated file. The publish date is the version, so a same-day re-run is free.",
            "Neueste Level-1-Datei laden. Das Publish-Datum ist die Version, ein erneuter Lauf am selben Tag ist gratis.",
        )


class GleifLoadStep(SnapshotLoadStep):
    source = gleif.SOURCE
    download_step = GleifDownloadStep

    def iter_documents(self, path: Path) -> Iterator[Dict[str, Any]]:
        return gleif.iter_documents(path)

    def indexes(self) -> List[IndexModel]:
        return gleif.indexes()

    @staticmethod
    def name() -> str:
        return "gleif_load"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Load GLEIF snapshot", "GLEIF-Snapshot laden")

    @staticmethod
    def description() -> Optional[LocalisationStringType]:
        return LocalisationString(
            "Keep entities whose legal address is in Austria, with their Firmenbuchnummer and street address.",
            "Einheiten mit Rechtssitz in Österreich behalten, samt Firmenbuchnummer und Adresse.",
        )
