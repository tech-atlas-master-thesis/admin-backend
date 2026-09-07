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
from registries import ror
from user_agent import USER_AGENT


class RorDownloadStep(StreamingDownloadStep):
    source = ror.SOURCE
    user_agent = USER_AGENT

    async def resolve_remote_file(
        self, session: aiohttp.ClientSession, user_config: Optional[UserStepConfig]
    ) -> RemoteFile:
        api = str((user_config or {}).get("ZENODO_API") or ror.ZENODO_API).rstrip("/")
        record_id = str((user_config or {}).get("ZENODO_RECORD") or ror.ZENODO_CONCEPT_RECORD)

        async with session.get(f"{api}/records/{record_id}") as response:
            response.raise_for_status()
            record = await response.json(content_type=None)

        latest = (record.get("links") or {}).get("latest")
        if latest:
            async with session.get(latest) as response:
                response.raise_for_status()
                record = await response.json(content_type=None)

        version, url = ror.parse_zenodo_release(record)
        return RemoteFile(url=url, version=version, filename=f"ror-{version}.zip")

    def user_config(self) -> List[StepUserConfig]:
        return [
            *super().user_config(),
            StepUserConfig(
                "ZENODO_API",
                LocalisationString("Zenodo API", "Zenodo-API"),
                LocalisationString("Base URL of the Zenodo REST API.", "Basis-URL der Zenodo-REST-API."),
                StepUserConfig.StepUserConfigType.STRING,
                ror.ZENODO_API,
                required=False,
            ),
            StepUserConfig(
                "ZENODO_RECORD",
                LocalisationString("Zenodo record", "Zenodo-Record"),
                LocalisationString("Concept record id of the ROR data dump.", "Concept-Record-ID des ROR-Dumps."),
                StepUserConfig.StepUserConfigType.STRING,
                ror.ZENODO_CONCEPT_RECORD,
                required=False,
            ),
        ]

    @staticmethod
    def name() -> str:
        return "ror_download"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Download ROR release", "ROR-Release herunterladen")

    @staticmethod
    def description() -> Optional[LocalisationStringType]:
        return LocalisationString(
            "Fetch the newest ROR release from Zenodo. Skips the download when the release version has not moved.",
            "Neuestes ROR-Release von Zenodo laden. Überspringt den Download, wenn die Version unverändert ist.",
        )


class RorLoadStep(SnapshotLoadStep):
    source = ror.SOURCE
    download_step = RorDownloadStep

    def iter_documents(self, path: Path) -> Iterator[Dict[str, Any]]:
        return ror.iter_documents(path)

    def indexes(self) -> List[IndexModel]:
        return ror.indexes()

    @staticmethod
    def name() -> str:
        return "ror_load"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Load ROR snapshot", "ROR-Snapshot laden")

    @staticmethod
    def description() -> Optional[LocalisationStringType]:
        return LocalisationString(
            "Keep the Austrian organisations and index their names and aliases for offline matching.",
            "Österreichische Organisationen behalten und ihre Namen und Aliasse für den Offline-Abgleich indizieren.",
        )
