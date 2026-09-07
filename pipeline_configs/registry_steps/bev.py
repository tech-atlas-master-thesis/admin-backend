from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from pymongo import IndexModel

from pipelineFramework import (
    LocalisationString,
    LocalisationStringType,
    SnapshotLoadStep,
    StaticUrlDownloadStep,
)
from registries import bev
from user_agent import USER_AGENT


class BevDownloadStep(StaticUrlDownloadStep):
    source = bev.SOURCE
    url = bev.DEFAULT_URL
    user_agent = USER_AGENT

    @staticmethod
    def name() -> str:
        return "bev_download"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Download BEV Adressregister", "BEV-Adressregister herunterladen")

    @staticmethod
    def description() -> Optional[LocalisationStringType]:
        return LocalisationString(
            "Fetch the Austrian address register. The version comes from the server's ETag or Last-Modified header.",
            "Österreichisches Adressregister laden. Die Version stammt aus ETag oder Last-Modified des Servers.",
        )


class BevLoadStep(SnapshotLoadStep):
    source = bev.SOURCE
    download_step = BevDownloadStep

    def iter_documents(self, path: Path) -> Iterator[Dict[str, Any]]:
        return bev.iter_documents(path)

    def indexes(self) -> List[IndexModel]:
        return bev.indexes()

    @staticmethod
    def name() -> str:
        return "bev_load"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Load BEV addresses", "BEV-Adressen laden")

    @staticmethod
    def description() -> Optional[LocalisationStringType]:
        return LocalisationString(
            "Join the relational tables and reproject Gauss-Krueger coordinates to WGS84.",
            "Relationale Tabellen verknüpfen und Gauß-Krüger-Koordinaten nach WGS84 umprojizieren.",
        )
