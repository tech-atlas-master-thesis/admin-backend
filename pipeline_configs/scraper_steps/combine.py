from typing import Optional, List, Union, Dict, Any

import pandas as pd

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    StepUserConfig,
    LocalisationString,
    LocalisationStringType,
    EventType,
)
from pipeline_configs.scraper_steps.ffg_fetch import FetchFFG
from pipeline_configs.scraper_steps.fwf_fetch import FetchFWF

DATA_SOURCE_COLUMN = "data_source"
ID_COLUMN = "externalId"
LIST_TEXT_COLUMNS = ["keywords", "keywords_en"]
LIST_SEPARATOR = ", "

DEFAULT_COLUMNS = [
    "externalId",
    "uri",
    "short",
    "title",
    "abstract",
    "bidding",
    "programme",
    "grant",
    "start",
    "end",
    "status",
    "funding_amount",
    "keywords",
    "organisations",
    "data_source",
    "title_en",
    "abstract_en",
    "keywords_en",
]


class CombineScraperResults(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        columns: List[str] = (user_config or {}).get("COLUMNS") or DEFAULT_COLUMNS
        sources = {"FFG": results.get(FetchFFG.name()), "FWF": results.get(FetchFWF.name())}
        missing = [source for source, frame in sources.items() if not isinstance(frame, pd.DataFrame)]
        if missing:
            raise FileNotFoundError(f"No scraper data found for {', '.join(missing)}")

        combined = combine_frames(sources, columns)
        counts = combined[DATA_SOURCE_COLUMN].value_counts().to_dict()
        yield f"Combined {len(combined)} projects ({counts})", EventType.INFO
        yield combined, EventType.RESULT

    def user_config(self) -> List[StepUserConfig]:
        return [
            StepUserConfig(
                "COLUMNS",
                LocalisationString("Output columns", "Ausgabespalten"),
                LocalisationString(
                    "Columns of the combined file, in this order. Missing columns are left empty.",
                    "Spalten der kombinierten Datei in dieser Reihenfolge. Fehlende Spalten bleiben leer.",
                ),
                StepUserConfig.StepUserConfigType.LIST,
                list(DEFAULT_COLUMNS),
            ),
        ]

    @staticmethod
    def name() -> str:
        return "combineScraperResults"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Combine Scraper Results", "Scraper Ergebnisse kombinieren")

    def description(self) -> LocalisationStringType:
        return LocalisationString(
            "Combines the projects of all data sources into one table",
            "Kombiniert die Projekte aller Datenquellen in eine Tabelle",
        )

    def dependencies(self) -> Union[List[str], None]:
        return [FetchFFG.name(), FetchFWF.name()]


def combine_frames(sources: Dict[str, pd.DataFrame], columns: List[str]) -> pd.DataFrame:
    if DATA_SOURCE_COLUMN not in columns:
        columns = [*columns, DATA_SOURCE_COLUMN]
    frames = []
    for source, frame in sources.items():
        frame = frame.copy()
        frame[DATA_SOURCE_COLUMN] = source
        for column in LIST_TEXT_COLUMNS:
            if column in frame.columns:
                frame[column] = frame[column].apply(_join_list)
        frames.append(frame.reindex(columns=columns))
    combined = pd.concat(frames, ignore_index=True)
    if ID_COLUMN in combined.columns:
        combined = combined.drop_duplicates(subset=[DATA_SOURCE_COLUMN, ID_COLUMN], keep="first")
    return combined.reset_index(drop=True)


def _join_list(value: Any) -> Any:
    if isinstance(value, (list, tuple)):
        entries = [str(entry).strip() for entry in value if entry is not None and str(entry).strip()]
        return LIST_SEPARATOR.join(dict.fromkeys(entries)) if entries else None
    return value
