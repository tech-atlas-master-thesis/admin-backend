import asyncio
import json
import math
from collections import Counter
from typing import Optional, Union, List, Dict, Any

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    LocalisationStringType,
    LocalisationString,
    StepUserConfig,
    EventType,
)
from pipeline_configs.transform_steps.project_extract import ProjectExtractStep, MATCH_ONLY_COLUMNS
from pipeline_configs.transform_steps.configs import GetTechnologyConfiguration
from pipeline_configs.transform_steps.technology_matcher import (
    ScoreWeights,
    TechnologyTerms,
    project_texts,
    score_project,
    technologies_from_config,
)

FIELD_COLUMNS = {
    "title": ["title", "short", "title_en"],
    "keywords": ["keywords", "keywords_en"],
    "abstract": ["abstract", "abstract_en"],
}


class ProjectsFilterStep(StepConfig):
    MIN_CHUNK_SIZE = 500
    PROGRESS_REPORTS = 10

    DEFAULTS = {
        "SCORE_TITLE": 5.0,
        "SCORE_KEYWORDS": 3.0,
        "SCORE_ABSTRACT": 1.0,
        "SCORE_EXCLUDED_MULTIPLIER": 1.0,
        "SCORE_THRESHOLD": 5.0,
    }

    async def run(self, user_config: Optional[UserStepConfig], results: Optional[Dict[str, Any]] = None, **_):
        if results is None:
            results = {}
        PROJECTS: List[Dict[str, Any]] = results.get(ProjectExtractStep.name())
        TECH_CONFIG = results.get(GetTechnologyConfiguration.name())
        if PROJECTS is None:
            raise FileNotFoundError("No project data found")
        if TECH_CONFIG is None:
            raise FileNotFoundError("No technology configuration found")
        yield "Data found", EventType.INFO

        weights = ScoreWeights(
            title=self._get(user_config, "SCORE_TITLE"),
            keywords=self._get(user_config, "SCORE_KEYWORDS"),
            abstract=self._get(user_config, "SCORE_ABSTRACT"),
            excluded_multiplier=self._get(user_config, "SCORE_EXCLUDED_MULTIPLIER"),
        )
        threshold = self._get(user_config, "SCORE_THRESHOLD")
        technologies = technologies_from_config(TECH_CONFIG)

        kept: List[Dict[str, Any]] = []
        dropped = Counter()
        chunk_size = max(self.MIN_CHUNK_SIZE, math.ceil(len(PROJECTS) / self.PROGRESS_REPORTS))
        for start in range(0, len(PROJECTS), chunk_size):
            chunk_kept, chunk_dropped = await asyncio.to_thread(
                self.filter_projects, PROJECTS[start : start + chunk_size], technologies, weights, threshold
            )
            kept.extend(chunk_kept)
            dropped.update(chunk_dropped)
            done = min(start + chunk_size, len(PROJECTS))
            if done < len(PROJECTS):
                yield f"Scored {done} of {len(PROJECTS)} projects", EventType.INFO

        kept_by_source = Counter(project.get("data_source") for project in kept)
        yield (
            f"Kept {len(kept)} of {len(PROJECTS)} projects with a technology score of at least {threshold} "
            f"(kept: {dict(kept_by_source)}, dropped: {dict(dropped)})",
            EventType.INFO,
        )
        yield kept, EventType.RESULT

    @staticmethod
    def filter_projects(
        projects: List[Dict[str, Any]], technologies: List[TechnologyTerms], weights: ScoreWeights, threshold: float
    ) -> tuple[List[Dict[str, Any]], Counter]:
        # Runs in a worker thread: it only reads the shared projects and builds new dicts.
        kept: List[Dict[str, Any]] = []
        dropped = Counter()
        for project in projects:
            match = score_project(project_texts(project, FIELD_COLUMNS), technologies, weights)
            for entry in match.values():
                entry["passed"] = entry["score"] >= threshold
            # keyTechnologies keeps holding labels (in configuration order), as later steps expect.
            passed = list(dict.fromkeys(tech.label for tech in technologies if match.get(tech.short, {}).get("passed")))
            if not passed:
                dropped[project.get("data_source")] += 1
                continue
            filtered = {key: value for key, value in project.items() if key not in MATCH_ONLY_COLUMNS}
            filtered["keyTechnologies"] = json.dumps(passed)
            filtered["_match"] = match
            kept.append(filtered)
        return kept, dropped

    def _get(self, user_config: Optional[UserStepConfig], key: str) -> float:
        value = user_config.get(key) if user_config else None
        return float(self.DEFAULTS[key] if value is None else value)

    def user_config(self) -> List[StepUserConfig]:
        return [
            StepUserConfig(
                "SCORE_TITLE",
                LocalisationString("Score: title hit", "Punkte: Treffer im Titel"),
                LocalisationString(
                    "Added for every occurrence of a search term in the project title",
                    "Wird für jedes Vorkommen eines Suchbegriffs im Projekttitel addiert",
                ),
                StepUserConfig.StepUserConfigType.FLOAT,
                self.DEFAULTS["SCORE_TITLE"],
            ),
            StepUserConfig(
                "SCORE_KEYWORDS",
                LocalisationString("Score: keyword hit", "Punkte: Treffer in Keywords"),
                LocalisationString(
                    "Added for every occurrence of a search term in the keywords given by the project",
                    "Wird für jedes Vorkommen eines Suchbegriffs in den Keywords des Projekts addiert",
                ),
                StepUserConfig.StepUserConfigType.FLOAT,
                self.DEFAULTS["SCORE_KEYWORDS"],
            ),
            StepUserConfig(
                "SCORE_ABSTRACT",
                LocalisationString("Score: abstract hit", "Punkte: Treffer im Abstract"),
                LocalisationString(
                    "Added for every occurrence of a search term in the abstract",
                    "Wird für jedes Vorkommen eines Suchbegriffs im Abstract addiert",
                ),
                StepUserConfig.StepUserConfigType.FLOAT,
                self.DEFAULTS["SCORE_ABSTRACT"],
            ),
            StepUserConfig(
                "SCORE_EXCLUDED_MULTIPLIER",
                LocalisationString("Score: excluded term multiplier", "Punkte: Multiplikator für Ausschlussbegriffe"),
                LocalisationString(
                    "An excluded term subtracts the score of its field times this multiplier",
                    "Ein Ausschlussbegriff zieht die Punkte seines Feldes mal diesem Multiplikator ab",
                ),
                StepUserConfig.StepUserConfigType.FLOAT,
                self.DEFAULTS["SCORE_EXCLUDED_MULTIPLIER"],
            ),
            StepUserConfig(
                "SCORE_THRESHOLD",
                LocalisationString("Score threshold", "Punkte-Schwellenwert"),
                LocalisationString(
                    "Minimum score for a technology to be assigned to a project. Projects without any "
                    "assigned technology are removed.",
                    "Mindestpunktzahl, damit eine Technologie einem Projekt zugeordnet wird. Projekte ohne "
                    "zugeordnete Technologie werden entfernt.",
                ),
                StepUserConfig.StepUserConfigType.FLOAT,
                self.DEFAULTS["SCORE_THRESHOLD"],
            ),
        ]

    @staticmethod
    def name() -> str:
        return "projects_filter"

    @staticmethod
    def display_name() -> LocalisationStringType:
        return LocalisationString("Filter Projects by Technology Score", "Projekte nach Technologie-Score filtern")

    def description(self) -> LocalisationStringType:
        return LocalisationString(
            "Scores every project against every technology and keeps only technologies above the threshold",
            "Bewertet jedes Projekt für jede Technologie und behält nur Technologien über dem Schwellenwert",
        )

    def dependencies(self) -> Union[List[str], None]:
        return [ProjectExtractStep.name(), GetTechnologyConfiguration.name()]
