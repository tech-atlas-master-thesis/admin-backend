import ast
import math
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional

FIELDS = ("title", "keywords", "abstract")

# A term starts and ends at a word boundary: no letter, digit or underscore directly before or
# after it. Hyphens and punctuation are boundaries, so "LLM" matches in "LLM-basierte".
_WORD = re.compile(r"\w+")
# Separates list entries (keywords) so that no phrase can run from one entry into the next.
_ENTRY_SEPARATOR = " | "


def _normalise(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip().casefold()


def _first_word(term: str) -> Optional[str]:
    word = _WORD.search(term.casefold())
    return word.group(0) if word else None


def _compile(terms: Iterable[str]) -> Optional[re.Pattern]:
    unique = {}
    for term in terms:
        key = _normalise(term)
        if key and key not in unique:
            unique[key] = term
    if not unique:
        return None
    # Longest first, so the regex alternation prefers the longer of two overlapping terms.
    ordered = sorted(unique, key=len, reverse=True)
    alternatives = "|".join(r"\s+".join(re.escape(part) for part in key.split(" ")) for key in ordered)
    return re.compile(rf"(?<!\w)(?:{alternatives})(?!\w)", re.IGNORECASE)


@dataclass
class TechnologyTerms:
    label: str
    short: str
    any_of: List[str]
    excluded: List[str]
    positive_pattern: Optional[re.Pattern] = field(init=False, repr=False)
    excluded_pattern: Optional[re.Pattern] = field(init=False, repr=False)
    first_words: set = field(init=False, repr=False)
    canonical: Dict[str, str] = field(init=False, repr=False)

    def __post_init__(self):
        self.positive_pattern = _compile(self.any_of)
        self.excluded_pattern = _compile(self.excluded)
        self.first_words = {w for w in map(_first_word, [*self.any_of, *self.excluded]) if w}
        # Maps the normalised matched text back to the term as written in the configuration.
        self.canonical = {}
        for term in [*self.any_of, *self.excluded]:
            self.canonical.setdefault(_normalise(term), term)

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "TechnologyTerms":
        search = config.get("searchTerms") or {}
        if "anyOf" not in search:
            raise KeyError(f"Technology configuration {config.get('label')} has no searchTerms.anyOf")
        if not config.get("short"):
            raise KeyError(f"Technology configuration {config.get('label')} has no short")
        return cls(config.get("label"), config["short"], search["anyOf"], search.get("excluded") or [])

    def count(self, pattern: Optional[re.Pattern], texts: List[str]) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        if pattern is None:
            return counts
        for text in texts:
            for found in pattern.finditer(text):
                term = self.canonical.get(_normalise(found.group(0)), found.group(0))
                counts[term] = counts.get(term, 0) + 1
        return counts


@dataclass
class ScoreWeights:
    title: float = 5
    keywords: float = 3
    abstract: float = 1
    excluded_multiplier: float = 1

    def of(self, field_name: str) -> float:
        return getattr(self, field_name)


def technologies_from_config(tech_config: List[Dict[str, Any]]) -> List[TechnologyTerms]:
    technologies = [
        TechnologyTerms.from_config(tech) for tech_field in tech_config for tech in tech_field["technologies"]
    ]
    shorts = [tech.short for tech in technologies]
    duplicates = sorted({short for short in shorts if shorts.count(short) > 1})
    if duplicates:
        raise ValueError(f"Technology shorts must be unique, duplicated: {', '.join(duplicates)}")
    return technologies


def _is_missing(value: Any) -> bool:
    return value is None or (isinstance(value, float) and math.isnan(value))


def _as_texts(value: Any) -> List[str]:
    if _is_missing(value):
        return []
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            try:
                parsed = ast.literal_eval(stripped)
                if isinstance(parsed, (list, tuple)):
                    value = parsed
            except ValueError, SyntaxError:
                pass
    if isinstance(value, (list, tuple)):
        entries = [str(entry) for entry in value if not _is_missing(entry)]
        return [_ENTRY_SEPARATOR.join(entries)] if entries else []
    return [str(value)]


def project_texts(project: Dict[str, Any], field_columns: Dict[str, List[str]]) -> Dict[str, List[str]]:
    return {
        field_name: [text for column in columns for text in _as_texts(project.get(column))]
        for field_name, columns in field_columns.items()
    }


def score_project(
    texts: Dict[str, List[str]], technologies: List[TechnologyTerms], weights: ScoreWeights
) -> Dict[str, Dict[str, Any]]:
    words = {word for field_texts in texts.values() for text in field_texts for word in _WORD.findall(text.casefold())}
    result: Dict[str, Dict[str, Any]] = {}
    for tech in technologies:
        if not (tech.first_words & words):
            continue
        matches = {f: tech.count(tech.positive_pattern, texts.get(f, [])) for f in FIELDS}
        excluded = {f: tech.count(tech.excluded_pattern, texts.get(f, [])) for f in FIELDS}
        if not any(matches.values()) and not any(excluded.values()):
            continue
        score = sum(weights.of(f) * sum(matches[f].values()) for f in FIELDS) - weights.excluded_multiplier * sum(
            weights.of(f) * sum(excluded[f].values()) for f in FIELDS
        )
        result[tech.short] = {"label": tech.label, "score": score, "matches": matches, "excluded": excluded}
    return result
