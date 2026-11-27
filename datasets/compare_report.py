import base64
import datetime
import io
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402
from jinja2 import Environment, FileSystemLoader, select_autoescape  # noqa: E402
from weasyprint import HTML  # noqa: E402

TEMPLATES = Path(__file__).parent / "templates"
TOP_PROJECTS = 10
TOP_ROWS = 50
ADDED, REMOVED, BASE, CANDIDATE = "#2e7d32", "#c62828", "#90a4ae", "#1565c0"

_environment = Environment(loader=FileSystemLoader(TEMPLATES), autoescape=select_autoescape(["html"]))


def _delta(value: Optional[float]) -> str:
    if value is None:
        return "–"
    if isinstance(value, float) and not value.is_integer():
        return f"{value:+.2f}"
    return f"{int(value):+d}"


def _number(value: Any) -> str:
    if value is None:
        return "–"
    if isinstance(value, float):
        return f"{value:g}"
    return f"{value:,}".replace(",", " ") if isinstance(value, int) else str(value)


def _percent(value: Optional[float]) -> str:
    return "–" if value is None else f"{100 * value:.0f} %"


def _date(value: Any) -> str:
    if isinstance(value, datetime.datetime):
        return value.strftime("%Y-%m-%d %H:%M")
    return "–" if value is None else str(value)


def _short(value: Any, length: int = 60) -> str:
    text = "–" if value is None else str(value)
    return text if len(text) <= length else text[: length - 1] + "…"


_environment.filters.update(delta=_delta, number=_number, percent=_percent, date=_date, short=_short)


def _svg(figure) -> str:
    buffer = io.BytesIO()
    figure.savefig(buffer, format="svg", bbox_inches="tight")
    plt.close(figure)
    return "data:image/svg+xml;base64," + base64.b64encode(buffer.getvalue()).decode()


def technology_chart(per_technology: List[Dict[str, Any]]) -> Optional[str]:
    rows = [row for row in per_technology if row["added"] or row["removed"]]
    if not rows:
        return None
    rows.sort(key=lambda row: (row["field"], row["label"] or row["short"]))
    labels = [_short(row["label"] or row["short"], 40) for row in rows]
    figure, axis = plt.subplots(figsize=(7, max(1.6, 0.26 * len(rows) + 0.8)))
    positions = range(len(rows))
    axis.barh(positions, [row["added"] for row in rows], color=ADDED, label="added")
    axis.barh(positions, [-row["removed"] for row in rows], color=REMOVED, label="removed")
    axis.set_yticks(list(positions), labels, fontsize=7)
    axis.invert_yaxis()
    axis.axvline(0, color="#333", linewidth=0.6)
    axis.tick_params(axis="x", labelsize=7)
    axis.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda value, _: f"{abs(int(value))}"))
    axis.set_xlabel("project links", fontsize=8)
    axis.legend(fontsize=7, loc="lower right", frameon=False)
    for side in ("top", "right"):
        axis.spines[side].set_visible(False)
    return _svg(figure)


def coverage_chart(coverage: List[Dict[str, Any]]) -> Optional[str]:
    if not coverage:
        return None
    figure, axis = plt.subplots(figsize=(7, 2.6))
    width = 0.4
    positions = range(len(coverage))
    axis.bar([p - width / 2 for p in positions], [row["base"] for row in coverage], width, color=BASE, label="base")
    axis.bar(
        [p + width / 2 for p in positions],
        [row["candidate"] for row in coverage],
        width,
        color=CANDIDATE,
        label="candidate",
    )
    axis.set_xticks(list(positions), [row["attribute"] for row in coverage], rotation=40, ha="right", fontsize=7)
    axis.tick_params(axis="y", labelsize=7)
    axis.set_ylabel("organisations with value", fontsize=8)
    axis.legend(fontsize=7, frameon=False)
    for side in ("top", "right"):
        axis.spines[side].set_visible(False)
    return _svg(figure)


def transition_matrix(table: Dict[str, Any]) -> Dict[str, Any]:
    states = table["states"]
    counts = {(cell["base"], cell["candidate"]): cell["count"] for cell in table["cells"]}
    maximum = max(counts.values(), default=0)
    return {
        "states": states,
        "rows": [
            {
                "state": old,
                "cells": [
                    {
                        "count": counts.get((old, new), 0),
                        "diagonal": old == new,
                        "shade": round(counts.get((old, new), 0) / maximum, 2) if maximum else 0,
                    }
                    for new in states
                ],
            }
            for old in states
        ],
    }


def _score(entry: Optional[Dict[str, Any]]) -> Optional[float]:
    return entry.get("score") if entry else None


def top_projects_per_technology(projects: List[Dict[str, Any]], technologies: List[Dict[str, Any]]):
    gained, lost = defaultdict(list), defaultdict(list)
    for row in projects:
        for detail in row["technologies"]:
            if detail["link"] not in ("added", "removed"):
                continue
            item = {
                "key": row["key"],
                "title": row["title"],
                "reason": detail["reason"],
                "base": _score(detail["base"]),
                "candidate": _score(detail["candidate"]),
                "terms": detail["newTerms"] if detail["link"] == "added" else detail["lostTerms"],
            }
            (gained if detail["link"] == "added" else lost)[detail["short"]].append(item)
    result = []
    for technology in technologies:
        short = technology["short"]
        if not gained.get(short) and not lost.get(short):
            continue
        result.append(
            {
                "short": short,
                "label": technology["label"],
                "gained": sorted(gained[short], key=lambda item: -(item["candidate"] or 0))[:TOP_PROJECTS],
                "lost": sorted(lost[short], key=lambda item: -(item["base"] or 0))[:TOP_PROJECTS],
                "gainedTotal": len(gained[short]),
                "lostTotal": len(lost[short]),
            }
        )
    return result


def build_context(comparison: Dict[str, Any], rows: Dict[str, List[Dict[str, Any]]], appendix: bool) -> Dict[str, Any]:
    summary = comparison["summary"]
    organisations = rows.get("organisations", [])
    tech_links = summary["technologyLinks"]
    return {
        "summary": summary,
        "computedAt": comparison.get("computedAt"),
        "generatedAt": datetime.datetime.now(datetime.UTC),
        "appendix": appendix,
        "charts": {
            "technologies": technology_chart(tech_links["perTechnology"]),
            "coverage": coverage_chart(summary["organisations"]["coverage"]),
        },
        "transitions": {
            stage: transition_matrix(table) for stage, table in summary["organisations"]["transitions"].items()
        },
        "changedTechnologies": [row for row in tech_links["perTechnology"] if row["added"] or row["removed"]],
        "topProjects": top_projects_per_technology(rows.get("projects", []), tech_links["perTechnology"]),
        "projectsEntered": [row for row in rows.get("projects", []) if row["change"] == "entered"],
        "projectsLeft": [row for row in rows.get("projects", []) if row["change"] == "left"],
        "conflicts": [row for row in organisations if row["change"] == "conflict"],
        "lostMatches": [
            row
            for row in organisations
            if any(row["attributes"].get(name, {}).get("change") == "lost" for name in ("ror", "lei", "geo"))
        ],
        "geoMoves": sorted(
            (row for row in organisations if row.get("geoFlag")), key=lambda row: -(row["geoDistanceKm"] or 0)
        ),
        "organisationsAdded": [row for row in organisations if row["change"] == "added"],
        "organisationsRemoved": [row for row in organisations if row["change"] == "removed"],
        "organisationsRenamed": [row for row in organisations if row["identity"] == "alias"],
        "grants": rows.get("grants", []),
        "programmes": rows.get("programmes", []),
        "projects": rows.get("projects", []),
        "organisations": organisations,
        "limit": None if appendix else TOP_ROWS,
    }


def render_html(comparison: Dict[str, Any], rows: Dict[str, List[Dict[str, Any]]], appendix: bool = False) -> str:
    return _environment.get_template("comparison_report.html").render(**build_context(comparison, rows, appendix))


def render_report(comparison: Dict[str, Any], rows: Dict[str, List[Dict[str, Any]]], appendix: bool = False) -> bytes:
    return HTML(string=render_html(comparison, rows, appendix), base_url=str(TEMPLATES)).write_pdf()
