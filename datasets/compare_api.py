import asyncio
import datetime
import re
from typing import Annotated, Any, Dict, List, Optional, Tuple

from bson import ObjectId
from bson.errors import InvalidId
from fastapi import Depends, FastAPI, HTTPException, Query
from starlette.responses import Response

from datasets.compare import COMPARISON_VERSION, DatasetData, compare_datasets, name_key
from pipelineFramework import (
    PageDto,
    PaginatedListDto,
    get_fe_db_client,
    get_pipeline_db_client,
    require_all_entitlements,
)
from pipelineFramework.server.api.pipeline_api import AUTH_REQUIREMENTS_EDIT

AUTH_REQUIREMENTS_VIEW = require_all_entitlements("tech-atlas:read")

ROW_BATCH = 1000
ROW_KINDS = ("projects", "organisations", "grants", "programmes")
DEFAULT_SORT = {
    "projects": {"maxScoreDelta": -1, "key": 1},
    "organisations": {"change": 1, "name": 1},
    "grants": {"projectsDelta": 1, "name": 1},
    "programmes": {"projectsDelta": 1, "name": 1},
}
HIDDEN_ROW_FIELDS = {"_id": 0, "comparison": 0, "base": 0, "candidate": 0, "kind": 0, "search": 0}

PROJECT_FIELDS = {
    "externalId": 1,
    "data_source": 1,
    "title": 1,
    "short": 1,
    "keyTechnologies": 1,
    "organisations": 1,
    "_match": 1,
}
ORGANISATION_FIELDS = {
    "name": 1,
    "type": 1,
    "ror": 1,
    "firmenbuchnummer": 1,
    "website": 1,
    "aliases": 1,
    "address": 1,
    "geo": 1,
    "_match.ror": 1,
    "_match.gleif": 1,
    "_match.geo": 1,
    "_match.address": 1,
    "_match.snapshots": 1,
    "_match.warnings": 1,
}

# One computation per dataset pair at a time; a second request waits for the first and then reads the cache.
_locks: Dict[Tuple[ObjectId, ObjectId], asyncio.Lock] = {}


def _comparisons():
    return get_pipeline_db_client().dataset_comparisons


def _rows():
    return get_pipeline_db_client().dataset_comparison_rows


def _object_id(value: str, name: str) -> ObjectId:
    try:
        return ObjectId(value)
    except InvalidId, TypeError:
        raise HTTPException(status_code=400, detail=f"'{value}' is not a valid {name} id")


def _pair(base: str, candidate: str) -> Tuple[ObjectId, ObjectId]:
    base_id, candidate_id = _object_id(base, "base dataset"), _object_id(candidate, "candidate dataset")
    if base_id == candidate_id:
        raise HTTPException(status_code=400, detail="Select two different datasets to compare")
    return base_id, candidate_id


async def _load(dataset_id: ObjectId) -> DatasetData:
    database = get_fe_db_client()
    dataset = await database.datasets.find_one({"_id": dataset_id})
    if dataset is None:
        raise HTTPException(status_code=404, detail=f"DataSet '{dataset_id}' not found")
    pipeline = None
    if dataset.get("pipeline") is not None:
        pipeline = await get_pipeline_db_client().pipelines.find_one({"_id": dataset["pipeline"]})

    async def collection(name: str, projection: Optional[Dict[str, int]] = None) -> List[Dict[str, Any]]:
        return await database.get_collection(name).find({"dataset": dataset_id}, projection).to_list(None)

    projects, organisations, technologies, fields, grants, programmes = await asyncio.gather(
        collection("projects", PROJECT_FIELDS),
        collection("organisations", ORGANISATION_FIELDS),
        collection("technologies"),
        collection("fields"),
        collection("grants"),
        collection("programmes"),
    )
    return DatasetData(dataset, pipeline, projects, organisations, technologies, fields, grants, programmes)


async def drop_comparisons(dataset_id: Optional[ObjectId] = None, pair: Optional[Tuple[ObjectId, ObjectId]] = None):
    if pair is not None:
        query: Dict[str, Any] = {"base": pair[0], "candidate": pair[1]}
    else:
        query = {"$or": [{"base": dataset_id}, {"candidate": dataset_id}]}
    await _rows().delete_many(query)
    await _comparisons().delete_many(query)


async def get_comparison(base_id: ObjectId, candidate_id: ObjectId) -> Dict[str, Any]:
    query = {"base": base_id, "candidate": candidate_id, "version": COMPARISON_VERSION}
    cached = await _comparisons().find_one(query)
    if cached:
        return cached
    lock = _locks.setdefault((base_id, candidate_id), asyncio.Lock())
    async with lock:
        cached = await _comparisons().find_one(query)
        if cached:
            return cached
        base, candidate = await asyncio.gather(_load(base_id), _load(candidate_id))
        comparison = await asyncio.to_thread(compare_datasets, base, candidate)

        await drop_comparisons(pair=(base_id, candidate_id))
        await _rows().create_index([("comparison", 1), ("kind", 1)])
        comparison_id = ObjectId()
        for kind, rows in comparison.rows.items():
            for start in range(0, len(rows), ROW_BATCH):
                await _rows().insert_many(
                    [
                        {**row, "comparison": comparison_id, "base": base_id, "candidate": candidate_id, "kind": kind}
                        for row in rows[start : start + ROW_BATCH]
                    ]
                )
        # The summary goes in last: a reader that finds it can rely on all rows being there.
        document = {
            "_id": comparison_id,
            **query,
            "computedAt": datetime.datetime.now(datetime.UTC),
            "summary": comparison.summary,
        }
        await _comparisons().insert_one(document)
        return document


def _sort(sort: Optional[str], kind: str) -> Dict[str, int]:
    if not sort:
        return DEFAULT_SORT[kind]
    return {field: int(order) for field, order in (single.split(":") for single in sort.split(";"))}


async def query_rows(
    comparison: Dict[str, Any],
    kind: str,
    filters: Dict[str, Any],
    search: Optional[str],
    sort: Optional[str],
    limit: Optional[int],
    offset: int,
) -> Tuple[List[Dict[str, Any]], int]:
    query: Dict[str, Any] = {"comparison": comparison["_id"], "kind": kind}
    query.update({key: value for key, value in filters.items() if value is not None})
    if search:
        query["search"] = {"$regex": re.escape(name_key(search))}
    cursor = _rows().find(query, HIDDEN_ROW_FIELDS).sort(_sort(sort, kind)).skip(offset)
    if limit:
        cursor = cursor.limit(limit)
    return await cursor.to_list(None), await _rows().count_documents(query)


def _in(values: Optional[List[str]]) -> Optional[Dict[str, Any]]:
    return {"$in": values} if values else None


def add_dataset_compare_endpoints(app: FastAPI, api_base_url: str) -> None:
    base_url = api_base_url + "/datasets/compare"

    async def rows_endpoint(kind, base, candidate, filters, search, sort, limit, offset):
        comparison = await get_comparison(*_pair(base, candidate))
        items, total = await query_rows(comparison, kind, filters, search, sort, limit, offset)
        return PaginatedListDto(items, PageDto(offset, limit, total))

    @app.get(base_url)
    async def get_dataset_comparison(base: str, candidate: str, _=Depends(AUTH_REQUIREMENTS_VIEW)) -> Dict[str, Any]:
        comparison = await get_comparison(*_pair(base, candidate))
        return {"computedAt": comparison["computedAt"], **comparison["summary"]}

    @app.delete(base_url, status_code=204)
    async def delete_dataset_comparison(base: str, candidate: str, _=Depends(AUTH_REQUIREMENTS_EDIT)):
        await drop_comparisons(pair=_pair(base, candidate))

    @app.get(base_url + "/projects")
    async def get_dataset_comparison_projects(
        base: str,
        candidate: str,
        change: Annotated[Optional[List[str]], Query()] = None,
        technology: Annotated[Optional[List[str]], Query()] = None,
        dataSource: Annotated[Optional[List[str]], Query()] = None,
        search: Optional[str] = None,
        sort: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
        _=Depends(AUTH_REQUIREMENTS_VIEW),
    ) -> PaginatedListDto[Any]:
        filters = {"change": _in(change), "touched": _in(technology), "dataSource": _in(dataSource)}
        return await rows_endpoint("projects", base, candidate, filters, search, sort, limit, offset)

    @app.get(base_url + "/organisations")
    async def get_dataset_comparison_organisations(
        base: str,
        candidate: str,
        change: Annotated[Optional[List[str]], Query()] = None,
        attribute: Annotated[Optional[List[str]], Query()] = None,
        statusStage: Optional[str] = None,
        statusBase: Optional[str] = None,
        statusCandidate: Optional[str] = None,
        geoFlag: Optional[bool] = None,
        search: Optional[str] = None,
        sort: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
        _=Depends(AUTH_REQUIREMENTS_VIEW),
    ) -> PaginatedListDto[Any]:
        filters: Dict[str, Any] = {"change": _in(change), "changedAttributes": _in(attribute), "geoFlag": geoFlag}
        if statusStage:
            filters[f"statusBase.{statusStage}"] = statusBase
            filters[f"statusCandidate.{statusStage}"] = statusCandidate
        return await rows_endpoint("organisations", base, candidate, filters, search, sort, limit, offset)

    @app.get(base_url + "/grants")
    async def get_dataset_comparison_grants(
        base: str,
        candidate: str,
        change: Annotated[Optional[List[str]], Query()] = None,
        search: Optional[str] = None,
        sort: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
        _=Depends(AUTH_REQUIREMENTS_VIEW),
    ) -> PaginatedListDto[Any]:
        return await rows_endpoint("grants", base, candidate, {"change": _in(change)}, search, sort, limit, offset)

    @app.get(base_url + "/programmes")
    async def get_dataset_comparison_programmes(
        base: str,
        candidate: str,
        change: Annotated[Optional[List[str]], Query()] = None,
        search: Optional[str] = None,
        sort: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
        _=Depends(AUTH_REQUIREMENTS_VIEW),
    ) -> PaginatedListDto[Any]:
        return await rows_endpoint("programmes", base, candidate, {"change": _in(change)}, search, sort, limit, offset)

    @app.get(base_url + "/report")
    async def get_dataset_comparison_report(
        base: str,
        candidate: str,
        appendix: bool = False,
        _=Depends(AUTH_REQUIREMENTS_VIEW),
    ) -> Response:
        # Imported here so WeasyPrint's native libraries are only needed when a report is rendered.
        from datasets.compare_report import render_report

        comparison = await get_comparison(*_pair(base, candidate))
        rows = {}
        for kind in ROW_KINDS:
            rows[kind], _total = await query_rows(comparison, kind, {}, None, None, None, 0)
        pdf = await asyncio.to_thread(render_report, comparison, rows, appendix)
        timestamp = datetime.datetime.now(datetime.UTC).strftime("%Y%m%dT%H%M%SZ")
        response = Response(pdf, media_type="application/pdf")
        response.headers["Content-Disposition"] = (
            f"inline; filename=DataSetComparison_{base}_{candidate}_{timestamp}.pdf"
        )
        return response
