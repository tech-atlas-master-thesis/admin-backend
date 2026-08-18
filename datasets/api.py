import datetime
import io
import re
import zipfile
from dataclasses import dataclass
from http import HTTPStatus
from typing import Optional, List, Any, Annotated, Dict

from bson import ObjectId, json_util as bson_util
from fastapi import FastAPI, HTTPException, Depends, Query
from starlette.responses import Response

from datasets.dto import DatasetDto
from pipelineFramework import (
    PaginatedListDto,
    PageDto,
    get_fe_db_client,
    require_all_entitlements,
    Lookup,
)
from pipelineFramework.server.api.pipeline_api import AUTH_REQUIREMENTS_EDIT

AUTH_REQUIREMENTS_VIEW = require_all_entitlements("tech-atlas:read")


@dataclass
class DataSetObject:
    collection: str
    search_fields: List[str]
    included_fields: List[Lookup]


PROJECTS_DATA = DataSetObject(
    "projects",
    ["short", "title", "abstract"],
    [
        # Lookup("keywords", "keywords", "keywords", "keywords"),
        # Lookup("keyTechnologies", "keyTechnologies", "keyTechnologies", "keyTechnologies"),
        Lookup("organisations", "organisations", "_id", "organisations"),
        Lookup("organisations", "projectLeader", "_id", "projectLeader"),
        Lookup("grants", "grant", "_id", "grant"),
    ],
)

ORGANISATIONS_DATA = DataSetObject(
    "organisations",
    ["name", "type", "website"],
    [],
)

GRANTS_DATA = DataSetObject(
    "grants",
    ["name"],
    [Lookup("programmes", "programme", "_id", "programme")],
)

PROGRAMMES_DATA = DataSetObject(
    "programmes",
    ["name"],
    [],
)

TECHNOLOGIES_DATA = DataSetObject("technologies", ["label", "short"], [Lookup("fields", "field", "_id", "field")])

FIELD_DATA = DataSetObject("fields", ["label", "short"], [])

OBJECT_CONFIGS: Dict[str, DataSetObject] = {
    "projects": PROJECTS_DATA,
    "organizations": ORGANISATIONS_DATA,
    "grants": GRANTS_DATA,
    "programmes": PROGRAMMES_DATA,
    "technologies": TECHNOLOGIES_DATA,
    "fields": FIELD_DATA,
}


def _serialize_object_ids(obj: Any) -> Any:
    if isinstance(obj, list):
        return [_serialize_object_ids(item) for item in obj]
    if isinstance(obj, dict):
        return {key: _serialize_object_ids(value) for key, value in obj.items()}
    if isinstance(obj, ObjectId):
        return str(obj)
    return obj


async def _get_data_set_object(
    collection: str,
    search_fields: List[str],
    included_fields: List[Lookup],
    dataset_id: str,
    search: Optional[str] = None,
    include_data: Optional[bool] = None,
    sort: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
) -> PaginatedListDto[Any]:
    dataset_db = get_fe_db_client().get_collection(collection)
    query = {}
    aggregation = [{"$match": {"dataset": ObjectId(dataset_id)}}]
    query["dataset"] = ObjectId(dataset_id)
    if search:
        matcher = [{field: {"$regex": re.escape(search), "$options": "i"}} for field in search_fields]
        query["$or"] = matcher
        aggregation.append({"$match": {"$or": matcher}})
    if include_data:
        aggregation += [lookup.serialize() for lookup in included_fields]
    if sort:
        single_sorts = (single_sort.split(":") for single_sort in sort.split(";"))
        sort_query = {field: int(order) for field, order in single_sorts}
    else:
        sort_query = {"_id": -1}
    aggregation.append({"$sort": sort_query})
    aggregation.append({"$skip": offset})
    aggregation.append({"$limit": limit})
    dataset_items = await dataset_db.aggregate(aggregation)
    total_records = await dataset_db.count_documents(query)
    return PaginatedListDto(
        [_serialize_object_ids(dataset_item) async for dataset_item in dataset_items],
        PageDto(offset, limit, total_records),
    )


async def _get_dataset_object_export_json(
    collection: str,
    search_fields: List[str],
    included_fields: List[Lookup],
    dataset_id: str,
    search: Optional[str] = None,
    include_data: Optional[bool] = None,
):

    dataset_db = get_fe_db_client().get_collection(collection)
    aggregation = [{"$match": {"dataset": ObjectId(dataset_id)}}]
    if search:
        aggregation.append(
            {"$or": [{field: {"$regex": re.escape(search), "$options": "$i"}} for field in search_fields]}
        )
    if include_data:
        aggregation += [lookup.serialize() for lookup in included_fields]
    return bson_util.dumps([export_file async for export_file in (await dataset_db.aggregate(aggregation))])


async def _get_data_set_export(
    collection: str,
    search_fields: List[str],
    included_fields: List[Lookup],
    dataset_id: str,
    search: Optional[str] = None,
    include_data: Optional[bool] = None,
) -> Response:
    response = Response(
        await _get_dataset_object_export_json(collection, search_fields, included_fields, dataset_id, search, include_data),
        media_type="text/json",
    )
    response.headers["Content-Disposition"] = (
        f"inline; filename=DataSetExport_{dataset_id}_{collection}_{search if search else 'full'}_{'incl' if include_data else 'ref'}_{datetime.datetime.now(datetime.UTC).isoformat()}.json"
    )
    return response


def add_dataset_endpoints(app: FastAPI, api_base_url: str) -> None:
    @app.get(api_base_url + "/datasets")
    async def get_datasets(
        pipelineType: Annotated[Optional[List[str]], Query()] = None,
        pipelineName: Annotated[Optional[str], Query()] = None,
        sort: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
        _=Depends(AUTH_REQUIREMENTS_VIEW),
    ) -> PaginatedListDto[DatasetDto]:
        dataset_db = get_fe_db_client().datasets
        query = {}
        if pipelineType:
            query["pipelineType"] = {"$in": pipelineType}
        if pipelineName:
            query["pipelineName"] = {"$regex": re.escape(pipelineName)}
        if sort:
            single_sorts = (single_sort.split(":") for single_sort in sort.split(";"))
            sort_query = {field: int(order) for field, order in single_sorts}
        else:
            sort_query = {"_id": -1}
        datasets = dataset_db.find(query).sort(sort_query).skip(offset).limit(limit)
        total_records = await dataset_db.count_documents(query)
        return PaginatedListDto(
            [DatasetDto.from_entity(pipeline) async for pipeline in datasets], PageDto(offset, limit, total_records)
        )

    @app.get(api_base_url + "/datasets/{dataset_id}")
    async def get_dataset(
        dataset_id: str,
        _=Depends(AUTH_REQUIREMENTS_VIEW),
    ) -> DatasetDto:
        dataset_db = get_fe_db_client().datasets
        dataset = await dataset_db.find_one({"_id": ObjectId(dataset_id)})

        if not dataset:
            raise HTTPException(status_code=404, detail=f"Pipeline '{dataset_id}' not found")
        return DatasetDto.from_entity(dataset)

    @app.put(api_base_url + "/datasets/{dataset_id}")
    async def update_dataset(
        dataset_id: str,
        body: DatasetDto,
        _=Depends(AUTH_REQUIREMENTS_EDIT),
    ) -> DatasetDto:
        dataset_db = get_fe_db_client().datasets
        result = await dataset_db.update_one(
            {"_id": ObjectId(dataset_id)},
            {
                "$set": {
                    "pipelineType": body.pipelineType,
                    "pipelineName": body.pipelineName,
                    "active": body.active,
                }
            },
        )

        if not result.acknowledged or result.modified_count != 1:
            raise HTTPException(status_code=404, detail=f"Pipeline '{dataset_id}' not found")

        dataset = await dataset_db.find_one({"_id": ObjectId(dataset_id)})
        if not dataset:
            raise HTTPException(status_code=404, detail=f"Pipeline '{dataset_id}' not found")
        return DatasetDto.from_entity(dataset)

    @app.delete(api_base_url + "/datasets/{dataset_id}", status_code=HTTPStatus.NO_CONTENT)
    async def update_dataset(
        dataset_id: str,
        _=Depends(AUTH_REQUIREMENTS_EDIT),
    ):
        dataset_db = get_fe_db_client().datasets
        result = await dataset_db.delete_one({"_id": ObjectId(dataset_id)})

        if not result.acknowledged or result.deleted_count != 1:
            raise HTTPException(status_code=404, detail=f"Pipeline '{dataset_id}' not found")

    @app.get(api_base_url + "/datasets/{dataset_id}/export")
    async def get_full_dataset_export(
        dataset_id: str,
        _=Depends(AUTH_REQUIREMENTS_VIEW),
    ):
        with io.BytesIO() as zip_buffer:
            with zipfile.ZipFile(zip_buffer, "w") as zip_file:
                dataset_db = get_fe_db_client().datasets
                dataset = await dataset_db.find_one({"_id": ObjectId(dataset_id)})

                zip_file.writestr(zipfile.ZipInfo("datasets.json"), bson_util.dumps([dataset]))

                for dataset_object in OBJECT_CONFIGS.values():
                    zip_file.writestr(
                        zipfile.ZipInfo(f"{dataset_object.collection}.json"),
                        await _get_dataset_object_export_json(dataset_object.collection, [], [], dataset_id),
                    )
            zip_buffer.seek(0)
            response = Response(zip_buffer.getvalue(), media_type="application/zip")
            response.headers["Content-Disposition"] = (
                f"inline; filename=DataSetFullExport_{dataset_id}_{datetime.datetime.now(datetime.UTC).isoformat()}.zip"
            )
            return response

    @app.get(api_base_url + "/datasets/{dataset_id}/{object_type}")
    async def get_dataset_object(
        dataset_id: str,
        object_type: str,
        search: Optional[str] = None,
        includeData: Optional[bool] = None,
        sort: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
        _=Depends(AUTH_REQUIREMENTS_VIEW),
    ) -> PaginatedListDto[Any]:
        object_config = OBJECT_CONFIGS.get(object_type)
        if not object_config:
            raise HTTPException(status_code=404, detail=f"DataSet object '{object_type}' not found")
        return await _get_data_set_object(
            object_config.collection,
            object_config.search_fields,
            object_config.included_fields,
            dataset_id,
            search,
            includeData,
            sort,
            limit,
            offset,
        )

    @app.get(api_base_url + "/datasets/{dataset_id}/{object_type}/export")
    async def get_dataset_object_export(
        dataset_id: str,
        object_type: str,
        search: Optional[str] = None,
        includeData: Optional[bool] = None,
        _=Depends(AUTH_REQUIREMENTS_VIEW),
    ) -> Response:
        object_config = OBJECT_CONFIGS.get(object_type)
        if not object_config:
            raise HTTPException(status_code=404, detail=f"DataSet object '{object_type}' not found")
        return await _get_data_set_export(
            object_config.collection,
            object_config.search_fields,
            object_config.included_fields,
            dataset_id,
            search,
            includeData,
        )
