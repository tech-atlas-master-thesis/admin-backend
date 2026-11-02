import datetime

from fastapi import FastAPI, Depends

from datasets import DatasetDto
from .dto import DashboardDto, DashboardPipelinesDto, DashboardSchedulesDto, DashboardDataSetsDto
from pipelineFramework import (
    PipelineDto,
    PipelineState,
    get_fe_db_client,
    get_pipeline_db_client,
    require_all_entitlements,
)
from pipelineFramework.server.schedules.pipeline_schedule import PipelineSchedule

AUTH_REQUIREMENTS_VIEW = require_all_entitlements("tech-atlas:read")

RECENT_LIMIT = 5
RECENT_PERIOD = datetime.timedelta(days=7)


async def _get_pipelines(recent_since: datetime.datetime) -> DashboardPipelinesDto:
    pipeline_db = get_pipeline_db_client().pipelines
    state_counts = await pipeline_db.aggregate([{"$group": {"_id": "$state", "count": {"$sum": 1}}}])
    counts = {state_count["_id"]: state_count["count"] async for state_count in state_counts}
    recent_errors = (
        pipeline_db.find({"state": PipelineState.ERROR.value, "created.at": {"$gte": recent_since}})
        .sort({"created.at": -1})
        .limit(RECENT_LIMIT)
    )
    return DashboardPipelinesDto(
        counts.get(PipelineState.OPEN.value, 0),
        counts.get(PipelineState.RUNNING.value, 0),
        counts.get(PipelineState.FINISHED.value, 0),
        counts.get(PipelineState.ERROR.value, 0),
        [PipelineDto.from_entity(pipeline) async for pipeline in recent_errors],
    )


async def _get_schedules(recent_since: datetime.datetime) -> DashboardSchedulesDto:
    schedule_db = get_pipeline_db_client().schedules
    recently_executed = (
        schedule_db.find({"lastExecution": {"$gte": recent_since}}).sort({"lastExecution": -1}).limit(RECENT_LIMIT)
    )
    return DashboardSchedulesDto(
        await schedule_db.count_documents({"active": True}),
        [PipelineSchedule.from_entity(schedule) async for schedule in recently_executed],
    )


async def _get_data_sets() -> DashboardDataSetsDto:
    dataset_db = get_fe_db_client().datasets
    latest = await dataset_db.find_one({"active": True}, sort={"_id": -1})
    return DashboardDataSetsDto(
        await dataset_db.count_documents({"active": True}),
        DatasetDto.from_entity(latest) if latest else None,
    )


def add_dashboard_endpoints(app: FastAPI, api_base_url: str) -> None:
    @app.get(api_base_url + "/dashboard")
    async def get_dashboard(
        _=Depends(AUTH_REQUIREMENTS_VIEW),
    ) -> DashboardDto:
        recent_since = datetime.datetime.now(datetime.UTC) - RECENT_PERIOD
        return DashboardDto(
            await _get_pipelines(recent_since),
            await _get_schedules(recent_since),
            await _get_data_sets(),
        )
