from fastapi import FastAPI, Depends

from .dto import DashboardDto, DashboardPipelinesDto, DashboardSchedulesDto, DashboardDataSetsDto
from pipelineFramework import require_all_entitlements

AUTH_REQUIREMENTS_VIEW = require_all_entitlements("tech-atlas:read")


def add_dashboard_endpoints(app: FastAPI, api_base_url: str) -> None:
    @app.get(api_base_url + "/dashboard")
    async def get_dashboard(
            _=Depends(AUTH_REQUIREMENTS_VIEW),) -> DashboardDto:
        # TODO: Implement
        return DashboardDto(DashboardPipelinesDto(0, 0, 0, 0, []), DashboardSchedulesDto(0, []), DashboardDataSetsDto(0))
