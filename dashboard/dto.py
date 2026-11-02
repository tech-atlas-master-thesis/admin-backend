from dataclasses import dataclass
from typing import List, Optional

from datasets import DatasetDto
from pipelineFramework import PipelineDto, ScheduleDto


@dataclass
class DashboardDto:
    pipelines: DashboardPipelinesDto
    schedules: DashboardSchedulesDto
    dataSets: DashboardDataSetsDto


@dataclass
class DashboardPipelinesDto :
    open: int
    running: int
    finished: int
    error: int
    recentErrors: List[PipelineDto]


@dataclass
class DashboardSchedulesDto :
    active: int
    recentlyExecuted: List[ScheduleDto]


@dataclass
class DashboardDataSetsDto :
    total: int
    latest: Optional[DatasetDto] = None

