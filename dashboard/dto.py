from dataclasses import dataclass
from typing import List, Optional

from datasets import DatasetDto
from pipelineFramework import PipelineDto
from pipelineFramework.server.schedules.pipeline_schedule import PipelineSchedule


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
    recentlyExecuted: List[PipelineSchedule]


@dataclass
class DashboardDataSetsDto :
    total: int
    latest: Optional[DatasetDto] = None

