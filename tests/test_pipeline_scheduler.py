"""Cron-triggered schedules, driven through the real APScheduler and PipelineServer.

Only MongoDB is replaced. The fake collection refuses to be used from any loop but the one it
was created for, the same way AsyncMongoClient does, so a job running on APScheduler's own
thread or loop fails here as it would in production.
"""

import asyncio
import contextvars
import datetime
import importlib
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest
import pytest_asyncio
from apscheduler.triggers.cron import CronTrigger
from bson import ObjectId

from pipelineFramework import EventType, PipelineConfig, PipelineServer, PipelineState, StepConfig
from pipelineFramework.server.dto import PipelineCreation
from pipelineFramework.server.dto.dto import AuditInfoDto, UserDto
from pipelineFramework.server.dto.schedules import ScheduleCreation
from pipelineFramework.server.schedules.pipeline_schedule import PipelineSchedule

pipeline_scheduler = importlib.import_module("pipelineFramework.server.schedules.pipeline_scheduler")
server_module = importlib.import_module("pipelineFramework.server.server")

USER = UserDto(1, "Test User")
PIPELINE_TYPE = "test-pipeline"
SUNDAY_CRON = "0 1 * * 0"
FIRE_TIMEOUT = 10


class QuickStep(StepConfig):
    async def run(self, user_config=None, results=None, pipeline=None, step=None, events=None):
        yield "ran", EventType.INFO

    @staticmethod
    def name():
        return "quick-step"

    @staticmethod
    def display_name():
        return None


PIPELINE_CONFIG = PipelineConfig(type=PIPELINE_TYPE, display_name=None, parallelize=False, steps=[QuickStep()])


class FakeCollection:
    def __init__(self, loop):
        self.loop = loop
        self.documents = {}
        self.read_from = []

    def _check_loop(self):
        if asyncio.get_running_loop() is not self.loop:
            raise RuntimeError("Cannot use AsyncMongoClient in different event loop")

    async def insert_one(self, document):
        self._check_loop()
        document = {"_id": ObjectId(), **document}
        self.documents[document["_id"]] = document
        return SimpleNamespace(inserted_id=document["_id"])

    async def find_one(self, query):
        self.read_from.append((asyncio.get_running_loop(), threading.get_ident()))
        self._check_loop()
        return self.documents.get(query["_id"])

    async def replace_one(self, query, document):
        self._check_loop()
        self.documents[query["_id"]] = document

    async def update_one(self, query, update):
        self._check_loop()
        self.documents[query["_id"]].update(update.get("$set", {}))

    async def delete_one(self, query):
        self._check_loop()
        del self.documents[query["_id"]]

    async def find(self):
        self._check_loop()
        for document in list(self.documents.values()):
            yield document


class FakeDatabase:
    def __init__(self, loop):
        self.schedules = FakeCollection(loop)
        self.pipelines = FakeCollection(loop)
        self.steps = FakeCollection(loop)


@pytest_asyncio.fixture
async def database(monkeypatch):
    database = FakeDatabase(asyncio.get_running_loop())
    monkeypatch.setattr(server_module, "get_pipeline_db_client", lambda: database)
    monkeypatch.setattr(pipeline_scheduler, "get_pipeline_db_client", lambda: database)
    return database


@pytest_asyncio.fixture
async def server(database):
    # APScheduler's shutdown has to run in the context its start-up ran in.
    context = contextvars.copy_context()
    server = context.run(PipelineServer, [PIPELINE_CONFIG], [])
    try:
        yield server
    finally:
        # Off the loop: closing waits for running jobs, and those wait for this loop.
        await asyncio.to_thread(context.run, server.scheduler.scheduler._exit_stack.close)
        await asyncio.gather(*server.running_tasks, return_exceptions=True)
        server.running_tasks.clear()


@pytest.fixture
def bare_scheduler(monkeypatch):
    """A scheduler with nothing behind it, for what happens without a usable main loop."""
    monkeypatch.setattr(pipeline_scheduler, "get_pipeline_db_client", lambda: SimpleNamespace(schedules=None))
    scheduler = pipeline_scheduler.PipelineScheduler(SimpleNamespace(pipeline_configs=[], event_loop=None))
    yield scheduler
    scheduler.scheduler._exit_stack.close()


@pytest.fixture
def fire_every_second(monkeypatch):
    """Cron's finest grain is a minute, too slow to wait for, so every schedule fires each second."""
    trigger = SimpleNamespace(from_crontab=lambda _cron: CronTrigger(second="*"))
    monkeypatch.setattr(pipeline_scheduler, "CronTrigger", trigger)


def schedule_creation(cron=SUNDAY_CRON, active=True, name="scheduled"):
    pipeline = PipelineCreation(type=PIPELINE_TYPE, name=name, description="")
    return ScheduleCreation(pipeline=pipeline, active=active, cron=cron)


def saved_schedule(cron=SUNDAY_CRON, active=True):
    return PipelineSchedule(
        str(ObjectId()),
        schedule_creation(cron or SUNDAY_CRON, active).to_pipeline(),
        active,
        cron,
        AuditInfoDto(USER, datetime.datetime.now(datetime.UTC)),
    ).to_entity()


def scheduled_ids(server):
    return {schedule.id for schedule in server.scheduler.scheduler.get_schedules()}


async def wait_until(condition):
    async with asyncio.timeout(FIRE_TIMEOUT):
        while not condition():
            await asyncio.sleep(0.05)


@pytest.mark.asyncio
async def test_firing_a_schedule_runs_it_on_the_main_loop_and_creates_a_pipeline(server, database, fire_every_second):
    await server.bind_event_loop()

    schedule = await server.scheduler.add_schedule(schedule_creation(), USER)
    await wait_until(lambda: server.pipelines)
    # One firing is enough: taking it away here keeps the next one out of the assertions below.
    server.scheduler.scheduler.remove_schedule(schedule.id)

    # run() starts by reading the schedule, so this is the thread and loop it ran on. The pipeline
    # landing on this server is what shows the job reached the bound scheduler instance.
    assert database.schedules.read_from == [(asyncio.get_running_loop(), threading.get_ident())]

    pipeline = server.pipelines[0]
    assert pipeline.schedule_id == schedule.id
    assert pipeline.created.by == USER
    assert pipeline.id in database.pipelines.documents

    await wait_until(lambda: pipeline.state == PipelineState.FINISHED)
    saved = database.schedules.documents[ObjectId(schedule.id)]
    assert saved["lastPipeline"] == str(pipeline.id)
    assert saved["lastExecution"] == pipeline.created.at


@pytest.mark.asyncio
async def test_schedules_firing_at_the_same_time_all_run(server, fire_every_second):
    await server.bind_event_loop()

    schedules = [await server.scheduler.add_schedule(schedule_creation(name=str(n)), USER) for n in range(3)]
    expected = {schedule.id for schedule in schedules}
    await wait_until(lambda: {pipeline.schedule_id for pipeline in server.pipelines} == expected)
    for schedule in schedules:
        server.scheduler.scheduler.remove_schedule(schedule.id)


@pytest.mark.asyncio
async def test_saved_schedules_are_restarted_and_fire(server, database, fire_every_second):
    active = saved_schedule()
    inactive = saved_schedule(active=False)
    without_cron = saved_schedule(cron=None)
    for entity in (active, inactive, without_cron):
        database.schedules.documents[entity["_id"]] = entity

    await server.bind_event_loop()

    assert scheduled_ids(server) == {str(active["_id"])}
    await wait_until(lambda: server.pipelines)
    server.scheduler.scheduler.remove_schedule(str(active["_id"]))
    assert server.pipelines[0].schedule_id == str(active["_id"])


@pytest.mark.asyncio
async def test_add_update_and_delete_keep_the_scheduler_in_step(server, database):
    await server.bind_event_loop()

    schedule = await server.scheduler.add_schedule(schedule_creation(), USER)
    inactive = await server.scheduler.add_schedule(schedule_creation(active=False), USER)
    assert scheduled_ids(server) == {schedule.id}
    assert ObjectId(inactive.id) in database.schedules.documents

    updated = await server.scheduler.update_schedule(schedule.id, schedule_creation(cron="0 2 * * *"), USER)
    assert updated.cron == "0 2 * * *"
    fire_time = server.scheduler.scheduler.get_schedule(schedule.id).next_fire_time
    assert (fire_time.hour, fire_time.minute) == (2, 0)
    assert database.schedules.documents[ObjectId(schedule.id)]["cron"] == "0 2 * * *"

    await server.scheduler.update_schedule(schedule.id, schedule_creation(active=False), USER)
    assert scheduled_ids(server) == set()

    await server.scheduler.update_schedule(schedule.id, schedule_creation(), USER)
    assert scheduled_ids(server) == {schedule.id}

    await server.scheduler.delete_schedule(schedule.id)
    assert scheduled_ids(server) == set()
    assert ObjectId(schedule.id) not in database.schedules.documents


@pytest.mark.asyncio
async def test_manual_run_uses_the_calling_user_and_needs_no_cron(server, database):
    await server.bind_event_loop()
    schedule = await server.scheduler.add_schedule(schedule_creation(active=False), USER)
    other_user = UserDto(2, "Other User")

    pipeline = await server.scheduler.run(schedule.id, other_user)

    assert pipeline.created.by == other_user
    assert pipeline.schedule_id == schedule.id
    assert database.schedules.documents[ObjectId(schedule.id)]["lastPipeline"] == str(pipeline.id)
    await wait_until(lambda: pipeline.state == PipelineState.FINISHED)


def test_a_firing_gives_up_once_the_main_loop_has_stopped(bare_scheduler):
    # A job left waiting on a stopped loop would hold up interpreter exit, which joins the job's thread.
    loop = asyncio.new_event_loop()
    started = threading.Event()

    async def never_finishes(_schedule_id):
        started.set()
        await asyncio.sleep(3600)

    bare_scheduler.run = never_finishes
    bare_scheduler.pipeline_server.event_loop = loop
    loop_thread = threading.Thread(target=loop.run_forever, daemon=True)
    loop_thread.start()
    executor = ThreadPoolExecutor(max_workers=1)
    try:
        firing = executor.submit(bare_scheduler._fire, str(ObjectId()))
        assert started.wait(FIRE_TIMEOUT)
        loop.call_soon_threadsafe(loop.stop)
        with pytest.raises(RuntimeError, match="stopped"):
            firing.result(timeout=FIRE_TIMEOUT)
    finally:
        executor.shutdown(wait=False)
        if loop.is_running():
            loop.call_soon_threadsafe(loop.stop)
        loop_thread.join(FIRE_TIMEOUT)
        pending = asyncio.all_tasks(loop)
        for task in pending:
            task.cancel()
        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()


def test_a_firing_without_a_running_main_loop_fails_cleanly(bare_scheduler, recwarn):
    with pytest.raises(RuntimeError, match="not running"):
        bare_scheduler._fire(str(ObjectId()))
    assert not [warning for warning in recwarn if "never awaited" in str(warning.message)]
