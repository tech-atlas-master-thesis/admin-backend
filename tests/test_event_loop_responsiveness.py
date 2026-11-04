import asyncio
import json
import logging
import random
import string
import threading
import time

import pytest

from pipelineFramework import SnapshotLoadStep
from pipelineFramework.server.pipeline.loop_monitor import STEP_TASK_PREFIX, monitor_event_loop
from pipelineFramework.server.pipeline.step import Step
from pipelineFramework.server.registries import load_step
from registries import matching, ror


def candidate(text, identity=None):
    return text, identity or text, {"_id": identity or text}


def random_name(rng):
    return " ".join("".join(rng.choices("abcdefgh", k=rng.randint(3, 7))) for _ in range(rng.randint(1, 4)))


@pytest.mark.parametrize(
    "threshold,margin",
    [(90.0, 4.0), (70.0, 0.0), (50.0, 10.0), (0.0, 0.0)],
)
def test_best_matches_decides_exactly_like_best_match(threshold, margin):
    rng = random.Random(11)
    # A small alphabet produces many near and exactly tied scores, and the shared identities
    # exercise the rule that compares the runner-up by identity rather than by position.
    pool = [candidate(random_name(rng), identity=str(index % 150)) for index in range(300)]
    query_lists = []
    for _ in range(120):
        text = rng.choice(pool)[0]
        words = text.split()
        rng.shuffle(words)
        query_lists.append([" ".join(words) + rng.choice(["", " a", "b"]), random_name(rng)])

    expected = [matching.best_match(queries, pool, threshold, margin) for queries in query_lists]
    assert any(expected)
    assert matching.best_matches(query_lists, pool, threshold, margin, chunk_size=7) == expected


def test_best_matches_keeps_only_the_top_candidates_like_best_match():
    # More exact ties than TOP_CANDIDATES: which ones survive decides the runner-up.
    pool = [candidate("same name", identity=str(index)) for index in range(25)]
    query_lists = [["same name"]]
    assert matching.best_matches(query_lists, pool, 90.0, 0.0) == [matching.best_match(["same name"], pool, 90.0, 0.0)]


def test_best_matches_returns_one_result_per_record_when_nothing_can_match():
    pool = [candidate("technische universitaet wien")]
    assert matching.best_matches([[], ["technische universitaet wien"], []], pool, 90.0, 4.0) == [
        None,
        (pool[0][2], 100.0),
        None,
    ]
    assert matching.best_matches([["anything"], []], [], 90.0, 4.0) == [None, None]
    assert matching.best_matches([], pool, 90.0, 4.0) == []


@pytest.mark.asyncio
async def test_best_matches_does_not_stall_the_event_loop():
    rng = random.Random(3)
    letters = string.ascii_lowercase
    pool = [candidate(" ".join("".join(rng.choices(letters, k=8)) for _ in range(4))) for _ in range(30_000)]
    query_lists = [[pool[index][0][:-1]] for index in range(1000)]

    task = asyncio.create_task(asyncio.to_thread(matching.best_matches, query_lists, pool, 90.0, 4.0))
    longest = 0.0
    while not task.done():
        started = time.perf_counter()
        await asyncio.sleep(0.005)
        longest = max(longest, time.perf_counter() - started)

    assert all(found is not None for found in task.result())
    assert longest < 0.25


@pytest.mark.asyncio
async def test_monitor_reports_a_stall_and_the_running_step(caplog):
    async def blocking_step():
        asyncio.current_task().set_name(f"{STEP_TASK_PREFIX}slow_step")
        await asyncio.sleep(0.02)
        time.sleep(0.15)
        await asyncio.sleep(0.02)

    monitor = asyncio.create_task(monitor_event_loop(interval=0.01, threshold=0.1))
    with caplog.at_level(logging.WARNING):
        await asyncio.create_task(blocking_step())
    monitor.cancel()

    assert any("stalled" in message and "slow_step" in message for message in caplog.messages)


def test_ror_documents_are_streamed_with_the_same_content_as_a_full_parse(tmp_path):
    records = [
        {
            "id": f"https://ror.org/{index}",
            "status": "active",
            "names": [{"value": f"Universität {index}", "types": ["ror_display"], "lang": "de"}],
            "locations": [
                {"geonames_details": {"lat": 48.2081743, "lng": 16.3738189, "name": "Wien", "country_code": "AT"}}
            ],
            "links": [{"type": "website", "value": "https://example.at"}],
            "external_ids": [],
        }
        for index in range(3)
    ]
    path = tmp_path / "ror.json"
    path.write_text(json.dumps(records), encoding="utf-8")

    assert list(ror.iter_documents(path)) == [ror.to_document(record) for record in records]


class FakeCollection:
    def __init__(self):
        self.batches = []

    async def insert_many(self, documents, ordered):
        self.batches.append(len(documents))


class FakeDatabase:
    def __init__(self):
        self.collection = FakeCollection()

    async def list_collection_names(self):
        return []

    def __getitem__(self, _name):
        return self.collection


class FakeStore:
    def __init__(self, _database):
        pass

    def collection_name(self, source, version):
        return f"{source}_{version}"

    async def get(self, _source):
        return None

    async def activate(self, *_):
        pass

    async def prune(self, _source, keep):
        return []


class FakeDownload:
    @staticmethod
    def name():
        return "fake_download"


class FakeLoad(SnapshotLoadStep):
    source = "fake"
    download_step = FakeDownload
    BATCH_SIZE = 3
    delete_archive = False
    parsed_on = set()

    @staticmethod
    def name():
        return "fake_load"

    @staticmethod
    def display_name():
        return None

    def iter_documents(self, path):
        for index in range(7):
            self.parsed_on.add(threading.current_thread())
            yield {"_id": index}


@pytest.mark.asyncio
async def test_load_step_parses_off_the_loop_and_keeps_the_batches(tmp_path, monkeypatch):
    database = FakeDatabase()
    monkeypatch.setattr(load_step, "get_registry_db_client", lambda: database)
    monkeypatch.setattr(load_step, "SnapshotStore", FakeStore)
    archive = tmp_path / "archive.zip"
    archive.touch()
    download = {"status": "downloaded", "path": str(archive), "version": "1"}

    events = [event async for event in FakeLoad().run(results={"fake_download": download})]

    assert database.collection.batches == [3, 3, 1]
    assert events[-1][0]["records"] == 7
    assert threading.current_thread() not in FakeLoad.parsed_on


def test_serialized_result_is_previewed_only_when_large():
    data, preview, preview_data, _ = Step._serialize_result({"a": 1})
    assert (data, preview, preview_data) == ('{\n  "a": 1\n}', False, None)

    data, preview, preview_data, _ = Step._serialize_result({str(index): "x" * 50 for index in range(100)})
    assert preview and data and preview_data.startswith("100 entries in total")
