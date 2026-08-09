import datetime
import filecmp
import os

import pandas as pd
import pytest

from pipelineFramework import EventType
from pipelineFramework.tests.test_helper import get_default_user_config
from pipeline_configs.FFG_steps.ffg_scraper import FFPScraper


@pytest.mark.asyncio
async def test_steps_full_data():
    results = {}

    ffg_step = FFPScraper()
    RESULT_STEP_INDEX = 3

    i = 0
    async for event, event_type in ffg_step.run(get_default_user_config(ffg_step)):
        if i == RESULT_STEP_INDEX:
            assert event_type == EventType.RESULT
            # assert compare_with_file("./tests/results/scrape_ffg_small_data.csv", event.to_csv(index=False))
            with open("./tests/results/scrape_ffg_small_data.csv", "r", newline="\n") as f:
                assert check_string(str.strip(f.read()), str.strip(event.to_csv(index=False)))
            results["getDataFFG"] = event
        else:
            assert event_type == EventType.INFO
        i += 1


def compare_with_file(expected_file, actual):
    os.makedirs("./tests/tmp", exist_ok=True)
    actual_file = f"./tests/tmp/{datetime.datetime.now().isoformat().replace(':', '-').replace('.', '-')}"
    with open(actual_file, "x", newline="\n") as f:
        f.write(actual)
    filecmp.clear_cache()
    return filecmp.cmp(expected_file, actual_file, shallow=False)


def check_string(expect, actual):
    print([len(expect), expect[-10:-1], ord(expect[-1]), len(actual), actual[-10:-1], ord(actual[-1])])
    print([ord(i) for i in expect[-10:-1]], [ord(i) for i in actual[-10:-1]])
    if expect == actual:
        return True

    print(f"./tests/logs/{datetime.datetime.now().isoformat().replace(':', '-').replace('.', '-')}")
    os.makedirs("./tests/logs", exist_ok=True)
    with open(
        f"./tests/logs/{datetime.datetime.now().isoformat().replace(':', '-').replace('.', '-')}", "x", newline="\n"
    ) as f:
        f.write(actual)

    return False
