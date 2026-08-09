import datetime
import filecmp
import os

import pytest

from pipelineFramework import EventType
from pipelineFramework.tests.test_helper import get_default_user_config
from pipeline_configs.FWF_steps.fwf_scraper import FWFScraper


@pytest.mark.asyncio
async def test_steps_full_data():
    results = {
        "getTechnologyConfiguration": [
            {"label": "Künstliche Intelligenz", "technologies": [{"label": "Maschinelles Lernen", "searchTerm": []}]}
        ]
    }

    fwf = FWFScraper()
    RESULT_STEP_INDEX = 3
    WARNING_STEPS = [1]

    i = 0
    async for event, event_type in fwf.run(user_config=get_default_user_config(fwf), results=results):
        if i == RESULT_STEP_INDEX:
            assert event_type == EventType.RESULT
            with open("./tests/results/scrape_fwf_small_data.csv", "r", newline="\n") as f:
                assert check_string(str.strip(f.read()), str.strip(event.to_csv(index=False)))
            results["getDataFWF"] = event
        elif i in WARNING_STEPS:
            print(f"Warning is being ignored\n{event}")
            assert event_type == EventType.WARNING
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
