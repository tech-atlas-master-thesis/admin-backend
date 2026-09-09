"""Fixtures for the registry end-to-end tests.

Nothing here fakes anything. The steps run against the real publishers (Zenodo, GLEIF,
BEV) and the real MongoDB from ``.env``; these fixtures only make sure the environment
they need is actually there, and fail fast with a readable reason when it is not.
"""

import asyncio
import os
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import pytest
import pytest_asyncio
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# The steps read their connection details from the environment at call time, exactly as
# they do under main.py, so the tests need the same .env loaded.
load_dotenv(REPO_ROOT / ".env")

from pipelineFramework import work_directory  # noqa: E402

PING_TIMEOUT = 5
# Enough headroom for the largest archive plus the one it replaces.
MINIMUM_FREE_GIB = 3

# Once MongoDB has failed to answer, every later test skips on that answer instead of
# waiting out the timeout again.
_unreachable: Optional[str] = None


def pytest_addoption(parser):
    parser.addoption(
        "--force-download",
        action="store_true",
        default=False,
        help="Set FORCE_DOWNLOAD on the download steps: fetch the archive even when its version is already active.",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "e2e: runs a pipeline step against the real registry and database")


@pytest.fixture(scope="session")
def force_download(request) -> bool:
    return bool(request.config.getoption("--force-download"))


@pytest.fixture(scope="session")
def pipeline_results() -> Dict[str, Any]:
    """The results dict a pipeline run shares between its steps.

    Running the whole module fills this as it goes, so a load step reads the result its
    download step just produced. Running a load step on its own finds it empty and falls
    back to the download the snapshot store has on record.
    """
    return {}


@pytest.fixture(scope="session")
def spool_dir() -> Path:
    """Where the download steps put archives - REGISTRY_WORK_DIR, resolved the same way they do."""
    directory = work_directory()
    free_gib = shutil.disk_usage(directory).free / (1 << 30)
    print(f"\nRegistry spool directory: {directory} ({free_gib:.1f} GiB free)", flush=True)
    if free_gib < MINIMUM_FREE_GIB:
        pytest.skip(
            f"{directory} has {free_gib:.1f} GiB free, and a registry archive needs at least {MINIMUM_FREE_GIB} GiB"
        )
    return directory


@pytest_asyncio.fixture
async def registry_database():
    """The registry database the steps write to, once it has answered a ping."""
    from pipelineFramework import get_registry_db_client

    global _unreachable
    if _unreachable:
        pytest.skip(_unreachable)

    database = get_registry_db_client()
    try:
        await asyncio.wait_for(database.command("ping"), timeout=PING_TIMEOUT)
    except Exception as error:  # noqa: BLE001 - any failure here means the same thing
        await database.client.close()
        _unreachable = (
            f'MongoDB "{os.environ.get("DB_REGISTRY_NAME")}" at {os.environ.get("MONGO_URL")} '
            f"is not reachable ({type(error).__name__}: {error})"
        )
        pytest.skip(_unreachable)

    try:
        yield database
    finally:
        await database.client.close()
