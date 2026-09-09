"""End-to-end tests for the registry pipeline: one test per step, nothing faked.

Each test starts the step as the pipeline would - its own HTTP session against the real
publisher, its own connection to the MongoDB from ``.env`` - and then checks two things:
the events it reported, and the state it left behind (the archive on disk, the documents
and indexes in the snapshot collection, what ``_snapshots`` says is active).

Running the module runs the pipeline in order, and each download hands its result to the
load step after it through the shared ``pipeline_results`` dict, exactly like a real run:

    pytest tests/test_registry_pipeline.py -s               # everything, in order
    pytest tests/test_registry_pipeline.py -s -k ror        # one registry
    pytest tests/test_registry_pipeline.py -s --force-download   # ignore "already active"

Use ``-s`` while developing: the steps stream their progress as they go.

A load step run on its own picks up the archive its download step left on disk, using
the record in ``_snapshots.last_download``. If there is none, it skips and says which
test to run first - the load steps delete the archive once it is in MongoDB, so a load
always needs a download in front of it.

These tests move real data: several hundred megabytes per download, and a few million
documents for BEV. Expect minutes per step, not seconds.
"""

import hashlib
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pytest

from helper.events import run_step
from pipelineFramework import SnapshotLoadStep, SnapshotStore, SnapshotSummaryStep
from pipeline_configs.registry_steps import (
    BevDownloadStep,
    BevLoadStep,
    GleifDownloadStep,
    GleifLoadStep,
    RorDownloadStep,
    RorLoadStep,
)

pytestmark = [pytest.mark.asyncio, pytest.mark.e2e]

DOWNLOADED = "downloaded"
SKIPPED_STATUSES = {"unchanged_version", "not_modified"}

# Austria's bounding box, for the coordinates BEV addresses are reprojected into.
AUSTRIA_LATITUDE = (46.3, 49.1)
AUSTRIA_LONGITUDE = (9.4, 17.2)


# --------------------------------------------------------------------------- steps


async def test_ror_download(pipeline_results, registry_database, spool_dir, force_download):
    """Resolve the newest ROR release on Zenodo and stream it into the spool directory."""
    log = await run_step(
        RorDownloadStep(),
        results=pipeline_results,
        user_config={"FORCE_DOWNLOAD": True} if force_download else None,
    )

    result = await check_download(log, source="ror", database=registry_database, spool_dir=spool_dir)
    assert result["version"], "ROR releases are versioned; an empty version would collide in the snapshot store"


async def test_ror_load(pipeline_results, registry_database):
    """Parse the ROR dump, keep the Austrian organisations and activate the snapshot."""
    step = RorLoadStep()
    download = await download_result_for(step, pipeline_results, registry_database)

    log = await run_step(step, results=pipeline_results)

    collection = await check_load(
        log,
        download=download,
        source="ror",
        database=registry_database,
        minimum_records=300,
        indexes=["search_names", "domains", "country"],
        keys=["ror_url", "names", "search_names", "location", "external_ids"],
    )
    async for document in collection.find().limit(25):
        assert document["location"]["country_code"] == "AT", f"{document['_id']} is not in Austria"
        assert document["search_names"], f"{document['_id']} has no searchable name"


async def test_gleif_download(pipeline_results, registry_database, spool_dir, force_download):
    """Fetch the newest GLEIF Level 1 concatenated file."""
    log = await run_step(
        GleifDownloadStep(),
        results=pipeline_results,
        user_config={"FORCE_DOWNLOAD": True} if force_download else None,
    )

    result = await check_download(log, source="gleif", database=registry_database, spool_dir=spool_dir)
    if result["status"] == DOWNLOADED:
        assert (
            result["bytes"] > 10 << 20
        ), "the concatenated LEI file is far larger than this - did we get an error page?"


async def test_gleif_load(pipeline_results, registry_database):
    """Load the entities whose legal address is in Austria, with their Firmenbuchnummer."""
    step = GleifLoadStep()
    download = await download_result_for(step, pipeline_results, registry_database)

    log = await run_step(step, results=pipeline_results)

    collection = await check_load(
        log,
        download=download,
        source="gleif",
        database=registry_database,
        minimum_records=500,
        indexes=["search_name", "firmenbuchnummer", "postal_code"],
        keys=["legal_name", "search_name", "legal_address", "firmenbuchnummer", "registration_authority_id"],
    )
    async for document in collection.find().limit(25):
        assert document["legal_address"]["country"] == "AT", f"{document['_id']} is not registered in Austria"

    with_firmenbuchnummer = await collection.count_documents({"firmenbuchnummer": {"$ne": None}})
    assert with_firmenbuchnummer > 0, "no Firmenbuchnummer was recognised - check the pattern against the current file"
    print(f"Firmenbuchnummer found for {with_firmenbuchnummer:,} of the Austrian entities", flush=True)


async def test_bev_download(pipeline_results, registry_database, spool_dir, force_download):
    """Fetch the BEV Adressregister, whose version comes from the server's own headers."""
    log = await run_step(
        BevDownloadStep(),
        results=pipeline_results,
        user_config={"FORCE_DOWNLOAD": True} if force_download else None,
    )

    await check_download(log, source="bev", database=registry_database, spool_dir=spool_dir)


async def test_bev_load(pipeline_results, registry_database):
    """Join the BEV tables and reproject every address from Gauss-Krueger to WGS84."""
    step = BevLoadStep()
    download = await download_result_for(step, pipeline_results, registry_database)

    log = await run_step(step, results=pipeline_results)

    collection = await check_load(
        log,
        download=download,
        source="bev",
        database=registry_database,
        minimum_records=1_000_000,
        indexes=["search_key", "postal_code_street", "street_key"],
        keys=["street", "postal_code", "city", "state", "lat", "lon", "source_crs", "search_key"],
    )
    async for document in collection.find().limit(25):
        assert AUSTRIA_LATITUDE[0] < document["lat"] < AUSTRIA_LATITUDE[1], f"{document['_id']} is not in Austria"
        assert AUSTRIA_LONGITUDE[0] < document["lon"] < AUSTRIA_LONGITUDE[1], f"{document['_id']} is not in Austria"
        assert document["source_crs"].startswith("EPSG:"), "the source CRS must stay with the point"


async def test_registry_summary(pipeline_results, registry_database):
    """Report which snapshot each registry currently resolves to."""
    log = await run_step(SnapshotSummaryStep(), results=pipeline_results)
    log.assert_step_contract()

    summary = log.result
    assert summary == await SnapshotStore(registry_database).describe(), "the summary is out of step with _snapshots"

    if not summary:
        pytest.skip("no registry snapshot is active yet - run the download and load steps first")

    for source, state in sorted(summary.items()):
        if state["collection"] is None:
            continue
        counted = await registry_database[state["collection"]].count_documents({})
        assert counted == state["records"], f"{source} reports {state['records']:,} records but holds {counted:,}"
        log.assert_narrative(rf"{source}: version .* records in {state['collection']}")


# ------------------------------------------------------------------------- checks


async def check_download(log, *, source: str, database, spool_dir: Path) -> Dict[str, Any]:
    """Assert what a download step must report and leave behind, whichever branch it took."""
    log.assert_step_contract()
    log.assert_narrative(rf'Upstream version for "{source}"')

    result = log.result
    assert result["source"] == source
    assert result["version"], "the version keys the snapshot, so it can never be empty"
    assert result["url"].startswith("http")
    assert result["status"] in {DOWNLOADED, *SKIPPED_STATUSES}, result["status"]

    state = await SnapshotStore(database).get(source)
    assert state is not None, "a download always leaves a trace in _snapshots"

    if result["status"] != DOWNLOADED:
        # Nothing to fetch: that is only correct when something is already serving.
        assert state.active is not None, f'"{source}" reported {result["status"]} without an active snapshot'
        assert "path" not in result, "no file was written, so the result must not point at one"
        print(f"{source}: {result['status']} (version {result['version']} is already active)", flush=True)
        return result

    log.assert_narrative(r"Downloading http", r"Downloaded .* to ")

    archive = Path(result["path"])
    assert archive.exists(), f"{archive} is missing although the step reported a download"
    assert archive.parent == spool_dir, f"the archive landed in {archive.parent}, not in the spool directory"
    assert archive.stat().st_size == result["bytes"] > 0, "the reported size does not match the file"
    assert sha256_of(archive) == result["sha256"], "the reported checksum does not match the file"
    assert not list(spool_dir.glob("*.part")), "a partial download was left behind"

    record = state.last_download
    assert record is not None and record.version == result["version"]
    assert record.path == result["path"] and record.sha256 == result["sha256"]
    return result


async def check_load(
    log,
    *,
    download: Dict[str, Any],
    source: str,
    database,
    minimum_records: int,
    indexes: Iterable[str],
    keys: Iterable[str],
):
    """Assert the snapshot a load step activated, and hand back its collection."""
    log.assert_step_contract()
    result = log.result
    assert result["source"] == source

    if download["status"] != DOWNLOADED:
        assert result == {"source": source, "status": "skipped", "version": download["version"]}
        pytest.skip(
            f"{source} had nothing new to load ({download['status']}); "
            f"rerun with --force-download to exercise the load step"
        )

    assert result["status"] == "loaded"
    assert result["collection"] == SnapshotStore.collection_name(source, result["version"])

    collection = database[result["collection"]]
    counted = await collection.count_documents({})
    assert counted == result["records"], f"the step reported {result['records']:,} documents but stored {counted:,}"
    assert (
        counted >= minimum_records
    ), f"only {counted:,} documents for {source} - the parser probably no longer matches the published layout"

    built = await collection.index_information()
    assert set(indexes) <= set(
        built
    ), f"missing index(es) {sorted(set(indexes) - set(built))} on {result['collection']}"

    document = await collection.find_one()
    missing: List[str] = [key for key in keys if key not in document]
    assert not missing, f"documents in {result['collection']} are missing {missing}: {sorted(document)}"

    active = await SnapshotStore(database).require_active(source)
    assert active.collection == result["collection"] and active.records == result["records"]

    assert not Path(download["path"]).exists(), "the archive should be discarded once it is in MongoDB"
    existing = await database.list_collection_names()
    for dropped in result["pruned"]:
        assert dropped not in existing, f"{dropped} was reported as pruned but is still there"
    print(f"{source}: {counted:,} documents in {result['collection']} (replaced {result['replaced']})", flush=True)
    return collection


# ------------------------------------------------------------------------ helpers


async def download_result_for(step: SnapshotLoadStep, results: Dict[str, Any], database) -> Dict[str, Any]:
    """The download result this load step needs, from this session or from the last run.

    Not a stand-in for the download: it is the record the download step wrote, pointing
    at the archive it actually left on disk. Without one there is nothing to load, so the
    test skips rather than failing.
    """
    name = step.download_step.name()
    if name in results:
        return results[name]

    state = await SnapshotStore(database).get(step.source)
    record = state.last_download if state else None
    if record and record.path and Path(record.path).exists():
        print(f"Reusing the archive {name} left behind: {record.path}", flush=True)
        results[name] = {
            "source": step.source,
            "status": DOWNLOADED,
            "version": record.version,
            "url": record.url,
            "path": record.path,
            "bytes": record.bytes,
            "sha256": record.sha256,
            "etag": record.etag,
            "last_modified": record.last_modified,
            "downloaded_at": record.downloaded_at.isoformat(),
        }
        return results[name]

    pytest.skip(
        f"no archive on disk for {step.source} - run test_{step.source}_download first "
        f"(with --force-download if its version is already active)"
    )


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()
