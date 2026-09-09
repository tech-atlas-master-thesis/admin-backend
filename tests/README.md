# Registry pipeline tests

End-to-end tests for the registry steps: one test per pipeline step, each starting the
step exactly as the pipeline would. Nothing is mocked - the steps download from Zenodo,
GLEIF and BEV, and write into the MongoDB configured in `.env`.

## Running them

```bash
pytest tests/test_registry_pipeline.py -s                     # the whole pipeline, in order
pytest tests/test_registry_pipeline.py -s -k ror              # one registry
pytest tests/test_registry_pipeline.py::test_bev_load -s      # one step
pytest tests/test_registry_pipeline.py -s --force-download    # ignore "version already active"
```

Use `-s`. The steps stream their progress (`Downloaded 256.0 MiB / 1.2 GiB (21%)`,
`Inserted 250,000 documents`) and without `-s` pytest swallows it until the end.

This moves real data: a few hundred megabytes per download, a few million documents for
BEV. Minutes per step, not seconds.

Prerequisites, each checked before anything is downloaded:

- **MongoDB** from `.env` (`MONGO_URL`, `DB_REGISTRY_*`). Unreachable, and every test
  skips within seconds naming the host it tried.
- **Disk** in `REGISTRY_WORK_DIR`, at least 3 GiB free.
- **Network** to the three publishers.

## What each test checks

1. **Start the step as is** - `helper/events.py:run_step` does what `Step.run` does
   around it: pass the user config the server would send, hand down the shared `results`
   dict, and file the step's RESULT under its own name. No patching of any kind.
2. **The events it reported.** Automatic contract on every run: each yield is a
   `(payload, EventType)` pair, message events carry real text, there is exactly one
   RESULT, it comes last, no ERROR events, and the payload survives `json.dumps` -
   which the runtime does when storing it, so an unserialisable result would fail a
   real run after the work is done. On top of that, `assert_narrative(...)` checks the
   phases the step reports, as ordered regular expressions with anything allowed in
   between, so an extra log line never breaks a test.
3. **The state it left behind.** For a download: the archive in the spool directory,
   its size and sha256 matching the result, no leftover `.part`, and the matching
   `last_download` record in `_snapshots`. For a load: the collection holding exactly as
   many documents as the result claims, the expected indexes, a plausible document shape
   (Austrian country code, coordinates inside Austria, a Firmenbuchnummer on at least
   some GLEIF entities), the active snapshot pointing at it, the pruned collections
   really gone, and the archive deleted.

The record counts are asserted against low floors (ROR 300, GLEIF 500, BEV 1,000,000).
They are not there to pin down a number that grows every release - they catch the
failure that matters, a parser that still runs but no longer matches the published
layout and quietly loads almost nothing.

## Download and load are separate steps

A load step reads its download step's result. Run the module and it gets it directly,
through the same `results` dict a real run uses. Run a load step on its own and it picks
up the archive its download step left on disk, via the `last_download` record in
`_snapshots` - the real record of the real file, not a stand-in. If the archive is not
there it skips and says which test to run first; the load steps delete the archive once
it is in MongoDB, so a load always needs a download in front of it.

Second runs are cheap by design: a download whose version is already active reports
`unchanged_version` without fetching anything, and the load step after it then reports
`skipped`. Both are correct, and the tests assert them as such - the load test skips
with a note to add `--force-download` when you want the full path exercised.

## Layout

```
tests/
  conftest.py                  .env loading, MongoDB check, spool directory, --force-download
  test_registry_pipeline.py    one test per step, in pipeline order
  helper/events.py             starts a step and collects/asserts its events
```

Adding a step means adding one test next to the others, plus its expectations
(minimum records, index names, document keys) to the `check_load` call.
