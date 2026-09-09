"""Run a pipeline step the way the runtime runs it, and keep what it yielded.

These are end-to-end tests, so a step is started with nothing patched: it opens its own
HTTP session against the real publisher and its own MongoDB connection from the
environment. The only thing this module adds is what ``Step.run`` would otherwise do
around it - supply the user config, hand down the shared ``results`` dict, and file the
step's RESULT under its name so the next step reads it exactly as it would in a run.

Events are printed as they arrive (visible with ``pytest -s``), because a step that
streams a few hundred megabytes should be watchable while it works.

Assertions on the event stream come in two flavours:

* :meth:`EventLog.assert_step_contract` - what every step must satisfy, including the
  invariants the runtime depends on: one RESULT, last, and JSON serialisable, because
  ``Step._save_result`` stores it with ``json.dumps``.
* :meth:`EventLog.assert_narrative` - the phases the step is expected to report, as
  ordered regular expressions with anything allowed in between, so a step that logs one
  more line does not fail the test.
"""

import json
import re
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Type

from pipelineFramework import EventType, PipelineDummy, StepConfig, StepDummy
from pipelineFramework.server.dto.serialisation import custom_json_encoder
from pipelineFramework.tests.test_helper import get_default_user_config

MESSAGE_EVENTS = (EventType.INFO, EventType.WARNING, EventType.ERROR, EventType.DEBUG)


@dataclass(frozen=True)
class RecordedEvent:
    index: int
    type: EventType
    payload: Any
    elapsed: float

    @property
    def message(self) -> str:
        return self.payload if isinstance(self.payload, str) else repr(self.payload)

    def __str__(self) -> str:
        return f"[{self.elapsed:7.1f}s] {self.type.value:<7} {self.message}"


class EventLog:
    def __init__(self, step: StepConfig):
        self.step = step
        self.events: List[RecordedEvent] = []
        self.started = time.monotonic()
        self.finished: Optional[float] = None

    @property
    def duration(self) -> float:
        return (self.finished or time.monotonic()) - self.started

    # -- collecting ----------------------------------------------------------

    def record(self, event: Any, *, echo: bool = True) -> RecordedEvent:
        if not isinstance(event, tuple) or len(event) != 2:
            raise AssertionError(f"{self._name()} yielded {event!r}; steps must yield (payload, EventType) pairs")
        payload, event_type = event
        if not isinstance(event_type, EventType):
            raise AssertionError(f"{self._name()} yielded event type {event_type!r}, which is not an EventType")

        recorded = RecordedEvent(len(self.events), event_type, payload, time.monotonic() - self.started)
        self.events.append(recorded)
        if echo:
            print(recorded, flush=True)
        return recorded

    # -- reading -------------------------------------------------------------

    def of_type(self, *types: EventType) -> List[RecordedEvent]:
        return [event for event in self.events if event.type in types]

    @property
    def messages(self) -> List[str]:
        return [event.message for event in self.of_type(*MESSAGE_EVENTS)]

    @property
    def infos(self) -> List[str]:
        return [event.message for event in self.of_type(EventType.INFO)]

    @property
    def warnings(self) -> List[str]:
        return [event.message for event in self.of_type(EventType.WARNING)]

    @property
    def errors(self) -> List[str]:
        return [event.message for event in self.of_type(EventType.ERROR)]

    @property
    def results(self) -> List[Any]:
        return [event.payload for event in self.of_type(EventType.RESULT)]

    @property
    def result(self) -> Any:
        results = self.results
        assert len(results) == 1, f"expected exactly one RESULT event, got {len(results)}\n{self.describe()}"
        return results[0]

    def describe(self) -> str:
        body = "\n".join(str(event) for event in self.events) or "    <no events>"
        return f"--- {self._name()} ({self.duration:.1f}s) ---\n{body}\n--- end ---"

    # -- asserting -----------------------------------------------------------

    def assert_step_contract(self, *, expect_result: bool = True, allow_warnings: bool = True) -> "EventLog":
        """Invariants that hold for every step, whatever it does."""
        for event in self.of_type(*MESSAGE_EVENTS):
            assert (
                isinstance(event.payload, str) and event.payload.strip()
            ), f"{event.type.value} events carry a human readable message, got {event.payload!r}\n{self.describe()}"

        assert not self.errors, f"step reported ERROR events\n{self.describe()}"
        if not allow_warnings:
            assert not self.warnings, f"step reported unexpected WARNING events\n{self.describe()}"

        results = self.of_type(EventType.RESULT)
        if not expect_result:
            assert not results, f"expected no RESULT event\n{self.describe()}"
            return self

        assert len(results) == 1, f"a step reports exactly one RESULT, got {len(results)}\n{self.describe()}"
        assert results[0] is self.events[-1], f"the RESULT is the last thing a step yields\n{self.describe()}"

        # The runtime stores results with json.dumps(..., default=custom_json_encoder);
        # a payload it cannot encode fails the pipeline after the step already did the work.
        try:
            json.dumps(results[0].payload, default=custom_json_encoder)
        except TypeError as error:
            raise AssertionError(f"the RESULT payload is not JSON serialisable: {error}\n{self.describe()}")
        return self

    def assert_narrative(self, *patterns: str) -> "EventLog":
        """Assert these patterns appear in the messages, in this order, others allowed in between."""
        remaining = list(self.messages)
        for pattern in patterns:
            expression = re.compile(pattern)
            for position, message in enumerate(remaining):
                if expression.search(message):
                    remaining = remaining[position + 1 :]
                    break
            else:
                raise AssertionError(f'no message matching "{pattern}" after the previous one\n{self.describe()}')
        return self

    def assert_never(self, *patterns: str) -> "EventLog":
        for pattern in patterns:
            expression = re.compile(pattern)
            offenders = [message for message in self.messages if expression.search(message)]
            assert not offenders, f'unexpected message(s) matching "{pattern}": {offenders}\n{self.describe()}'
        return self

    def _name(self) -> str:
        return type(self.step).__name__


def user_config_for(step: StepConfig, overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """The defaults the server would send for this step, with the run's overrides applied."""
    config = get_default_user_config(step)
    unknown = set(overrides or {}) - set(config)
    assert not unknown, f"{type(step).__name__} declares no user config named {sorted(unknown)}"
    config.update(overrides or {})
    return config


async def run_step(
    step: StepConfig,
    *,
    results: Optional[Dict[str, Any]] = None,
    user_config: Optional[Dict[str, Any]] = None,
    echo: bool = True,
) -> EventLog:
    """Start the step for real and collect its events.

    ``results`` is the dict a pipeline hands down between steps. The step's RESULT is
    stored there under its own name, just like ``Step.run`` does, so a later step in the
    same test session reads it the way it would in a real run.
    """
    config = user_config_for(step, user_config)
    shared: Dict[str, Any] = results if results is not None else {}
    log = EventLog(step)
    if echo:
        print(f"\n=== {step.name()} ({type(step).__name__}) ===", flush=True)

    try:
        async for event in step.run(
            user_config=config,
            results=shared,
            pipeline=PipelineDummy(),
            step=StepDummy(),
            warnings=[],
        ):
            recorded = log.record(event, echo=echo)
            if recorded.type == EventType.RESULT:
                shared[step.name()] = recorded.payload
    finally:
        log.finished = time.monotonic()
        if echo:
            print(f"=== {step.name()} finished in {log.duration:.1f}s ===\n", flush=True)
    return log


async def run_step_raising(
    exception: Type[BaseException],
    step: StepConfig,
    *,
    results: Optional[Dict[str, Any]] = None,
    user_config: Optional[Dict[str, Any]] = None,
    match: Optional[str] = None,
) -> Tuple[BaseException, EventLog]:
    """Run a step that is expected to fail, keeping the events it emitted before it did."""
    log = EventLog(step)
    shared: Dict[str, Any] = results if results is not None else {}
    raised: Optional[BaseException] = None

    try:
        async for event in step.run(
            user_config=user_config_for(step, user_config),
            results=shared,
            pipeline=PipelineDummy(),
            step=StepDummy(),
            warnings=[],
        ):
            log.record(event)
    except exception as error:  # noqa: B902 - the type is what the test expects
        raised = error
    finally:
        log.finished = time.monotonic()

    assert raised is not None, f"expected {exception.__name__}, but the step finished\n{log.describe()}"
    if match:
        assert re.search(match, str(raised)), f'"{raised}" does not match "{match}"'
    return raised, log
