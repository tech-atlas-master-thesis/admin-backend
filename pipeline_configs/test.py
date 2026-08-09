import asyncio
from typing import Union, List

from pipelineFramework import LocalisationString, PipelineConfig, StepConfig, EventType


class TestStep(StepConfig):
    async def run(self, **_):
        yield "Sleep for 5 second", EventType.INFO
        await asyncio.sleep(5)
        print("This is a test step")

    def name(self) -> str:
        return "test"

    def display_name(self):
        return LocalisationString("Step 1", "Schritt 1")

    def dependencies(self) -> Union[List[str], None]:
        return None


class TestStep2(StepConfig):
    async def run(self, **_):
        yield "Sleep for 15 second", EventType.INFO
        await asyncio.sleep(15)
        yield "Test Warning", EventType.WARNING
        await asyncio.sleep(20)
        print("This is another test step")

    def name(self) -> str:
        return "test2"

    def display_name(self):
        return LocalisationString("Step 2", "Schritt 2")

    def dependencies(self) -> Union[List[str], None]:
        return None


TEST_PIPELINE = PipelineConfig(
    type="test", display_name="Test Pipeline", steps=[TestStep(), TestStep2()], parallelize=True
)
