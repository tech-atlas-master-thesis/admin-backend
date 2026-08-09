from typing import Optional, List, Union

from fastapi import HTTPException

from pipelineFramework import (
    StepConfig,
    UserStepConfig,
    StepUserConfig,
    LocalisationString,
    LocalisationStringType,
    ConfigurationManager,
    EventType,
)


class TechConfigStep(StepConfig):
    async def run(self, user_config: Optional[UserStepConfig], results, **_):
        if user_config is None:
            raise HTTPException(status_code=404, detail="User config not provided")
        version = self.get_version(user_config)
        yield version.configuration, EventType.RESULT

    def get_version(self, user_config: UserStepConfig):
        user_input = user_config.get("getTechnologyConfiguration")
        if user_input is None:
            raise HTTPException(status_code=404, detail="User config not found")
        configuration_id = user_input.get("configuration")
        version_id = user_input.get("version")
        if configuration_id is None:
            raise HTTPException(status_code=404, detail="Configuration not provided")
        manager = ConfigurationManager([])
        if version_id is None:
            return manager.get_latest_version(configuration_id)
        return manager.get_version(configuration_id, version_id)

    def user_config(self) -> List[StepUserConfig]:
        return [
            StepUserConfig(
                "getTechnologyConfiguration",
                LocalisationString("Technology Configuration", "Technologie Konfiguration"),
                None,
                StepUserConfig.StepUserConfigType.CONFIGURATION,
                configurationType="technologies",
            )
        ]

    def name(self) -> str:
        return "tech_config_step"

    def display_name(self) -> LocalisationStringType:
        return LocalisationString("Get Technology Configuration", "Technologie Konfiguration Laden")

    def description(self) -> LocalisationStringType:
        return LocalisationString("Desc", "Desc")

    def dependencies(self) -> Union[List[str], None]:
        return None
