from dataclasses import dataclass
from typing import Dict, Any, Optional, Callable

from bson import ObjectId

from pipelineFramework import AuditInfoDto


def _get(obj: Dict[str, Any], key: str, transformer: Optional[Callable[[Any], Any]] = None) -> Any:
    if key not in obj:
        return None
    value = obj[key]
    if transformer:
        return transformer(value)
    return value


@dataclass
class DatasetDto:
    id: str
    pipeline: str
    pipelineType: str
    pipelineName: str
    active: bool
    created: AuditInfoDto

    @classmethod
    def from_entity(cls, entity: Dict):
        return cls(
            _get(entity, "_id", str),
            _get(entity, "pipeline", str),
            _get(entity, "pipelineType"),
            _get(entity, "pipelineName"),
            _get(entity, "active", bool),
            _get(entity, "created"),
        )

    def to_entity(self) -> Dict:
        return {
            "_id": ObjectId(self.id),
            "pipeline": ObjectId(self.pipeline),
            "pipelineType": self.pipelineType,
            "pipelineName": self.pipelineName,
            "active": self.active,
            "created": self.created,
        }
