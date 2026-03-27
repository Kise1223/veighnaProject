"""Paper-only bootstrap context for M8 shadow sessions."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from libs.execution.artifacts import ExecutionArtifactStore
from libs.execution.shadow_artifacts import ShadowArtifactStore
from libs.planning.artifacts import PlanningArtifactStore


@dataclass
class ShadowSessionContext:
    project_root: Path
    planning_store: PlanningArtifactStore
    execution_store: ExecutionArtifactStore
    shadow_store: ShadowArtifactStore
    send_order_called: bool = False


class ShadowSessionBootstrap:
    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root.resolve()

    def bootstrap(self) -> ShadowSessionContext:
        return ShadowSessionContext(
            project_root=self.project_root,
            planning_store=PlanningArtifactStore(self.project_root),
            execution_store=ExecutionArtifactStore(self.project_root),
            shadow_store=ShadowArtifactStore(self.project_root),
        )

