"""Paper-only bootstrap context for M7 execution sandbox."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from libs.execution.artifacts import ExecutionArtifactStore
from libs.planning.artifacts import PlanningArtifactStore


@dataclass
class PaperExecutionContext:
    project_root: Path
    planning_store: PlanningArtifactStore
    execution_store: ExecutionArtifactStore
    send_order_called: bool = False


class PaperExecutionBootstrap:
    def __init__(self, project_root: Path) -> None:
        self.project_root = project_root.resolve()

    def bootstrap(self) -> PaperExecutionContext:
        return PaperExecutionContext(
            project_root=self.project_root,
            planning_store=PlanningArtifactStore(self.project_root),
            execution_store=ExecutionArtifactStore(self.project_root),
        )
