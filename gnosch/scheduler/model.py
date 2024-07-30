from dataclasses import dataclass
from typing import Optional

# NOTE switch to pydantic for easier serde?
# NOTE introduce a completely custom resource spec...


@dataclass
class TaskInput:
	dataset_id: str
	# NOTE add in flags like 'allow_lazy', 'read_only'. And 'required_at_pct' -- or rather encourage smaller jobs?


@dataclass
class TaskOutput:
	dataset_id: str  # NOTE similar flags like for TaskInput
	size_mb: int


@dataclass
class Task:
	"""Schedulable atom. Will run as a standalone process on a single worker"""

	# dependencies
	inputs: list[TaskInput]

	# provides
	outputs: list[TaskOutput]

	# resources
	memory_mb: int
	prefered_cpus: int  # NOTE add in maximum cpus?
	runtime_est_s: int

	@property
	def cpusecs(self) -> int:
		return self.runtime_est_s * self.prefered_cpus


TaskId = str
TaskGraph = dict[TaskId, Task]
NodeName = str


@dataclass
class NodeSpec:
	cpus: int
	memory_mb: int


@dataclass
class ClusterSpec:
	nodes: dict[NodeName, NodeSpec]
	comm_mbps: dict[tuple[NodeName, NodeName], float]


@dataclass
class SchedulingCommand:
	"""An Either[...] command"""

	fetch_dataset: Optional[str] = None
	drop_dataset: Optional[str] = None  # TODO this needs some [task_done] understanding...
	launch_task: Optional[TaskId] = None
	# TODO constructor validation


"""The controller is supposed to follow the schedule by issuing the first command in the queue for a node
whenever the node has (memory) capacity and in case of fetch_dataset it is already available somewhere."""
Schedule = dict[NodeName, list[SchedulingCommand]]
