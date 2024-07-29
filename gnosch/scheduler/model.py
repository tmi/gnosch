from dataclasses import dataclass

# NOTE switch to pydantic for easier serde?
# NOTE introduce a completely custom resource spec...


@dataclass
class TaskInput:
	dataset_id: str
	# NOTE add in flags like 'allow_lazy', 'read_only', ... Or 'required_at_pct', ...


@dataclass
class TaskOutput:
	dataset_id: str
	# NOTE similar flags like for TaskInput


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


TaskId = str
TaskGraph = dict[TaskId, Task]
NodeName = str


@dataclass
class NodeSpec:
	cpus: int
	memory_mb: int


@dataclass
class Cluster:
	nodes: dict[NodeName, NodeSpec]
	comm_mbps: dict[tuple[NodeName, NodeName], float]


@dataclass
class SchedulingConditions:
	# host
	target_node: NodeName

	# preconditions (at host)
	tasks_completed: set[TaskId]
	datasets_available: set[str]

	# NOTE this is very rigid (we cant say "if either of two tasks completes, launch another one")
	# NOTE this is rather incomplete -- we don't prescribe when to copy a dataset, and where from
	# NOTE this is rather incomplete -- we don't prescribe cpu2task allocation


Schedule = dict[TaskId, SchedulingConditions]
