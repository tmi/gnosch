"""
Given a schedule and cluster spec, simulate how long it would, in theory, run.
"""

import math
import logging
from dataclasses import dataclass
from gnosch.scheduler.model import ClusterSpec, Schedule, SchedulingCommand, TaskGraph, NodeSpec, TaskId, Task, NodeName

logger = logging.getLogger(__name__)


@dataclass
class NodeState:
	# TODO unify with controller's representation
	# TODO either decouple data from behav, or remove dataclass
	spec: NodeSpec
	dataset_sizes: dict[str, int]
	task_spec: dict[TaskId, Task]
	completion_pct: dict[TaskId, float]

	def remaining_mem_mb(self) -> int:
		# NOTE perhaps cache
		return self.spec.memory_mb - sum(self.dataset_sizes.values()) - sum(e.memory_mb for e in self.task_spec.values())

	def task_done_in_s(self) -> float:
		next_task = float("inf")
		if not self.completion_pct:
			return next_task
		cpu_per_task = self.spec.cpus / len(self.completion_pct)
		for task_id, current_pc in self.completion_pct.items():
			task = self.task_spec[task_id]
			remaining_cpusecs = (1 - current_pc) * task.cpusecs
			until_done = remaining_cpusecs / cpu_per_task
			next_task = min(until_done, next_task)
		return next_task

	def progress_for(self, secs: float) -> dict[str, int]:
		finished_tasks: set[str] = set()
		if not self.completion_pct:
			return {}
		cpu_per_task = self.spec.cpus / len(self.completion_pct)
		elapsed_cpusecs = secs * cpu_per_task
		for task_id in self.completion_pct:
			task = self.task_spec[task_id]
			progress = elapsed_cpusecs / task.cpusecs
			self.completion_pct[task_id] += progress
			if math.isclose(self.completion_pct[task_id], 1.0):
				finished_tasks.add(task_id)
		datasets = {output.dataset_id: output.size_mb for task in finished_tasks for output in self.task_spec[task].outputs}
		self.dataset_sizes.update(datasets)
		for task_id in finished_tasks:
			self.completion_pct.pop(task_id)
			self.task_spec.pop(task_id)
		return datasets

	def enque(self, commands: list[SchedulingCommand], available_datasets: dict[str, int], task_specs: TaskGraph) -> int:
		enqueued = 0
		while True:
			if enqueued >= len(commands):
				break
			candidate = commands[enqueued]
			if candidate.fetch_dataset:
				ds = available_datasets.get(candidate.fetch_dataset, None)
				if ds and self.remaining_mem_mb() >= ds:
					self.dataset_sizes[candidate.fetch_dataset] = ds
					enqueued += 1
				else:
					break
			elif candidate.drop_dataset:
				self.dataset_sizes.pop(candidate.drop_dataset)
				enqueued += 1
			elif candidate.launch_task:
				task = task_specs[candidate.launch_task]
				deps = set(inp.dataset_id for inp in task.inputs)
				miss = set(self.dataset_sizes.keys()) - deps
				if not miss and task.memory_mb <= self.remaining_mem_mb():
					self.task_spec[candidate.launch_task] = task
					self.completion_pct[candidate.launch_task] = 0.0
					enqueued += 1
				else:
					break
		return enqueued


def simulate(cluster_spec: ClusterSpec, task_graph: TaskGraph, schedule: Schedule) -> float:
	"""Always returns time estimate, does not account for memory crashes or swapping slowdowns."""
	# TODO does yet not account for internode comms

	cluster = {
		node_id: NodeState(spec=node_spec, dataset_sizes={}, task_spec={}, completion_pct={})
		for node_id, node_spec in cluster_spec.nodes.items()
	}
	current_time = 0.0
	progress: dict[NodeName, int] = {node_id: 0 for node_id in schedule}
	remaining = sum(len(commands) for commands in schedule.values())
	datasets: dict[str, int] = {}  # TODO include set[NodeName] as well?

	while True:
		next_event = min(node.task_done_in_s() for node in cluster.values())
		if next_event < float("inf"):
			# TODO consider ongoing dataset copies as event too
			for node in cluster.values():
				datasets.update(node.progress_for(next_event))
			current_time += next_event
		for node_name in schedule:
			enqueued = cluster[node_name].enque(schedule[node_name][progress[node_name] :], datasets, task_graph)
			progress[node_name] += enqueued
			remaining -= enqueued
		running = sum(len(e.completion_pct) for e in cluster.values())
		if remaining == 0 and running == 0:
			break
	return current_time
