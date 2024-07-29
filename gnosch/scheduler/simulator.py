"""
Given a schedule and cluster spec, simulate how long it would, in theory, run.
"""

import logging
from collections import defaultdict
from gnosch.scheduler.model import Cluster, Schedule, TaskGraph

logger = logging.getLogger(__name__)


def simulate(cluster_spec: Cluster, task_graph: TaskGraph, schedule: Schedule) -> float:
	"""Always returns time estimate, does not account for memory crashes or swapping slowdowns.
	Ignores inter-node comm delays -- assumes instant output dataset broadcasts"""

	current_time = 0.0
	task_completions: dict[str, float] = {}
	task_cpusecs: dict[str, int] = {task_id: task.prefered_cpus * task.runtime_est_s for task_id, task in task_graph.items()}
	unscheduled: set[str] = {task for task in schedule}

	running: dict[str, set[str]] = defaultdict(set)
	datasets: set[str] = set()
	done: set[str] = set()

	for task, conditions in schedule.items():
		task_completions[task] = 0.0
		if not conditions.tasks_completed and not conditions.datasets_available:
			unscheduled.remove(task)
			running[conditions.target_node].add(task)

	while task_completions:
		next_event = float("inf")
		finished_tasks = set()
		# identify task to finish first
		for node, tasks in running.items():
			cpus_per_task = cluster_spec.nodes[node].cpus / len(tasks)
			for task in tasks:
				remaining_cpusecs = (1 - task_completions[task]) * task_cpusecs[task]
				to_complete = remaining_cpusecs / cpus_per_task
				next_event = min(next_event, to_complete)
		# increase completion for every task
		logger.debug(f"next event will happen in {next_event} secs")
		for node, tasks in running.items():
			cpus_per_task = cluster_spec.nodes[node].cpus / len(tasks)
			for task in tasks:
				elapsed_cpusecs = next_event * cpus_per_task
				elapsed_pctage = elapsed_cpusecs / task_cpusecs[task]
				task_completions[task] += elapsed_pctage
				if task_completions[task] > 0.99:
					logger.debug(f"marking {task=} as finished")
					finished_tasks.add(task)
		# mark as done, remove from in progress, add new datasets
		for task in finished_tasks:
			task_completions.pop(task)
		for node in cluster_spec.nodes:
			running[node] = running[node] - finished_tasks
		done = done.union(finished_tasks)
		for task in finished_tasks:
			for output in task_graph[task].outputs:
				logger.debug("marking dataset {output.dataset_id} as computed")
				datasets.add(output.dataset_id)
		current_time += next_event
		logger.debug(f"total time is now {current_time} secs")
		# launch new tasks
		next_launch: set[str] = set()
		for task in unscheduled:
			# NOTE this is a rather unperformant part -- replace the set calls with some dynamically shrinked copy
			if not (set(schedule[task].datasets_available) - datasets) and not (set(schedule[task].tasks_completed) - done):
				logger.debug(f"launching task {task} at node {schedule[task].target_node}")
				next_launch.add(task)
				running[schedule[task].target_node].add(task)
		unscheduled = unscheduled - next_launch
	return current_time
