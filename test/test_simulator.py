from gnosch.scheduler.model import ClusterSpec, NodeSpec, Task, TaskOutput, TaskInput, SchedulingCommand
from gnosch.scheduler.simulator import simulate
import pytest

import logging

logging.basicConfig(level=logging.DEBUG)


def test_basic_simulation():
	logging.info("c1 start")
	node = NodeSpec(cpus=1, memory_mb=2048)
	cluster = ClusterSpec(nodes={"n1": node}, comm_mbps={})

	# dag:
	# task1 --> task2
	# task3
	task_graph = {
		"t1": Task(inputs=[], outputs=[TaskOutput(dataset_id="d1", size_mb=128)], memory_mb=512, prefered_cpus=2, runtime_est_s=5),
		"t2": Task(inputs=[TaskInput(dataset_id="d2")], outputs=[], memory_mb=512, prefered_cpus=2, runtime_est_s=5),
		"t3": Task(inputs=[], outputs=[], memory_mb=512, prefered_cpus=3, runtime_est_s=5),
	}

	# first t1 and t3 run concurrently for 20s -> then t1 finishes, and t3 remains with 5 cpusecs
	# then t2 and t3 run concurrently for 10s -> then t3 finished
	# then t2 runs until its 5 cpusecs are done in 5s
	# => 35s total
	schedule = {
		"n1": [
			SchedulingCommand(launch_task="t1"),
			SchedulingCommand(launch_task="t3"),
			SchedulingCommand(launch_task="t2"),
		],
	}

	simulated_runtime = simulate(cluster, task_graph, schedule)
	expected_runtime = pytest.approx(35.0)

	assert simulated_runtime == expected_runtime


def test_two_nodes():
	n1 = NodeSpec(cpus=1, memory_mb=1024)
	n2 = NodeSpec(cpus=1, memory_mb=1024)
	cluster = ClusterSpec(
		nodes={
			"n1": n1,
			"n2": n2,
		},
		comm_mbps={},
	)
	# dag:
	# task1 -\
	# task2 ---> task3
	task_graph = {
		"t1": Task(inputs=[], outputs=[TaskOutput(dataset_id="d1", size_mb=128)], memory_mb=512, prefered_cpus=1, runtime_est_s=5),
		"t2": Task(inputs=[], outputs=[TaskOutput(dataset_id="d2", size_mb=128)], memory_mb=512, prefered_cpus=1, runtime_est_s=5),
		"t3": Task(
			inputs=[TaskInput(dataset_id="d1"), TaskInput(dataset_id="d2")], outputs=[], memory_mb=512, prefered_cpus=1, runtime_est_s=5
		),
	}

	# t1 runs on n1, t2 runs on n2, then d1 gets copied to n2 and t3 runs there
	schedule = {
		"n1": [
			SchedulingCommand(launch_task="t1"),
		],
		"n2": [
			SchedulingCommand(launch_task="t2"),
			SchedulingCommand(fetch_dataset="d1"),
			SchedulingCommand(launch_task="t3"),
		],
	}

	simulated_runtime = simulate(cluster, task_graph, schedule)
	expected_runtime = pytest.approx(10.0)

	assert simulated_runtime == expected_runtime
