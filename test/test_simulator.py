from gnosch.scheduler.model import Cluster, Schedule, TaskGraph, NodeSpec, Task, TaskOutput, TaskInput, SchedulingConditions
from gnosch.scheduler.simulator import simulate
import pytest

def test_basic_simulation():
	node = NodeSpec(cpus=1, memory_mb=1024)
	cluster = Cluster(nodes={"n1": node}, comm_mbps={})

	# dag:
	# task1 --> task2
	# task3
	task_graph = {
		"t1": Task(inputs=[], outputs=[TaskOutput(dataset_id="d1")], memory_mb=512, prefered_cpus=2, runtime_est_s=5),
		"t2": Task(inputs=[TaskInput(dataset_id="d2")], outputs=[], memory_mb=512, prefered_cpus=2, runtime_est_s=5),
		"t3": Task(inputs=[], outputs=[], memory_mb=512, prefered_cpus=3, runtime_est_s=5),
	}
	# first t1 and t3 run concurrently for 20s -> then t1 finishes, and t3 remains with 5 cpusecs
	# then t2 and t3 run concurrently for 10s -> then t3 finished
	# then t2 runs until its 5 cpusecs are done in 5s
	# => 35s total

	schedule = {
		"t1": SchedulingConditions(target_node="n1", tasks_completed=[], datasets_available=[]),
		"t2": SchedulingConditions(target_node="n1", tasks_completed=[], datasets_available=["d1"]),
		"t3": SchedulingConditions(target_node="n1", tasks_completed=[], datasets_available=[]),
	}
	
	simulated_runtime = simulate(cluster, task_graph, schedule)
	expected_runtime = pytest.approx(35.)

	assert simulated_runtime == expected_runtime
