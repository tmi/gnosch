"""
Example file for jobs to be executed on the cluster.

In a real setting, this would instead be a standalone module, installed separately to the worker's venv
"""

import os
import numpy as np
import gnosch.worker.job_interface as worker

def data_consumer() -> None:
	print(f"consuming dataset from {os.getpid()}")
	raw, h = worker.get_dataset('d1')
	print(f"data is {np.frombuffer(raw, dtype=int, count=3)}")
	h()
	print("consumer done")

def data_producer() -> None:
	print(f"producing dataset from {os.getpid()}")
	data = np.array([1, 2, 3]).tobytes()
	L = len(data)
	buf = worker.get_new_buffer('d1', L)
	buf[:L] = data
	worker.notify_upload_done('d1')
	print("producer done")
