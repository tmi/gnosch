"""
Example file for jobs to be executed on the cluster.

In a real setting, this would instead be a standalone module, installed separately to the worker's venv
"""

import os
import numpy as np
import gnosch.worker.job_interface as worker
import logging

logger = logging.getLogger(__name__)


def data_consumer() -> None:
	logger.info(f"consuming dataset from {os.getpid()}")
	raw, h, a = worker.get_dataset("d1", 5_000)
	if not a:
		raise ValueError("get_dataset failed!")
	logger.info(f"data is {np.frombuffer(raw, dtype=int, count=3)}")
	h()
	logger.info("consumer done")


def data_producer() -> None:
	logger.info(f"producing dataset from {os.getpid()}")
	data = np.array([1, 2, 3]).tobytes()
	L = len(data)
	buf = worker.get_new_buffer("d1", L)
	buf[:L] = data
	worker.notify_upload_done("d1")
	logger.info("producer done")
