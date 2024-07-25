"""
Interface for cluster jobs -- involves locally submitting dataset (which
the controller then publishes cluster-wide) or obtaining a published
dataset for usage in the job.
"""

# TODO add retry/recovery

import time
import socket
from multiprocessing import shared_memory
from typing import Callable, Any
import os
from gnosch.worker.local_comm import send_command, await_command

datasets: dict[str, shared_memory.SharedMemory] = {}
def get_new_buffer(name: str, size: int) -> memoryview:
	if name in datasets:
		print(f"dataset already exists! Dropping. {name=}")
		datasets.pop(name).close()
	response = send_command("new", name)
	if response == 'N':
		raise ValueError(f"dataset already exists! {name=}")
	datasets[name] = shared_memory.SharedMemory(name=name, create=True, size=size)
	return datasets[name].buf

def notify_upload_done(name: str) -> None:
	datasets[name].close()
	response = send_command("ready", name)
	if response == 'N':
		raise ValueError(f"problem: {response=}")

def get_dataset(name: str, timeout_ms: int) -> tuple[bytes, Callable, bool]:
	# TODO return status instead of bool... and wrap in a dataclass
	print("about to await")
	if not await_command("ready_ds", name, timeout_ms):
		return b"", lambda : None, False
	print("about to bind")
	m = shared_memory.SharedMemory(name=name, create=False)
	return m.buf, lambda : m.close(), True # or register the m.close for atexit instead?
