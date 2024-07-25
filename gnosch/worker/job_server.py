"""
Handles:
 - dataset management of the worker: holding of datasets, providing to local
   processes, uploading to other (remote) workers. Actual implementation is
   in worker.datasets, here we call api of that module
 - spawning of new jobs (processes), their monitoring, reporting their state
   to controller. Actual implementation is in worker.jobs, here we call api
   of that module.

This module is thus a Bridge responsibe for the event loop reading from
LocalServer and invoking the right methods from Dataset- and Job- Managers.
"""

# TODO try catches in the loop etc
# TODO add active job monitoring
# TODO add in grpc client to communicate with the controller
# TODO add in grpc client to communicate to other workers
# TODO make the command names systematic, get rid of repetitive code
#      each command should be Callable[payload, bool|exception]
#      the error codes should be made systematic as well, sorta like http

import os
import time
from gnosch.worker.datasets import DatasetManager, DatasetStatus
from gnosch.worker.jobs import JobManager
from gnosch.worker.local_comm import LocalServer

def start(local_server: LocalServer, dataset_manager: DatasetManager, job_manager: JobManager):
	while True:
		payload, client = local_server.receive()
		print(payload)
		command, data = payload.decode('ascii').split(":", 1)
		if command == 'ping':
			print("sending Y back")
			local_server.sendto(b'Y', client)
		elif command == 'new':
			if dataset_manager.new(data):
				local_server.sendto(b'Y', client)
			else:
				local_server.sendto(b'N', client)
		elif command == 'ready': 
			if dataset_manager.finalize(data):
				local_server.sendto(b'Y', client)
			else:
				local_server.sendto(b'N', client)
		elif command == 'ready_ds':
			ds_status = dataset_manager.status(data)
			if ds_status == DatasetStatus.finalized:
				local_server.sendto(b'Y', client)
			else:
				local_server.sendto(b'N', client)
		elif command == 'drop_ds':
			if dataset_manager.drop(data):
				print(f"dataset was dropped: {data}")
				local_server.sendto(b'Y', client)
			else:
				print(f"dataset was not present/finalized: {data}")
				local_server.sendto(b'N', client)
		elif command == 'submit':
			job_name, job_code = data.split('_', 1)
			if job_manager.submit(job_name, job_code):
				local_server.sendto(b'Y', client)
			else:
				local_server.sendto(b'N', client)
		elif command == 'ready_job':
			job_status = job_manager.status(data)
			if not job_status.exists:
				local_server.sendto(b'N', client)
			elif job_status.code == 0:
				local_server.sendto(b'Y', client)
			elif job_status.code is None:
				local_server.sendto(b'N', client)
			else:
				local_server.sendto(b'E', client)
		elif command == 'quit':
			local_server.sendto(b'Y', client)
			break
		else:
			local_server.sendto(b'E', client)
