"""
Implementation of the grpc api of the worker -- that is, the communication layer
for receiving commands from controller. Uses worker.local_comm to relay commands
to individual jobs, via worker.job_server.
"""

# TODO expose configs (grpc port, thread count)
# TODO separate out the controller part

import logging
import gnosch.api.worker_pb2_grpc as services
import uuid
import gnosch.api.worker_pb2 as protos
import grpc
from concurrent import futures
from gnosch.worker.local_comm import send_command
from gnosch.worker.job_interface import get_dataset
from typing import Any
from gnosch.worker.bootstrap import new_process

logger = logging.getLogger(__name__)

class WorkerImpl(services.Worker):
	def Ping(self, request: protos.PingRequest, context: Any): # type: ignore
		return protos.PingResponse(status=protos.ServerStatus.OK)
	
	def submit_job(self, definition: str) -> protos.ClientCommandResponse:
		job_id = str(uuid.uuid4())
		status = send_command("submit", f"{job_id}_{definition}")
		resp = protos.ClientCommandResponse(job_id=job_id)
		if status == 'Y':
			resp.job_status = protos.JobStatus.COORDINATOR_ACCEPTED
		else:
			resp.job_status = protos.JobStatus.COORDINATOR_ERROR
		return resp

	def job_status(self, job_id: str) -> protos.ClientCommandResponse:
		status = send_command("ready_job", job_id)
		resp = protos.ClientCommandResponse(job_id=job_id, worker_id="single")
		if status == 'Y':
			resp.job_status = protos.JobStatus.FINISHED
		elif status == 'N':
			resp.job_status = protos.JobStatus.WORKER_RUNNING
		else:
			resp.job_status = protos.JobStatus.WORKER_ERROR
		return resp

	def drop_dataset(self, dataset_id: str) -> protos.ClientCommandResponse:
		status = send_command("drop_ds", dataset_id)
		resp = protos.ClientCommandResponse()
		if status == 'Y':
			resp.dataset_id = dataset_id
		return resp

	def ClientCommand(self, request: protos.ClientCommandRequest, context: Any) -> protos.ClientCommandResponse: # type: ignore
		if request.new_job_definition:
			return self.submit_job(request.new_job_definition)
		elif request.query_job_status_id:
			return self.job_status(request.query_job_status_id)
		elif request.drop_dataset_id:
			return self.drop_dataset(request.drop_dataset_id)
		else:
			return protos.ClientCommandResponse()

	def RetrieveDataset(self, request: protos.RetrieveDatasetRequest, context: Any) -> protos.RetrieveDatasetBlock: # type: ignore
		data, h, available = get_dataset(request.dataset_id, 1_000)
		if not available:
			yield protos.RetrieveDatasetBlock(data=b"", status=protos.DatasetStatus.DATASET_NOT_FOUND)
		else:
			logger.debug("about to stream dataset")
			i, k, L = 0, request.block_size_hint, len(data)
			while i < L:
				yield protos.RetrieveDatasetBlock(data=bytes(data[i:i+k]), status=protos.DatasetStatus.DATASET_AVAILABLE)
				i+=k
			h()

def start() -> None:
	new_process()
	server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
	services.add_WorkerServicer_to_server(WorkerImpl(), server)
	server.add_insecure_port("[::]:50051")
	server.start()
	server.wait_for_termination()
