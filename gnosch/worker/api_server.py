"""
Implementation of the grpc api of the worker -- that is, the communication layer
for receiving commands from controller. Uses worker.local_comm to relay commands
to individual jobs, via worker.job_server.
"""

# TODO expose configs (grpc port, thread count)
# TODO separate out the controller part

import logging
import gnosch.api.gnosch_pb2_grpc as services
import uuid
import gnosch.api.gnosch_pb2 as protos
import grpc
from concurrent import futures
from gnosch.worker.local_comm import send_command
from gnosch.worker.job_interface import get_dataset
from typing import Any, Iterator
from gnosch.common.bootstrap import new_process
from gnosch.worker.client_controller import ClientController

logger = logging.getLogger(__name__)

class WorkerImpl(services.GnoschBase):
	worker_id: str

	def __init__(self):
		with ClientController.get_channel() as channel:
			client = services.GnoschControllerStub(channel)
			request = protos.RegisterWorkerRequest(url="localhost:50052") # TODO param
			self.worker_id = client.RegisterWorker(request).worker_id
		# TODO await ping for the job_server
		status = send_command("report_worker_id", self.worker_id)
		if status != "Y":
			raise ValueError("failed to register worker id")

	def Ping(self, request: protos.PingRequest, context: Any):  # type: ignore
		return protos.PingResponse(status=protos.ServerStatus.OK)

	def JobCreate(self, request: protos.JobCreateRequest, context: Any) -> protos.JobResponse: # type: ignore
		job_id = str(uuid.uuid4())
		status = send_command("submit", f"{job_id}_{request.definition}")
		resp = protos.JobResponse(job_id=job_id, worker_id=self.worker_id)
		if status == "Y":
			resp.job_status = protos.JobStatus.WORKER_ACCEPTED
		else:
			resp.job_status = protos.JobStatus.WORKER_ERROR
		return resp

	def JobStatus(self, request: protos.JobStatusRequest, context: Any) -> protos.JobResponse: # type: ignore
		status = send_command("ready_job", request.job_id)
		resp = protos.JobResponse(job_id=request.job_id, worker_id=self.worker_id)
		if status == "Y":
			resp.job_status = protos.JobStatus.FINISHED
		elif status == "N":
			resp.job_status = protos.JobStatus.WORKER_RUNNING
		else:
			resp.job_status = protos.JobStatus.WORKER_ERROR
		return resp

	def DatasetCommand(self, request: protos.DatasetCommandRequest, context: Any) -> Iterator[protos.DatasetCommandResponse]: # type: ignore
		if request.retrieve:
			data, h, available = get_dataset(request.dataset_id, 1_000)
			if not available:
				yield protos.DatasetCommandResponse(data=b"", status=protos.DatasetCommandResult.DATASET_NOT_FOUND, worker_id=self.worker_id)
			else:
				logger.debug("about to stream dataset")
				i, k, L = 0, request.block_size_hint, len(data)
				while i < L:
					yield protos.DatasetCommandResponse(data=bytes(data[i : i + k]), status=protos.DatasetCommandResult.DATASET_AVAILABLE, worker_id=self.worker_id)
					i += k
				h()
		if request.drop:
			response = protos.DatasetCommandResponse(data=bytes(), dataset_id=request.dataset_id, worker_id=self.worker_id)
			status = send_command("drop_ds", request.dataset_id)
			if status == "Y":
				response.status=protos.DatasetCommandResult.DATASET_DROPPED
			else:
				response.status=protos.DatasetCommandResult.DATASET_NOT_FOUND
			yield response

def start() -> None:
	new_process()
	logger.info("starting worker grpc server")

	server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
	services.add_GnoschBaseServicer_to_server(WorkerImpl(), server)
	server.add_insecure_port("[::]:50052") # TODO param
	server.start()
	server.wait_for_termination()
