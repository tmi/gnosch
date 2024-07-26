"""
Implementation of the grpc api of the controller -- that is, the communication layer
for receiving requests by the client or events from workers. Mostly just updates
internal state, invokes scheduler, and issues further requests to workers
"""

import atexit
from dataclasses import dataclass
from typing import Any, Iterator
from concurrent import futures
import uuid
import logging
from gnosch.common.bootstrap import new_process
import gnosch.api.gnosch_pb2_grpc as services
import gnosch.api.gnosch_pb2 as protos
import grpc
from gnosch.controller.types import WorkerId
from gnosch.controller.datasets import DatasetManager

logger = logging.getLogger(__name__)

@dataclass
class Worker:
	url: str
	channel: Any
	client: Any
	# resources, status, ...

	def quit(self):
		self.channel.close()

class ControllerImpl(services.GnoschBase, services.GnoschController):

	workers: dict[WorkerId, Worker]
	dataset_manager: DatasetManager

	def __init__(self, dataset_manager):
		self.workers = {}
		self.dataset_manager = dataset_manager

	def Ping(self, request: protos.PingRequest, context: Any):  # type: ignore
		return protos.PingResponse(status=protos.ServerStatus.OK)

	def JobCreate(self, request: protos.JobCreateRequest, context: Any) -> protos.JobResponse: # type: ignore
		# TODO scheduling
		for worker in self.workers.values():
			return worker.client.JobCreate(request)
		raise ValueError("no workers")

	def JobStatus(self, request: protos.JobStatusRequest, context: Any) -> protos.JobResponse: # type: ignore
		# TODO job lookup
		for worker in self.workers.values():
			return worker.client.JobStatus(request)
		raise ValueError("no workers")

	def DatasetCommand(self, request: protos.DatasetCommandRequest, context: Any) -> Iterator[protos.DatasetCommandResponse]: # type: ignore
		primary_id = self.dataset_manager.primary_of(request.dataset_id)
		if not primary_id:
			raise ValueError("no workers")
		primary = self.workers[primary_id]
		if request.drop:
			workers = self.dataset_manager.replicas_with(request.dataset_id)
			subreq = protos.DatasetCommandRequest(dataset_id=request.dataset_id, drop=True)
			# NOTE [perf] run in async/pool. Also update status to purging
			for worker_id in workers:
				for response in self.workers[worker_id].client.DatasetCommand(subreq):
					self.dataset_manager.update(response)
					yield response
			for response in primary.client.DatasetCommand(subreq):
				self.dataset_manager.update(response)
				yield response
		if request.retrieve:
			# NOTE [perf] consider asking multiple workers, each for a different block
			# NOTE [perf] consider returning the assignment for client to fetch on their own instead
			for response in primary.client.DatasetCommand(request):
				self.dataset_manager.update(response)
				yield response

	def RegisterWorker(self, request: protos.RegisterWorkerRequest, context: Any) -> protos.RegisterWorkerResponse: # type: ignore
		logger.info(f"registering worker {request}")
		while True:
			worker_id = str(uuid.uuid4())
			if worker_id not in self.workers:
				break
		channel = grpc.insecure_channel(request.url)
		client = services.GnoschBaseStub(channel)
		self.workers[worker_id] = Worker(url=request.url, channel=channel, client=client)
		logger.debug(f"currently registered {len(self.workers)} workers")
		return protos.RegisterWorkerResponse(worker_id=worker_id)

	def RegisterDataset(self, request: protos.RegisterDatasetRequest, context: Any) -> protos.RegisterDatasetResponse: # type: ignore
		logger.info(f"registering dataset {request}")
		if request.status != protos.DatasetCommandResult.DATASET_AVAILABLE:
			raise NotImplementedError(request.status)
		self.dataset_manager.update(request)
		

	def quit(self):
		for worker in self.workers.values():
			worker.quit()


def start() -> None:
	new_process()
	logger.info("starting controller grpc server")

	dataset_manager = DatasetManager()
	controller = ControllerImpl(dataset_manager)

	server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
	atexit.register(controller.quit)
	services.add_GnoschControllerServicer_to_server(controller, server)
	services.add_GnoschBaseServicer_to_server(controller, server)
	server.add_insecure_port("[::]:50051")
	server.start()
	server.wait_for_termination()
