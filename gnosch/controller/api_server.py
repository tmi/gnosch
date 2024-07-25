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

	workers: dict[str, Worker]

	def __init__(self):
		self.workers = {}

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
		# TODO dataset lookup
		for worker in self.workers.values():
			yield from worker.client.DatasetCommand(request)
		if not self.workers:
			raise ValueError("no workers")

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

	def quit(self):
		for worker in self.workers.values():
			worker.quit()


def start() -> None:
	new_process()
	logger.info("starting controller grpc server")

	server = grpc.server(futures.ThreadPoolExecutor(max_workers=2))
	controller = ControllerImpl()
	atexit.register(controller.quit)
	services.add_GnoschControllerServicer_to_server(controller, server)
	services.add_GnoschBaseServicer_to_server(controller, server)
	server.add_insecure_port("[::]:50051")
	server.start()
	server.wait_for_termination()
