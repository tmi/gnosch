import grpc
from typing import Any
import gnosch.api.gnosch_pb2_grpc as services
import gnosch.api.gnosch_pb2 as protos

class ClientController():
	controller_url = "localhost:50051" # TODO param
	channel: Any
	client: Any

	@classmethod
	def get_channel(cls) -> Any:
		"""For one-off calls"""
		return grpc.insecure_channel(cls.controller_url)

	def __init__(self):
		self.channel = grpc.insecure_channel(self.controller_url)
		client = services.GnoschControllerStub(self.channel)

	def quit(self):
		self.channel.close()

	def register_dataset(self, dataset_id: str, worker_id: str) -> bool:
		request = protos.DatasetCommandResponse(
			status=protos.DatasetCommandResult.DATASET_AVAILABLE,
			dataset_id=dataset_id,
			worker_id=worker_id,
		)
		response = self.client.RegisterDataset(request)
		if response.status == protos.ServerStatus.OK:
			return True
		else:
			return False
			
