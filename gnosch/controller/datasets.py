"""
Managment of information about datasets:
 - which worker holds it
 - the last known state of it
"""

from typing import Iterable, Optional
import gnosch.api.gnosch_pb2 as protos
from gnosch.controller.types import WorkerId, DatasetId
from dataclasses import dataclass
from enum import Enum
from time import time
import logging

logger = logging.getLogger(__name__)

class DatasetStatus(Enum):
	TRANSFERING = 0
	COMPUTING = 1
	AVAILABLE = 2
	PURGING = 3

@dataclass
class Dataset:
	primary_worker: WorkerId
	primary_status: DatasetStatus
	replicas: dict[WorkerId, DatasetStatus]
	last_update: float
	size_bytes: int

class DatasetManager():
	datasets: dict[DatasetId, Dataset]

	def __init__(self):
		datasets = {}

	def replicas_with(self, dataset_id: DatasetId) -> Iterable[WorkerId]:
		ds = self.datasets.get(dataset_id, None)
		if not ds:
			return []
		else:
			# NOTE [perf] maybe cache, pyrsistent, etc...
			return ds.replicas.keys()

	def primary_of(self, dataset_id: DatasetId) -> Optional[WorkerId]:
		# NOTE [perf] this method should instead yield the "least busy worker with this dataset".
		#      the whole primary concept should go away to simplify the rest of the code
		ds = self.datasets.get(dataset_id, None)
		if not ds:
			return None
		else:
			return ds.primary_worker

	def update(self, response: protos.DatasetCommandResponse) -> None:
		ds = self.datasets.get(response.dataset_id, None)
		if response.status == protos.DatasetCommandResult.DATASET_AVAILABLE:
			if ds:
				ds.replicas[response.worker_id] = DatasetStatus.AVAILABLE
			else:
				self.datasets[response.dataset_id] = Dataset(
					primary_worker=response.worker_id,
					primary_status=DatasetStatus.AVAILABLE,
					replicas={},
					last_update=time(),
					size_bytes=-1,
				)
		elif ds:
			ds.last_update = max(ds.last_update, time())
			if response.status == protos.DatasetCommandResult.DATASET_DROPPED:
				if response.worker_id != ds.primary_worker:
					ds.replicas.pop(response.worker_id)
				else:
					if not ds.replicas:
						self.datasets.pop(response.dataset_id)
					else:
						raise NotImplementedError("must first remove all replicas")
		else:
			logger.warning(f"received dataset update for a non-existent dataset: {response}")
