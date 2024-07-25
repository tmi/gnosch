"""
Managment of information about datasets:
 - which worker holds it
 - the last known state of it
"""

from gnosch.controller.types import WorkerId, DatasetId
from dataclasses import dataclass
from enum import Enum

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
	last_update: int
	size_bytes: int

class DatasetManager():
	datasets: dict[DatasetId, Dataset]

	def __init__(self):
		datasets = {}

	def workers_with(dataset_id: DatasetId) -> list[WorkerId]:
		ds = self.datasets.get(dataset_id, None)
		if not ds:
			return []
		else:
			# NOTE [perf] we may want to cache this, use pyrsistent, ...
			return [ds.primary_worker] + ds.replicas.values()

	def primary_of(dataset_id: DatasetId) -> Optional[WorkerId]:
		# NOTE [perf] this method should instead yield the "least busy worker with this dataset"
		ds = self.datasets.get(dataset_id, None)
		if not ds:
			return None
		else:
			return ds.primary_worker
		
