"""
Manager of the shared memory for holding datasets. Used from worker.job_server
"""

from dataclasses import dataclass
from multiprocessing import shared_memory
from typing import Optional
from enum import Enum

# memory management
# NOTE we are currently based on shared_memory, but we may want to explore other venues:
# - mmap, via
#   datasets[name] = mmap(-1, L)
#   datasets[name][:] = data
#   https://stackoverflow.com/questions/4991533/sharing-memory-between-processes-through-the-use-of-mmap
# - https://semanchuk.com/philip/posix_ipc/
#   https://stackoverflow.com/questions/7419159/giving-access-to-shared-memory-after-child-processes-have-already-started

@dataclass
class Dataset:
	shm: Optional[shared_memory.SharedMemory]
	finalized: bool

class DatasetStatus(Enum):
	missing = 0
	not_finalized = 1
	finalized = 2
	

class DatasetManager():
	datasets: dict[str, Dataset]

	def __init__(self):
		self.datasets = {}

	def status(self, dataset_key: str) -> DatasetStatus:
		if dataset_key not in self.datasets:
			return DatasetStatus.missing
		elif self.datasets[dataset_key].finalized:
			return DatasetStatus.finalized
		else:
			return DatasetStatus.not_finalized

	def new(self, dataset_key: str) -> bool:
		status = self.status(dataset_key)
		if status == DatasetStatus.missing:
			self.datasets[dataset_key] = Dataset(shm=None, finalized=False)
			return True
		else:
			return False

	def finalize(self, dataset_key: str) -> bool:
		status = self.status(dataset_key)
		if status == DatasetStatus.missing or status == DatasetStatus.finalized:
			return False
		else:
			self.datasets[dataset_key].shm = shared_memory.SharedMemory(name=dataset_key, create=False)
			self.datasets[dataset_key].finalized = True
			return True

	def drop(self, dataset_key: str, pop: bool = True) -> bool:
		status = self.status(dataset_key)
		if status == DatasetStatus.finalized:
			if pop:
				shm = self.datasets.pop(dataset_key).shm 
			else:
				shm = self.datasets[dataset_key].shm
			if shm is None:
				raise ValueError(f"finalized but None dataset: {dataset_key}")
			shm.close()
			shm.unlink()
			return True
		else:
			return False

	def quit(self):
		for k in self.datasets.keys():
			try:
				self.drop(k, pop=False)
			except Exception as e:
				print(f"gotten exception when unlinking: {k}, {e}")
