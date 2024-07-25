"""
Manager of the spawned processes (jobs). Used from worker.job_server
"""

import logging
from multiprocessing import Process
from dataclasses import dataclass
from typing import Optional
from gnosch.worker.bootstrap import new_process

logger = logging.getLogger(__name__)

def spawned_job_entrypoint(name: str, code: str) -> None:
	new_process()
	logger.debug(f"job starting: {name}")
	try:
		exec(code)
	except Exception:
		logger.exception(f"job got exception! {name}")
		raise

@dataclass
class JobStatus:
	exists: bool
	code: Optional[int]

class JobManager:
	jobs: dict[str, Process]

	def __init__(self):
		self.jobs = {}

	def quit(self):
		for job_name, job_process in self.jobs.items():
			logger.debug(f"joining {job_name}")
			job_process.join()

	def submit(self, name: str, code: str) -> bool:
		if name in self.jobs:
			return False
		else:
			p = Process(target = spawned_job_entrypoint, args = (name, code,))
			p.start()
			self.jobs[name] = p
			logger.debug(f"started job {p.pid} with {p.exitcode=}")
			return True

	def status(self, name: str) -> JobStatus:
		if name not in self.jobs:
			return JobStatus(False, None)
		else:
			logger.debug(f"inquries about job {self.jobs[name].pid} with {self.jobs[name].exitcode}")
			return JobStatus(True, self.jobs[name].exitcode)
