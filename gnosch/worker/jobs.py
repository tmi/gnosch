"""
Manager of the spawned processes (jobs). Used from worker.job_server
"""

from multiprocessing import Process
from dataclasses import dataclass
from typing import Optional

def spawned_job_entrypoint(name: str, code: str) -> None:
	print(f"job starting: {name}")
	try:
		exec(code)
	except Exception as e:
		print(f"job got exception! {name} {e}")
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
			print(f"joining {job_name}")
			job_process.join()

	def submit(self, name: str, code: str) -> bool:
		if name in self.jobs:
			return False
		else:
			p = Process(target = spawned_job_entrypoint, args = (name, code,))
			p.start()
			self.jobs[name] = p
			return True

	def status(self, name: str) -> JobStatus:
		if name not in self.jobs:
			return JobStatus(False, None)
		else:
			return JobStatus(True, self.jobs[name].exitcode)
