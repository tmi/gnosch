"""
Entrypoint for the worker process -- starts api_server and job_server.
"""

import logging
from multiprocessing import Process
from multiprocessing import set_start_method
import atexit
import gnosch.worker.local_comm as local_comm
import gnosch.worker.datasets as datasets
import gnosch.worker.jobs as jobs
import gnosch.worker.api_server as api_server
import gnosch.worker.job_server as job_server
from gnosch.common.bootstrap import new_process

logger = logging.getLogger(__name__)


def start() -> None:
	new_process()
	logger.info("starting server")

	set_start_method("forkserver")
	local_server = local_comm.LocalServer()
	dataset_manager = datasets.DatasetManager()
	job_manager = jobs.JobManager()
	grpc_server = Process(target=api_server.start)
	grpc_server.start()

	def _shutdown():
		grpc_server.join()
		job_manager.quit()
		dataset_manager.quit()
		local_server.quit()

	atexit.register(_shutdown)
	job_server.start(local_server, dataset_manager, job_manager)
