"""
Assumes a controller and at least one worker running.

Submits a simple job corresponding to gnosch.examples.jobs
"""

import numpy as np
import time
import os
from multiprocessing import set_start_method
import grpc
import gnosch.api.worker_pb2_grpc as services
import gnosch.api.worker_pb2 as protos


def main() -> None:
	print(f"main starting with pid {os.getpid()}")
	channel = grpc.insecure_channel("localhost:50051")
	client = services.WorkerStub(channel)
	ping = client.Ping(protos.PingRequest())
	if ping.status != protos.ServerStatus.OK:
		raise ValueError(f"controller not responding OK to ping: {ping}")

	print("about to purge previous run dataset (if exists)")
	purgeReq = protos.ClientCommandRequest(drop_dataset_id="d1")
	purgeRes = client.ClientCommand(purgeReq)
	print(f"{purgeRes=}")

	print("about to run producer")
	job1req = protos.ClientCommandRequest(new_job_definition="import gnosch.examples.jobs; gnosch.examples.jobs.data_producer()")
	job1res = client.ClientCommand(job1req)
	print(f"{job1res=}")

	print("about to run consumer")
	job2req = protos.ClientCommandRequest(new_job_definition="import gnosch.examples.jobs; gnosch.examples.jobs.data_consumer()")
	job2res = client.ClientCommand(job2req)
	print(f"{job2res=}")

	job2stReq = protos.ClientCommandRequest(query_job_status_id=job2res.job_id)
	while True:
		job2stRes = client.ClientCommand(job2stReq)
		print(f"{job2stRes=}")
		if job2stRes.job_status == protos.JobStatus.WORKER_RUNNING:
			time.sleep(0.2)
		elif job2stRes.job_status == protos.JobStatus.FINISHED:
			print("done")
			break
		else:
			raise ValueError(job2stRes)

	finReq = protos.RetrieveDatasetRequest(dataset_id="d1", block_size_hint=1024)
	finRes = client.RetrieveDataset(finReq)
	finResBuf = b""
	for finResIt in finRes:
		if finResIt.status != protos.DatasetStatus.DATASET_AVAILABLE:
			print("error obtaining final result")
		else:
			finResBuf += finResIt.data
	finResNp = np.frombuffer(finResBuf, dtype=int, count=3)
	print(f"final result: {finResNp}")


if __name__ == "__main__":
	set_start_method("forkserver")
	main()
