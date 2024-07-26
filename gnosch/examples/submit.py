"""
Assumes a controller and at least one worker running.

Submits a simple job corresponding to gnosch.examples.jobs
"""

import numpy as np
import time
import os
import grpc
import gnosch.api.gnosch_pb2_grpc as services
import gnosch.api.gnosch_pb2 as protos


def main() -> None:
	print(f"main starting with pid {os.getpid()}")
	channel = grpc.insecure_channel("localhost:50052")
	client = services.GnoschBaseStub(channel)
	ping = client.Ping(protos.PingRequest())
	if ping.status != protos.ServerStatus.OK:
		raise ValueError(f"worker not responding OK to ping: {ping}")
	else:
		print("worker healthy")

	channel = grpc.insecure_channel("localhost:50051")
	client = services.GnoschBaseStub(channel)
	ping = client.Ping(protos.PingRequest())
	if ping.status != protos.ServerStatus.OK:
		raise ValueError(f"controller not responding OK to ping: {ping}")
	else:
		print("controller up and ready")

	print("about to purge previous run dataset (if exists)")
	purgeReq = protos.DatasetCommandRequest(dataset_id="d1", drop=True)
	purgeRes = client.DatasetCommand(purgeReq)
	print(f"{purgeRes=}")

	print("about to run producer")
	job1req = protos.JobCreateRequest(definition="import gnosch.examples.jobs; gnosch.examples.jobs.data_producer()")
	job1res = client.JobCreate(job1req)
	print(f"{job1res=}")

	print("about to run consumer")
	job2req = protos.JobCreateRequest(definition="import gnosch.examples.jobs; gnosch.examples.jobs.data_consumer()")
	job2res = client.JobCreate(job2req)
	print(f"{job2res=}")

	job2stReq = protos.JobStatusRequest(job_id=job2res.job_id)
	while True:
		job2stRes = client.JobStatus(job2stReq)
		print(f"{job2stRes=}")
		if job2stRes.job_status == protos.JobStatus.WORKER_RUNNING:
			time.sleep(0.2)
		elif job2stRes.job_status == protos.JobStatus.FINISHED:
			print("done")
			break
		else:
			raise ValueError(job2stRes)

	finReq = protos.DatasetCommandRequest(dataset_id="d1", block_size_hint=1024, retrieve=True)
	finRes = client.DatasetCommand(finReq)
	finResBuf = b""
	for finResIt in finRes:
		if finResIt.status != protos.DatasetCommandResult.DATASET_AVAILABLE:
			print("error obtaining final result")
		else:
			finResBuf += finResIt.data
	finResNp = np.frombuffer(finResBuf, dtype=int, count=3)
	print(f"final result: {finResNp}")


if __name__ == "__main__":
	main()
