"""
Implements the non-public part of worker.job_interface, ie, the communication
between worker process and cluster jobs created by it.

Also used by the grpc server of the worker to relay the controller's commands.

The fact that unix sockets is used for local communications should remain
confined to this module.
"""

import time
import socket
import os

client_port_envvar = "_PORT"
def publish_client_port(port: int) -> None:
	"""Used by worker on start, to ensure future child processes can comm"""
	os.environ[client_port_envvar] = str(port)

client_port = int(os.getenv(client_port_envvar, "0"))

class LocalServer():
	"""Abstraction over socket, used for local comms. No business logic, just io"""
	def __init__(self):
		self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
		server = ("0.0.0.0", 0)
		self.sock.bind(server)
		publish_client_port(self.sock.getsockname()[1])

	def receive(self) -> tuple[bytes, str]:
		return self.sock.recvfrom(1024)

	def sendto(self, payload: bytes, address: str) -> None:
		self.sock.sendto(b'Y', address)

	def quit(self) -> None:
		self.sock.close()

def send_command(command: str, data: str) -> str:
	sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	if client_port == 0:
		raise ValueError("Client port not available -- process not launched correctly by worker")
	sock.connect(("localhost", client_port))
	sock.send(f"{command}:{data}".encode())
	response = sock.recv(1).decode()
	sock.close()
	return response

def await_command(command: str, data: str) -> None:
	# TODO add timeout
	while True:
		response = send_command(command, data)
		if response != 'N':
			return
		else:
			time.sleep(1)
