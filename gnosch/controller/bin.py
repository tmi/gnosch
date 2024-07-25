"""
Entrypoint for the controller process -- starts api_server.
"""

# NOTE this module is trivial, compared to worker/bin -- but we keep it for org parity

import gnosch.controller.api_server as api_server

def start() -> None:
	api_server.start()
