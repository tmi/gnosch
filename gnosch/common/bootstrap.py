"""
Util functions for bootstrapping new processes
"""

import logging


def new_process():
	logging.basicConfig(
		format="%(asctime)s %(levelname)s [%(process)d/%(thread)d] %(name)s@%(lineno)d: %(message)s",
		level=logging.DEBUG,
		force=True,
	)
