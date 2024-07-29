protoc:
	python -m grpc_tools.protoc -Ignosch/api=./gnosch/proto --python_out=. --pyi_out=. --grpc_python_out=. gnosch/proto/gnosch.proto
	touch gnosch/api/__init__.py

worker-server:
	python -c 'import gnosch.worker.bin as w; w.start()'

controller-server:
	python -c 'import gnosch.controller.bin as c; c.start()'

example:
	PYTHONPATH="." python ./gnosch/examples/submit.py

clean-proto:
	rm -f gnosch/api/*py gnosch/api/*pyi

clean-pyc:
	find . -type f -name '*.py[co]' -delete -o -type d -name __pycache__ -delete

val:
	mypy gnosch --ignore-missing-imports
	mypy test --ignore-missing-imports

pytest:
	PYTHONPATH="." pytest .
