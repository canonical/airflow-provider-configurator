set export
set fallback


[private]
default:
	just --list

# Run lint
lint:
	uv tool run --python 3.12 tox -e lint

# Run format
format:
	uv tool run --python 3.12 tox -e format

# Run unit tests
unit:
	uv tool run --python 3.12 tox -e unit

# Run integration tests
integration:
	uv tool run --python 3.12 tox -e integration

# Get system state for debugging a failed integration run
get-system-state:
	#!/usr/bin/bash

	df -h
	echo "---"

	# The tests run in a temporary model unless JUJU_MODEL is set, so report on
	# whichever models exist rather than assuming a fixed name.
	juju models || true
	echo "---"

	juju status --color --relations --storage || true
	echo "---"

	sudo k8s status || true
	echo "---"
