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
