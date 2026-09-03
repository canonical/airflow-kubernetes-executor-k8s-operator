set export
set fallback


[private]
default:
	just --list

[private]
clean-mock-charm-libs:
	rm -rf tests/integration/mock-coordinator-charm/lib

[private]
clean-charms:
	find . -type f -name '*.charm' -delete

# Run lint
lint: (clean-mock-charm-libs)
	uv tool run tox -e lint

# Run format
format: (clean-mock-charm-libs)
	uv tool run tox -e format

# Run unit tests
unit:
	uv tool run tox -e unit

# Run integration tests
integration debug="": (clean)
	#!/usr/bin/bash
	set -euo pipefail

	charmcraft pack

	cp -r lib tests/integration/mock-coordinator-charm/lib

	trap 'just clean-mock-charm-libs' EXIT

	charmcraft pack --project-dir tests/integration/mock-coordinator-charm
	cp tests/integration/mock-coordinator-charm/mock-coordinator-charm*.charm .

	pdb_options=$(if [ -n "${debug}" ]; then echo "--pdb"; fi)

	JUJU_MODEL=test uv tool run tox -e integration -- ${pdb_options}

# Clean up test environment
clean: (clean-mock-charm-libs) (clean-charms)
	juju destroy-model --force --destroy-storage --no-prompt test || true

# Get system state for debugging
get-system-state:
    #!/usr/bin/bash

    df -h
    echo "---"

    juju status --model test --color --relations --storage
    echo "---"

    sudo k8s status
    echo "---"
