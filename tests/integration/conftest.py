# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
#
# The integration tests use the Jubilant library. See https://documentation.ubuntu.com/jubilant/
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

import logging
import os
import pathlib
import sys
import time

import jubilant
import pytest
import sh
from test_charm import EXECUTOR_NAMESPACE

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def juju(request: pytest.FixtureRequest):
    """Create a temporary Juju model for running tests."""
    # Create the K8s namespace that the executor charm will use to schedule worker pods.
    # The charm expects this namespace to already exist in the cluster.
    sh.kubectl("create", "namespace", EXECUTOR_NAMESPACE)

    try:
        if "JUJU_MODEL" in os.environ:
            juju = jubilant.Juju(wait_timeout=10 * 60)

            juju.add_model(os.environ["JUJU_MODEL"], config={"update-status-hook-interval": "10s"})

            yield juju

            if request.session.testsfailed:
                logger.info("Collecting Juju logs...")
                time.sleep(0.5)  # Wait for Juju to process logs.
                log = juju.debug_log(limit=1000)
                print(log, end="", file=sys.stderr)

            return

        with jubilant.temp_model(config={"update-status-hook-interval": "10s"}) as juju:
            juju.wait_timeout = 10 * 60

            yield juju

            if request.session.testsfailed:
                logger.info("Collecting Juju logs...")
                time.sleep(0.5)  # Wait for Juju to process logs.
                log = juju.debug_log(limit=1000)
                print(log, end="", file=sys.stderr)
    finally:
        sh.kubectl("delete", "namespace", EXECUTOR_NAMESPACE, "--ignore-not-found")


@pytest.fixture(scope="session")
def charm():
    """Return the path of the charm under test."""
    if "CHARM_PATH" in os.environ:
        charm_path = pathlib.Path(os.environ["CHARM_PATH"])
        if not charm_path.exists():
            raise FileNotFoundError(f"Charm does not exist: {charm_path}")
        return charm_path

    charm_paths = list(pathlib.Path(".").glob("airflow-kubernetes-executor-k8s*.charm"))
    if not charm_paths:
        raise FileNotFoundError(
            "No airflow-kubernetes-executor-k8s .charm file in current directory"
        )

    if len(charm_paths) > 1:
        path_list = ", ".join(str(path) for path in charm_paths)
        raise ValueError(f"More than one .charm file in current directory: {path_list}")

    return charm_paths[0]


@pytest.fixture(scope="session")
def mock_coordinator_charm():
    """Return the path of the mock coordinator charm."""
    if "MOCK_COORDINATOR_CHARM_PATH" in os.environ:
        charm_path = pathlib.Path(os.environ["MOCK_COORDINATOR_CHARM_PATH"])
        if not charm_path.exists():
            raise FileNotFoundError(f"Mock coordinator charm does not exist: {charm_path}")
        return charm_path

    charm_paths = list(pathlib.Path(".").glob("mock-coordinator-charm*.charm"))
    if not charm_paths:
        raise FileNotFoundError("No mock-coordinator-charm .charm file in current directory")

    if len(charm_paths) > 1:
        path_list = ", ".join(str(path) for path in charm_paths)
        raise ValueError(
            f"More than one mock coordinator .charm file in current directory: {path_list}"
        )

    return charm_paths[0]
