# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
#
# The integration tests use the Jubilant library. See https://documentation.ubuntu.com/jubilant/
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

import logging
import pathlib

import jubilant

import constants

logger = logging.getLogger(__name__)

WORKLOAD_IMAGE = "ubuntu/airflow:3.1-24.04_edge"


def test_deploy(juju: jubilant.Juju, charm: pathlib.Path, mock_coordinator_charm: pathlib.Path):
    """Deploy the executor charm and the mock coordinator."""
    logger.info("Deploying airflow-kubernetes-executor-k8s")

    juju.deploy(
        charm.resolve(),
        app="airflow-kubernetes-executor-k8s",
        config={
            "base_image": WORKLOAD_IMAGE,
            "namespace": juju.model,
        },
        trust=True,
    )

    juju.wait(
        lambda status: (
            jubilant.all_blocked(status, "airflow-kubernetes-executor-k8s")
            and status.apps["airflow-kubernetes-executor-k8s"].app_status.message
            == constants.MISSING_REQUIRES_RELATION_MESSAGE
        )
    )

    logger.info("Deploying mock-coordinator")

    juju.deploy(mock_coordinator_charm.resolve(), app="mock-coordinator")

    juju.wait(lambda status: jubilant.all_active(status, "mock-coordinator"))


def test_integrate_with_coordinator(juju: jubilant.Juju):
    """Integrate executor with the mock coordinator and verify active status."""
    logger.info("Integrating executor <-> mock coordinator")

    juju.integrate(
        "airflow-kubernetes-executor-k8s:airflow-config",
        "mock-coordinator:airflow-config",
    )

    juju.wait(jubilant.all_active)
