# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.
#
# The integration tests use the Jubilant library. See https://documentation.ubuntu.com/jubilant/
# To learn more about testing, see https://documentation.ubuntu.com/ops/latest/explanation/testing/

import json
import logging
import pathlib

import jubilant
import sh

import constants

logger = logging.getLogger(__name__)

WORKLOAD_IMAGE = "ubuntu/airflow:3.1-24.04_edge"
EXECUTOR_APP = "airflow-kubernetes-executor-k8s"
MOCK_COORDINATOR_APP = "mock-coordinator"
EXECUTOR_NAMESPACE = "airflow-workers-test"


def test_deploy(juju: jubilant.Juju, charm: pathlib.Path, mock_coordinator_charm: pathlib.Path):
    """Deploy the executor charm and the mock coordinator."""
    logger.info("Deploying airflow-kubernetes-executor-k8s")

    juju.deploy(
        charm.resolve(),
        app=EXECUTOR_APP,
        config={
            "base_image": WORKLOAD_IMAGE,
            "namespace": EXECUTOR_NAMESPACE,
        },
        trust=True,
    )

    juju.wait(
        lambda status: (
            jubilant.all_blocked(status, EXECUTOR_APP)
            and status.apps[EXECUTOR_APP].app_status.message
            == constants.MISSING_REQUIRES_RELATION_MESSAGE
        )
    )

    logger.info("Deploying mock-coordinator")

    juju.deploy(mock_coordinator_charm.resolve(), app=MOCK_COORDINATOR_APP)

    juju.wait(lambda status: jubilant.all_active(status, MOCK_COORDINATOR_APP))


def test_integrate_with_coordinator(juju: jubilant.Juju):
    """Integrate executor with the mock coordinator and verify active status."""
    logger.info("Integrating executor <-> mock coordinator")

    juju.integrate(
        f"{EXECUTOR_APP}:airflow-config",
        f"{MOCK_COORDINATOR_APP}:airflow-config",
    )

    juju.integrate(
        f"{EXECUTOR_APP}:airflow-executor-config",
        f"{MOCK_COORDINATOR_APP}:airflow-executor-config",
    )

    juju.wait(jubilant.all_active)


def test_executor_config_in_relation_databag(juju: jubilant.Juju):
    """Verify the executor shares config_template and pod spec via the relation databag."""
    unit_name = f"{EXECUTOR_APP}/0"
    raw = juju.cli("show-unit", unit_name, "--format", "json")
    unit_data = json.loads(raw)[unit_name]

    executor_relation = next(
        rel
        for rel in unit_data["relation-info"]
        if rel["endpoint"] == constants.AIRFLOW_CONFIG_PROVIDES_RELATION_NAME
    )

    app_data = executor_relation["application-data"]
    databag = json.loads(app_data["data"])["fixed_request_id"]

    assert "config-template" in databag, "config-template missing from executor relation databag"

    config_template = databag["config-template"]
    assert config_template["core"]["executor"] == "KubernetesExecutor"
    assert "namespace" in config_template["kubernetes_executor"]
    assert "pod_template_file" in config_template["kubernetes_executor"]
    assert "base_image" in config_template["kubernetes_executor"]

    assert "kubernetes-executor-pod-spec" in databag, (
        "kubernetes-executor-pod-spec missing from executor relation databag"
    )


def test_k8s_resources_exist():
    """Verify the namespace, ConfigMap, and Secret created by the executor exist."""
    sh.kubectl("get", "namespace", EXECUTOR_NAMESPACE)
    sh.kubectl("get", "configmap", constants.CONFIGMAP_NAME, "-n", EXECUTOR_NAMESPACE)
    sh.kubectl("get", "secret", constants.SECRET_NAME, "-n", EXECUTOR_NAMESPACE)
