# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
import unittest.mock

import charms.airflow_coordinator_k8s.v0.airflow_coordinator as airflow_coordinator
import ops.testing
import pytest

import constants
from charm import AirflowKubernetesExecutorK8SCharm
from k8s_manager import AirflowK8sManager

logger = logging.getLogger(__name__)

# Sample Airflow config template shared by the coordinator.
MOCK_CONFIG_TEMPLATE = (
    "[core]\nexecutor = {{ core__executor }}\n\n"
    "[database]\nsql_alchemy_conn = {{ database__sql_alchemy_conn }}\n"
)

# Sample sensitive data shared by the coordinator.
# Keys follow the {section}__{key} format so the charm can derive the
# Airflow env var name as "AIRFLOW__" + key.upper().
MOCK_SENSITIVE_DATA = {
    "core__fernet_key": "somekey",
    "database__sql_alchemy_conn": "postgresql+psycopg2://user:pass@host/db",
}

# Sample charm config covering all required options.
MOCK_CONFIG = {
    "base_image": "ubuntu/airflow:latest",
    "namespace": "mock-namespace",
}


@pytest.fixture(autouse=True)
def mock_lightkube_client():
    """Prevent unit tests from making real Kubernetes API calls."""
    with unittest.mock.patch.object(
        AirflowK8sManager,
        "k8s_resource_handler",
        return_value=unittest.mock.MagicMock(),
    ):
        yield


@pytest.fixture(scope="function")
def context():
    return ops.testing.Context(charm_type=AirflowKubernetesExecutorK8SCharm)


@pytest.fixture(scope="function")
def coordinator_relation():
    """A minimal coordinator relation with no data yet (coordinator not ready)."""
    return ops.testing.Relation(constants.AIRFLOW_CONFIG_REQUIRES_RELATION_NAME)


@pytest.fixture(scope="function")
def base_state(coordinator_relation):
    """Base state with coordinator relation present but not yet providing config."""
    return ops.testing.State(
        leader=True,
        relations=[coordinator_relation],
        config=MOCK_CONFIG,
    )


@pytest.fixture
def mock_apply_k8s_resources():
    """Mock _apply_k8s_resources to avoid live k8s calls."""
    with unittest.mock.patch.object(
        AirflowKubernetesExecutorK8SCharm,
        "_apply_k8s_resources",
    ) as mock:
        yield mock


@pytest.fixture
def mock_provider_content_ready():
    """Mock provider_content on the coordinator requires returning a ready model."""
    model = airflow_coordinator.AirflowCoordinatorProviderModel(
        config_template=MOCK_CONFIG_TEMPLATE,
        sensitive_data=json.dumps(MOCK_SENSITIVE_DATA),
    )
    with unittest.mock.patch.object(
        airflow_coordinator.AirflowCoordinatorRequires,
        "provider_content",
        new_callable=unittest.mock.PropertyMock,
        return_value=model,
    ):
        yield model


@pytest.fixture
def mock_render_pod_template():
    """Mock _render_pod_template to return a deterministic string."""
    with unittest.mock.patch.object(
        AirflowKubernetesExecutorK8SCharm,
        "_render_pod_template",
        return_value="apiVersion: v1\nkind: Pod\nmetadata:\n  name: airflow-worker\n",
    ) as mock:
        yield mock
