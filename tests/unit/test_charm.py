# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

import dataclasses
import json
import unittest.mock

import charms.airflow_coordinator_k8s.v0.airflow_coordinator as airflow_coordinator
import ops
import ops.testing

import constants
from charm import AirflowKubernetesExecutorK8SCharm
from tests.unit.conftest import MOCK_CONFIG, MOCK_CONFIG_TEMPLATE, MOCK_SENSITIVE_DATA


def test_non_leader_unit_does_not_reconcile(context, coordinator_relation):
    """Non-leader units must not run reconciliation logic."""
    state = ops.testing.State(leader=False, relations=[coordinator_relation])
    status_before = state.unit_status

    state_out = context.run(context.on.start(), state)

    assert state_out.unit_status == status_before


def test_missing_coordinator_relation_sets_blocked(context):
    """Without the coordinator relation the unit must go to BlockedStatus."""
    state = ops.testing.State(leader=True, relations=[], config=MOCK_CONFIG)

    state_out = context.run(context.on.start(), state)

    assert state_out.unit_status == ops.BlockedStatus(constants.MISSING_REQUIRES_RELATION_MESSAGE)


def test_no_config_from_coordinator_sets_waiting(context, base_state):
    """No coordinator config available yet → WaitingStatus."""
    with unittest.mock.patch.object(
        airflow_coordinator.AirflowCoordinatorRequires,
        "provider_content",
        new_callable=unittest.mock.PropertyMock,
        return_value=None,
    ):
        state_out = context.run(context.on.start(), base_state)

    assert state_out.unit_status == ops.WaitingStatus(
        constants.WAITING_FOR_COORDINATOR_CONFIG_MESSAGE
    )


def test_config_present_but_no_sensitive_data_sets_waiting(context, base_state):
    """Config template present but sensitive data missing → WaitingStatus."""
    model = airflow_coordinator.AirflowCoordinatorProviderModel(
        config_template="[core]\nexecutor = LocalExecutor\n",
        sensitive_data=None,
    )
    with unittest.mock.patch.object(
        airflow_coordinator.AirflowCoordinatorRequires,
        "provider_content",
        new_callable=unittest.mock.PropertyMock,
        return_value=model,
    ):
        state_out = context.run(context.on.start(), base_state)

    assert state_out.unit_status == ops.WaitingStatus(
        constants.WAITING_FOR_COORDINATOR_READY_MESSAGE
    )


def test_active_when_coordinator_ready(
    context,
    base_state,
    mock_provider_content_ready,
    mock_apply_k8s_resources,
):
    """When coordinator provides config + sensitive data → ActiveStatus."""
    state_out = context.run(context.on.start(), base_state)

    assert state_out.unit_status == ops.ActiveStatus()
    mock_apply_k8s_resources.assert_called_once()


def test_update_status_triggers_reconcile(
    context,
    base_state,
    mock_provider_content_ready,
    mock_apply_k8s_resources,
):
    """update-status event must also trigger reconciliation and reach ActiveStatus."""
    state_out = context.run(context.on.update_status(), base_state)

    assert state_out.unit_status == ops.ActiveStatus()


def test_config_changed_sends_executor_metadata(
    context,
    base_state,
    mock_provider_content_ready,
    mock_apply_k8s_resources,
):
    """On ConfigChangedEvent, _broadcast_executor_metadata must be called."""
    with unittest.mock.patch.object(
        AirflowKubernetesExecutorK8SCharm,
        "_broadcast_executor_metadata",
    ) as mock_broadcast:
        state_out = context.run(context.on.config_changed(), base_state)

    mock_broadcast.assert_called_once()
    assert state_out.unit_status == ops.ActiveStatus()


def test_config_changed_does_not_send_metadata_without_relation(context):
    """Config changed without coordinator relation → Blocked, no metadata sent."""
    state = ops.testing.State(leader=True, relations=[], config=MOCK_CONFIG)

    with unittest.mock.patch.object(
        AirflowKubernetesExecutorK8SCharm,
        "_broadcast_executor_metadata",
    ) as mock_broadcast:
        state_out = context.run(context.on.config_changed(), state)

    assert state_out.unit_status == ops.BlockedStatus(constants.MISSING_REQUIRES_RELATION_MESSAGE)
    mock_broadcast.assert_not_called()


def test_k8s_resources_applied_with_correct_namespace(
    context,
    base_state,
    mock_provider_content_ready,
    mock_apply_k8s_resources,
):
    """_apply_k8s_resources is called and context contains namespace from charm config."""
    state = dataclasses.replace(
        base_state,
        config={"base_image": "ubuntu/airflow:latest", "namespace": "my-namespace"},
    )

    with context(context.on.start(), state) as manager:
        charm = manager.charm
        assert charm._context["namespace"] == "my-namespace"

    mock_apply_k8s_resources.assert_called_once()


def test_k8s_resources_failure_sets_blocked(
    context,
    base_state,
    mock_provider_content_ready,
):
    """When resource application raises, the charm goes Blocked."""
    with unittest.mock.patch.object(
        AirflowKubernetesExecutorK8SCharm,
        "_apply_k8s_resources",
        side_effect=RuntimeError("k8s error"),
    ):
        state_out = context.run(context.on.start(), base_state)

    assert state_out.unit_status == ops.BlockedStatus(constants.K8S_RESOURCES_APPLY_FAILED_MESSAGE)


class TestPodTemplateRendering:
    def test_render_pod_template_uses_config_values(self, context, base_state):
        """Rendered pod template contains values from charm config."""
        state = dataclasses.replace(
            base_state,
            config={
                "pod_name": "my-worker",
                "base_image": "myregistry/airflow:latest",
                "namespace": "production",
            },
        )
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
            with context(context.on.start(), state) as manager:
                charm = manager.charm
                rendered = charm._render_pod_template()

        assert "my-worker" in rendered
        assert "myregistry/airflow:latest" in rendered
        assert "production" in rendered

    def test_render_pod_template_includes_extra_env(self, context, base_state):
        """Sensitive keys from the coordinator become env vars with the AIRFLOW__ prefix."""
        sensitive_data = {**MOCK_SENSITIVE_DATA, "core__secret_key": "secret"}
        model = airflow_coordinator.AirflowCoordinatorProviderModel(
            config_template=MOCK_CONFIG_TEMPLATE,
            sensitive_data=json.dumps(sensitive_data),
        )

        with unittest.mock.patch.object(
            airflow_coordinator.AirflowCoordinatorRequires,
            "provider_content",
            new_callable=unittest.mock.PropertyMock,
            return_value=model,
        ):
            with context(context.on.start(), base_state) as manager:
                charm = manager.charm
                rendered = charm._render_pod_template()

        assert "AIRFLOW__CORE__SECRET_KEY" in rendered

    def test_render_pod_template_injects_spark_env_from_extra_data(self, context, base_state):
        """Spark namespace/username from extra_data become plain env vars in the pod template."""
        model = airflow_coordinator.AirflowCoordinatorProviderModel(
            config_template=MOCK_CONFIG_TEMPLATE,
            sensitive_data=json.dumps(MOCK_SENSITIVE_DATA),
            extra_data={
                constants.SPARK_NAMESPACE_KEY: "airflow-spark",
                constants.SPARK_USERNAME_KEY: "spark",
            },
        )

        with unittest.mock.patch.object(
            airflow_coordinator.AirflowCoordinatorRequires,
            "provider_content",
            new_callable=unittest.mock.PropertyMock,
            return_value=model,
        ):
            with context(context.on.start(), base_state) as manager:
                rendered = manager.charm._render_pod_template()

        assert "SPARK_NAMESPACE" in rendered
        assert "airflow-spark" in rendered
        assert "SPARK_USERNAME" in rendered
        assert "serviceAccountName: spark" in rendered

    def test_render_pod_template_no_spark_without_extra_data(self, context, base_state):
        """Without extra_data, no spark env vars or serviceAccountName are rendered."""
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
            with context(context.on.start(), base_state) as manager:
                rendered = manager.charm._render_pod_template()

        assert "SPARK_NAMESPACE" not in rendered
        assert "serviceAccountName" not in rendered


class TestExecutorConfig:
    def test_build_executor_config_contains_required_keys(self, context, base_state):
        """Executor config must contain the executor type and kubernetes_executor section."""
        with context(context.on.start(), base_state) as manager:
            charm = manager.charm
            config = charm._build_executor_config()

        assert config["core"]["executor"] == "KubernetesExecutor"
        assert "namespace" in config["kubernetes_executor"]
        assert "pod_template_file" in config["kubernetes_executor"]

    def test_build_executor_config_uses_namespace_from_config(self, context, base_state):
        """Executor config namespace must reflect the charm's namespace config option."""
        state = dataclasses.replace(
            base_state, config={"base_image": "ubuntu/airflow:latest", "namespace": "custom-ns"}
        )

        with context(context.on.start(), state) as manager:
            charm = manager.charm
            config = charm._build_executor_config()

        assert config["kubernetes_executor"]["namespace"] == "custom-ns"

    def test_build_executor_config_pod_template_file_path(self, context, base_state):
        """Executor config pod_template_file must match the constant path."""
        with context(context.on.start(), base_state) as manager:
            charm = manager.charm
            config = charm._build_executor_config()

        assert config["kubernetes_executor"]["pod_template_file"] == (
            constants.AIRFLOW_POD_TEMPLATE_FILE_PATH
        )
