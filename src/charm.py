#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""The Airflow Kubernetes Executor charm application."""

import json
import logging
import pathlib

import charms.airflow_coordinator_k8s.v0.airflow_coordinator as airflow_coordinator
import jinja2
import ops
from lightkube import ApiError

import constants
from k8s_manager import AirflowK8sManager

logger = logging.getLogger(__name__)

# Template rendering control keys that are not stored in the K8s Secret and
# must be excluded from extra_env injection.
_NON_SECRET_KEYS = frozenset({"render_sensitive_data"})


class ExitWithStatusError(Exception):
    """Base class of exceptions for when a method has an opinion on the unit status."""

    def __init__(self, msg: str, status_type):
        super().__init__(str(msg))
        self.msg = str(msg)
        self.status_type = status_type

    @property
    def status(self):
        """Return an instance of self.status_type with a message."""
        return self.status_type(self.msg)


class AirflowKubernetesExecutorK8SCharm(ops.CharmBase):
    """Charm the Airflow Kubernetes Executor."""

    def __init__(self, framework: ops.Framework):
        super().__init__(framework)

        self.config_requires = airflow_coordinator.AirflowCoordinatorRequires(
            self,
            constants.AIRFLOW_CONFIG_REQUIRES_RELATION_NAME,
            callback=self._reconcile,
        )

        self.config_provides = airflow_coordinator.AirflowCoordinatorProvides(
            self,
            constants.AIRFLOW_CONFIG_PROVIDES_RELATION_NAME,
            callback=self._reconcile,
            dependencies_check_callable=self._required_dependencies_exist,
        )

        self._k8s_manager = AirflowK8sManager(
            app_name=self.app.name,
            model_name=self.model.name,
        )

        for event in [
            self.on.start,
            self.on.config_changed,
            self.on.update_status,
        ]:
            self.framework.observe(event, self._reconcile)

    def _required_dependencies_exist(self) -> bool:
        """Return True if all required dependencies for the coordinator exist."""
        return bool(self.model.get_relation(constants.AIRFLOW_CONFIG_REQUIRES_RELATION_NAME))

    def _build_executor_config(self) -> dict:
        """Build the executor config key-value pairs to share with the coordinator.

        Returns a nested dict keyed by airflow.cfg section, e.g.:
            {"core": {"executor": "KubernetesExecutor"}, "kubernetes_executor": {...}}
        The requirer side reads these pairs and incorporates them into the full airflow.cfg.
        """
        return {
            "core": {
                "executor": "KubernetesExecutor",
            },
            "kubernetes_executor": {
                "namespace": self.config["namespace"],
                "pod_template_file": constants.AIRFLOW_POD_TEMPLATE_FILE_PATH,
                "base_image": self.config["base_image"],
            },
        }

    def _check_for_required_configs(self) -> None:
        """Check that required charm config options are set, otherwise raise.

        Raises:
            ExitWithStatusError: If base_image or namespace are not configured.
        """
        missing = [key for key in ("base_image", "namespace") if not self.config.get(key)]
        if missing:
            raise ExitWithStatusError(
                f"Missing required config: {', '.join(missing)}", ops.BlockedStatus
            )

    def _render_pod_template(self) -> str:
        """Render the pod template Jinja2 template using charm config."""
        provider_content = self.config_requires.provider_content
        sensitive_data = json.loads(provider_content.sensitive_data) if provider_content and provider_content.sensitive_data else {}

        # We will assume that coordinator always send these values in the format
        # <section>__<key>, exactly as they are shown in the Airflow Configuration
        # documentation.
        extra_env = [
            {"name": "AIRFLOW__" + key.upper(), "secret_key": key}
            for key in sensitive_data
            if key not in _NON_SECRET_KEYS
        ]

        template_str = pathlib.Path(constants.POD_TEMPLATE_PATH).read_text()
        return jinja2.Template(template_str).render(
            pod_name=self.config["pod_name"],
            base_image=self.config["base_image"],
            namespace=self.config["namespace"],
            extra_env=extra_env,
        )

    @property
    def _context(self) -> dict:
        """Build the Jinja2 context for rendering Kubernetes resource templates."""
        provider_content = self.config_requires.provider_content
        config_template = provider_content.config_template if provider_content else None
        sensitive_data_raw = provider_content.sensitive_data if provider_content else None
        sensitive_data = json.loads(sensitive_data_raw) if sensitive_data_raw else {}

        # Render only the non-sensitive section of the config for the ConfigMap.
        # sensitive_data is passed so the template can resolve all variable references
        # without raising UndefinedError; render_sensitive_data=False instructs the
        # template to skip any sensitive blocks, so those values never appear in output.
        rendered_config = (
            jinja2.Template(config_template).render(
                **{**sensitive_data, "render_sensitive_data": False}
            )
            if config_template
            else ""
        )

        # Strip the rendering-control flag before storing values in the K8s Secret.
        secret_data = {k: v for k, v in sensitive_data.items() if k != "render_sensitive_data"}

        return {
            "app_name": self.app.name,
            "model_name": self.model.name,
            "configmap_name": constants.CONFIGMAP_NAME,
            "secret_name": constants.SECRET_NAME,
            "airflow_config": rendered_config,
            "sensitive_data": secret_data,
            "namespace": self.config["namespace"],
        }

    def _apply_k8s_resources(self) -> None:
        """Apply Kubernetes resources, retrying with force on field-manager conflicts."""
        try:
            self._k8s_manager.k8s_resource_handler(self._context).apply()
        except ApiError as error:
            if error.status.code == 409:
                logger.warning("Encountered a conflict: %s", error)
                self.unit.status = ops.MaintenanceStatus("Force applying K8S resources")
                logger.warning("Apply K8S resources with forced changes against conflicts")
                self._k8s_manager.k8s_resource_handler(self._context).apply(force=True)
            else:
                raise ExitWithStatusError(
                    constants.K8S_RESOURCES_APPLY_FAILED_MESSAGE, ops.BlockedStatus
                )

    def _check_required_relation_and_act(self) -> None:
        """Verify the coordinator relation exists, otherwise raise.

        If the relation is absent the K8s resources are removed so worker pods
        cannot be scheduled with stale configuration.

        Raises:
            ExitWithStatusError: If the airflow-coordinator relation is missing.
        """
        if not self.model.get_relation(constants.AIRFLOW_CONFIG_REQUIRES_RELATION_NAME):
            self._k8s_manager.k8s_resource_handler(context={}).delete()
            raise ExitWithStatusError(
                constants.MISSING_REQUIRES_RELATION_MESSAGE, ops.BlockedStatus
            )

    def _check_provider_content_and_act(self) -> None:
        """Verify the coordinator has shared config, otherwise raise.

        Raises:
            ExitWithStatusError: If the coordinator config or sensitive data is missing.
        """
        provider_content = self.config_requires.provider_content
        if not provider_content:
            raise ExitWithStatusError(
                constants.WAITING_FOR_COORDINATOR_CONFIG_MESSAGE, ops.WaitingStatus
            )
        if not provider_content.sensitive_data:
            raise ExitWithStatusError(
                constants.WAITING_FOR_COORDINATOR_READY_MESSAGE, ops.WaitingStatus
            )

    def _broadcast_executor_metadata(self) -> None:
        """Publish executor config and pod template to all related charms."""
        self.config_provides.set_validation_errors()
        self.config_provides.set_airflow_config(
            config_template=json.dumps(self._build_executor_config()),
            k8s_executor_pod_spec_template=self._render_pod_template(),
        )

    def _reconcile(self, _) -> None:
        """Reconcile the state of the charm for any event by running all operations."""
        if not self.unit.is_leader():
            return

        try:
            self._check_for_required_configs()
            self._check_required_relation_and_act()
            self._check_provider_content_and_act()
            self._apply_k8s_resources()
            self._broadcast_executor_metadata()
        except ExitWithStatusError as e:
            self.unit.status = e.status
            return
        except Exception:
            logger.exception("Unexpected error during K8s resource application")
            self.unit.status = ops.BlockedStatus(constants.K8S_RESOURCES_APPLY_FAILED_MESSAGE)
            return

        self.unit.status = ops.ActiveStatus()


if __name__ == "__main__":  # pragma: nocover
    ops.main(AirflowKubernetesExecutorK8SCharm)
