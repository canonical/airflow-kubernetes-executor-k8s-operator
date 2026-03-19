# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Kubernetes resource management for the Airflow Kubernetes Executor charm."""

import logging

from charmed_kubeflow_chisme.kubernetes import (
    KubernetesResourceHandler,
    create_charm_default_labels,
)
from lightkube.generic_resource import load_in_cluster_generic_resources
from lightkube.resources.core_v1 import ConfigMap, Secret

import constants

logger = logging.getLogger(__name__)


class AirflowK8sManager:
    """Manages Kubernetes resources for the Airflow Kubernetes Executor charm.

    Wraps KubernetesResourceHandler to apply and delete the ConfigMap and
    Secret resources required by Airflow worker Pods.
    """

    def __init__(self, app_name: str, model_name: str, field_manager: str = "lightkube"):
        """Initialise the Kubernetes resource manager.

        Args:
            app_name: Juju application name, used for labelling K8s resources.
            model_name: Juju model name, used for labelling K8s resources.
            field_manager: Field-manager name passed to server-side apply.
        """
        self._app_name = app_name
        self._model_name = model_name
        self._field_manager = field_manager
        self._handler: KubernetesResourceHandler | None = None

    def _labels(self) -> dict:
        """Return default labels to apply to all managed resources."""
        return create_charm_default_labels(
            application_name=self._app_name,
            model_name=self._model_name,
            scope="all-resources",
        )

    def k8s_resource_handler(self, context: dict) -> KubernetesResourceHandler:
        """Return the resource handler, creating it on first use.

        The context is refreshed on every call so that apply/delete always
        use the latest charm configuration.

        Args:
            context: Template rendering context (namespace, config data, etc.).

        Returns:
            A KubernetesResourceHandler ready to apply or delete resources.
        """
        if self._handler is None:
            self._handler = KubernetesResourceHandler(
                field_manager=self._field_manager,
                template_files=constants.K8S_RESOURCE_FILES,
                context=context,
                logger=logger,
                labels=self._labels(),
                resource_types={ConfigMap, Secret},
            )
        else:
            self._handler.context = context
        load_in_cluster_generic_resources(self._handler.lightkube_client)
        return self._handler
