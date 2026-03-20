#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Mock Airflow Coordinator charm for integration testing."""

import logging

import charms.airflow_coordinator_k8s.v0.airflow_coordinator as airflow_coordinator
import ops

logger = logging.getLogger(__name__)

CONFIG_TEMPLATE = "[core]\ndags_folder = /opt/airflow/dags\nexecutor = LocalExecutor"
SENSITIVE_DATA = {
    "database__sql_alchemy_conn": "postgresql+psycopg2://airflow:password@postgres:5432/airflow",
}


class MockCoordinatorCharm(ops.CharmBase):
    """Mock Airflow Coordinator for testing the executor charm."""

    def __init__(self, framework: ops.Framework):
        super().__init__(framework)

        self._provider_handler = airflow_coordinator.AirflowCoordinatorProviderEventHandler(
            self, "airflow-config", airflow_coordinator.AirflowCoordinatorRequirerModel
        )

        self._executor_config = airflow_coordinator.AirflowCoordinatorRequires(
            self,
            "airflow-executor-config",
            callback=self._on_executor_config_changed,
        )

        self.framework.observe(self.on.start, self._set_active)
        self.framework.observe(self.on.update_status, self._set_active)
        self.framework.observe(
            self.on["airflow-config"].relation_joined,
            self._on_config_relation_joined,
        )

    def _set_active(self, _):
        """Set the unit status to active."""
        self.unit.status = ops.ActiveStatus()

    def _on_executor_config_changed(self, _):
        """Handle executor config relation changes."""
        self.unit.status = ops.ActiveStatus()

    def _share_config(self, config_template=CONFIG_TEMPLATE, sensitive_data=None):
        """Share config template and sensitive data with related charms."""
        if sensitive_data is None:
            sensitive_data = SENSITIVE_DATA
        self._provider_handler.update_content(
            config_template=config_template,
            sensitive_data=sensitive_data,
        )

    def _on_config_relation_joined(self, _):
        """Share default config when the airflow-config relation is joined."""
        self._share_config()
        self.unit.status = ops.ActiveStatus()


if __name__ == "__main__":
    ops.main(MockCoordinatorCharm)
