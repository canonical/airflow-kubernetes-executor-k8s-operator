# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Constants for the Airflow Kubernetes Executor charm."""

AIRFLOW_CONFIG_REQUIRES_RELATION_NAME = "airflow-config"
AIRFLOW_CONFIG_PROVIDES_RELATION_NAME = "airflow-executor-config"

# Names of the Kubernetes resources created by this charm.
CONFIGMAP_NAME = "airflow-kubernetes-executor-managed-configmap"
SECRET_NAME = "airflow-secret-config"

# Path to the pod template Jinja2 template file (relative to charm root).
POD_TEMPLATE_PATH = "src/templates/pod_template.yaml.j2"

# Airflow configuration sections sent to the coordinator.
EXECUTOR_SECTION = "executor"
KUBERNETES_EXECUTOR_SECTION = "kubernetes_executor"

# Airflow pod template file path inside the Scheduler workload container.
# The coordinator distributes this path to the scheduler, which writes the
# rendered pod spec there.
AIRFLOW_POD_TEMPLATE_FILE_PATH = "/opt/airflow/pod_templates/worker_pod_template.yaml"

# Keys used in the extra_data dict received from the coordinator, populated from the
# Spark Integration Hub relation (spark-service-account interface).
SPARK_NAMESPACE_KEY = "spark_namespace"
SPARK_USERNAME_KEY = "spark_username"

# Jinja2 template files rendered into Kubernetes resources by KubernetesResourceHandler.
K8S_RESOURCE_FILES = [
    "src/templates/configmap.j2",
    "src/templates/secret.j2",
]

# Status messages
MISSING_REQUIRES_RELATION_MESSAGE = "Missing required 'airflow-config' relation with coordinator"
WAITING_FOR_COORDINATOR_CONFIG_MESSAGE = "Waiting for config in 'airflow-config' requires relation"
WAITING_FOR_COORDINATOR_READY_MESSAGE = "Waiting for coordinator config and sensitive data"
K8S_RESOURCES_APPLY_FAILED_MESSAGE = "Failed to apply Kubernetes resources"
