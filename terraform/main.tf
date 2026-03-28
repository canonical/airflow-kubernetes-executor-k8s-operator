resource "juju_application" "airflow_kubernetes_executor_k8s" {
  name       = var.app_name
  model_uuid = var.model_uuid

  charm {
    name     = "airflow-kubernetes-executor-k8s"
    revision = var.revision
    channel  = var.channel
  }

  constraints = var.constraints
  config      = var.config
  trust       = var.trust

  units = var.units
}
