# CC008: charm modules must output the deployed application object.
output "application" {
  value = juju_application.airflow_kubernetes_executor_k8s
}

output "provides" {
  value = {
    airflow_kubernetes_executor = "airflow-executor-config"
  }
}

output "requires" {
  value = {
    airflow_coordinator = "airflow-config"
  }
}
