# airflow-kubernetes-executor-k8s

The Kubernetes Executor charm enables a Charmed Airflow deployment to run tasks as individual Pods in a Kubernetes cluster, providing strong isolation and elastic scaling.

The charm manages the Kubernetes resources (ConfigMap and Secret) required by Airflow worker Pods, and shares the executor-specific pod template and configuration with a requirer charm (normally the Airflow Coordinator).

## Usage

```
juju deploy airflow-kubernetes-executor-k8s --trust
juju deploy airflow-coordinator-k8s
juju integrate airflow-kubernetes-executor-k8s:airflow-config airflow-coordinator-k8s
```

## Configuration

| Option | Required | Description |
|---|---|---|
| `base_image` | Yes | OCI image to use for worker Pods |
| `namespace` | Yes | Kubernetes namespace in which worker Pods will be scheduled |
| `pod_name` | No | Base name for worker Pods (default: `airflow-worker`) |

## OCI Images

The `base_image` config option must point to an Airflow-compatible OCI image. For Local DAG bundle sources the image must contain the DAGs at the configured `dag_folder` path.
