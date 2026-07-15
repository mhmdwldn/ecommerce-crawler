# Helm Chart — E-Commerce Crawler Pipeline

Deploy full stack (18 services) ke Kubernetes.

## Quick Start

```bash
# 1. Install chart dengan semua service
helm install ecommerce-crawler ./deployment/helm

# 2. Override environment
helm install ecommerce-crawler ./deployment/helm \
  --set global.environment=staging \
  --set airflow.replicas=2

# 3. Minimal deployment (hanya pipeline services)
helm install ecommerce-crawler ./deployment/helm \
  --set elasticsearch.enabled=false \
  --set metabase.enabled=false \
  --set superset.enabled=false

# 4. Upgrade
helm upgrade ecommerce-crawler ./deployment/helm --set global.imageTag=latest

# 5. Uninstall
helm uninstall ecommerce-crawler
```

## Architecture

Chart ini mengkonversi `source/deployment/compose.yaml` ke resource K8s:

| Compose Service | K8s Resource | Notes |
|---|---|---|
| `airflow` | Deployment (1 replica) | Image dari GHCR |
| `postgres` | StatefulSet + PVC | Persistent storage |
| `clickhouse` | StatefulSet + PVC | Persistent storage |
| `kafka` | StatefulSet + PVC | Single broker (dev) |
| `minio` | StatefulSet + PVC | S3-compatible storage |
| `prometheus` | Deployment + PVC | 7-day TSDB retention |
| `grafana` | Deployment | Dashboard pre-loaded |
| `caddy` | Deployment + Service (LoadBalancer) | Reverse proxy ingress |
| Lainnya | Deployment | Stateless services |

## Production Considerations

1. **Secrets:** Ganti password di values.yaml dengan `Secret` resource + external secret manager (Vault/AWS Secrets Manager)
2. **Storage:** Ganti PVC dengan cloud-native storage (EBS/EFS untuk AWS, Persistent Disk untuk GCP)
3. **Ingress:** Ganti Caddy dengan cloud ingress (ALB Ingress Controller, nginx-ingress)
4. **Kafka:** Gunakan Strimzi operator atau managed Kafka (MSK) untuk production
5. **Monitoring:** Tambah ServiceMonitor untuk integrasi dengan Prometheus Operator
6. **Autoscaling:** Tambah HPA (HorizontalPodAutoscaler) untuk Airflow worker
