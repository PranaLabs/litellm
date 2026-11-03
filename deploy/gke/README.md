# Small GKE deployment

This profile reuses a GKE cluster and runs one gateway worker, PostgreSQL 16 with a 20 GiB disk, and Redis 7.4 with a 5 GiB disk and AOF persistence. It is not highly available. A zone outage or database restart interrupts service. PVC retention protects against StatefulSet deletion but does not replace backups

Use a dedicated kubeconfig and namespace. Set `PROJECT`, `CLUSTER`, `ZONE`, and `DOMAIN` for your deployment. The manifests select the `default-pool` node pool and `standard-rwo` storage class. Check both exist before applying them

Private GKE nodes need an approved outbound path to public model providers. Private Google Access only covers Google APIs and services. Check for Cloud NAT or an existing outbound proxy before rollout. Do not enable shared-subnet egress without checking the impact on other workloads. If nodes cannot reach GHCR, mirror the verified image into a regional Artifact Registry repository and override `image.repository` and `image.tag` in a private values file. Preserve the upstream digest and verify that the mirrored platform manifest matches it. Give the node service account `roles/artifactregistry.reader` on that repository

```bash
export KUBECONFIG="$(mktemp)"
gcloud container clusters get-credentials "$CLUSTER" --project="$PROJECT" --zone="$ZONE"
kubectl create namespace litellm
```

Create `litellm-database` with `username=litellm` and a random `password`, and `litellm-runtime` with random `LITELLM_MASTER_KEY`, `LITELLM_SALT_KEY`, `UI_PASSWORD`, and `REDIS_PASSWORD`. Use a password manager or protected local files with `kubectl create secret generic --from-file`. The master key must start with `sk-`. Use URL-safe database passwords because the chart constructs a connection URL. Never put secrets in Helm values, shell history, Git, or command output. Preserve the salt key and database credentials for recovery. No `.env` file is needed

The gateway image is pinned by digest and was verified with the repository's `cosign.pub`. Reverify signatures when changing it. PostgreSQL and Redis use digest-pinned official images. Review release security advisories before upgrades

```bash
kubectl apply -f deploy/gke/dependencies.yaml
kubectl -n litellm rollout status statefulset/litellm-postgresql --timeout=5m
kubectl -n litellm rollout status statefulset/litellm-redis --timeout=5m
helm lint helm/litellm-helm -f deploy/gke/values.yaml
helm upgrade --install litellm helm/litellm-helm --namespace litellm \
  --values deploy/gke/values.yaml --wait --timeout 15m
```

The pre-install and pre-upgrade migration hook requires the database and secrets to exist first. Back up the database before every upgrade. Gateway schema updates are disabled because the hook owns migrations. Preserve the previous image digest and database backup together, since rolling back an image does not roll back the schema

For public HTTPS, reserve the address below and render the example domain into a temporary manifest. The GKE built-in Ingress controller uses the `gce` annotation

```bash
gcloud compute addresses create litellm-public-ip --global --project="$PROJECT"
gcloud compute addresses describe litellm-public-ip --global --project="$PROJECT" \
  --format='value(address)'
sed "s/llm.example.com/$DOMAIN/g" deploy/gke/ingress.yaml > /tmp/litellm-ingress.yaml
kubectl apply -f /tmp/litellm-ingress.yaml
kubectl -n litellm get ingress,managedcertificate
```

Create an unproxied DNS A record for `DOMAIN` pointing to that static IP. Wait for the managed certificate to become `Active` before sending credentials. The load balancer redirects HTTP to HTTPS and permits long streaming requests. PostgreSQL and Redis have only ClusterIP services. Other pods in the same cluster can reach the gateway at `http://litellm.litellm.svc.cluster.local:4000`. This address is not reachable from another cluster without additional networking

Sign in at `https://$DOMAIN/ui` with username `admin` and the private `UI_PASSWORD`. No upstream models are preconfigured. Add provider credentials through the authenticated administration interface, then issue scoped virtual keys to callers. Keep the master key for administration. Validate both regular and streaming requests against each real provider before giving callers access

For OpenRouter, add `OPENROUTER_API_KEY` to the existing `litellm-runtime` Secret without replacing its other keys. Pass `-f deploy/gke/openrouter-values.yaml` after the base values on every Helm install or upgrade for this profile, alongside any private image or domain overrides. The gateway routes `openrouter/<vendor>/<model>` through that account. Use an exact model ID from OpenRouter's current catalog. The wildcard route does not guarantee that this pinned LiteLLM version lists the entire upstream catalog through `/v1/models`. Issue virtual keys restricted to the models each caller needs. Restart the gateway after rotating its environment-based provider key

The OpenRouter profile also registers `openrouter/openai/gpt-6-luna` explicitly so clients can discover it through `/v1/models`. Include that exact ID in the caller key's `models` allowlist even when the key also grants `openrouter/*`, since this pinned release does not expand that grant for client listings. Add explicit entries for other models that callers need to select from a model picker

Verify `/health/readiness`, rejection of missing and invalid API keys, successful authenticated `/v1/models`, virtual-key permissions and revocation, and a real streaming completion. Check `kubectl -n litellm get pods,pvc` and confirm Redis AOF is enabled. Restart each dependency during a maintenance window and verify its data survives. HTTP health checks alone do not prove inference works

Set `umask 077`, then create a protected PostgreSQL backup with `kubectl -n litellm exec litellm-postgresql-0 -- pg_dump -U litellm -d litellm -Fc > backup.dump`. Encrypt and copy backups off the cluster, and test restoration into a separate database. This profile does not configure automatic backups. Do not delete the namespace or PVCs as a rollback procedure
