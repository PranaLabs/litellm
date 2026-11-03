# GCS request archive

This optional plugin stores completed model requests and responses in a private GCS bucket. PostgreSQL keeps the spend-log rows used for listings and authorization. The existing `/spend/logs/ui/{request_id}` endpoint reads archived bodies through `AdditionalLoggingUtils`, so the dashboard does not need a new API. No Enterprise callback or license check is modified

Build an image from this checkout with the `proxy` extra, which includes `google-cloud-storage`. The upstream image pinned in `values.yaml` does not contain this plugin. Set your verified image repository and digest in a private values file before enabling this overlay

## Authentication and storage

Create a private bucket in the gateway's region. Use GKE Workload Identity Federation and grant the gateway identity `roles/storage.objectCreator` and `roles/storage.objectViewer` on that bucket only. The SDK uses Application Default Credentials. Do not put service account keys in the image or Helm values

The object name is `requests/v1/<hash-prefix>/<sha256(request-id)>.json`. A dedicated bucket avoids collisions between independent gateways. Objects are created with a generation precondition and CRC32C checksums. Duplicate delivery succeeds only if the existing bytes match. Conflicting content is retained locally and reported as an error, never overwritten

The plugin stores messages, the final response, request/session/trace identifiers, model, status, tool definitions, tool choice, response format, and user/team ownership. It deliberately omits transport headers, credentials, arbitrary metadata and provider configuration. It does not record local Agent execution that never passes through the gateway

The persistent spool is a SQLite queue on its own PVC, not another database service. Each spool instance owns its connection and serializes access; `aclose()` closes it after the uploader stops. Transactions use SQLite's default FULL synchronous rollback journal. A successful GCS write is acknowledged locally afterward. A restart replays unacknowledged records, including a write that succeeded remotely before its local acknowledgement. Multiple workers can share the queue on the same local filesystem

Pending payloads are capped at 1 GiB. The PVC also needs room for SQLite pages and journals. A full or unavailable spool reports an error and rejects the new logging event without removing existing records. This is a best-effort callback, not a lossless audit log. A process can exit before LiteLLM dispatches a callback or before its queue transaction completes. Monitor callback errors and storage health. Never treat successful inference as proof that its archive was stored

Only final streaming completions are archived. Individual stream chunks are ignored. A failed stream may not expose a complete response. Upload retries run on startup, when events arrive, and periodically. Graceful users of the class should await `aclose()` before closing their event loop. Pending records remain durable if the process terminates during upload

Health checks create or verify the fixed `health/v1/probe.json` object to test actual write access. A missing bucket or denied write is unhealthy. Failed uploads move to the end of the queue and wait for the next retry interval, so one conflicting object cannot permanently block later records

## Enable on GKE

Keep the existing database, Redis and OpenRouter overlays. This overlay replaces the callback and volume lists, so merge any additional callbacks or mounts explicitly

```bash
kubectl -n litellm apply -f deploy/gke/gcs-archive-spool.yaml
kubectl -n litellm create configmap litellm-archive-callback \
  --from-file=deploy/gke/gcs_archive_callback.py \
  --dry-run=client -o yaml | kubectl apply -f -
```

Add `envVars.GCS_ARCHIVE_BUCKET` to the private values file and configure the Kubernetes service account's Workload Identity binding. Use a dedicated bucket with no public access. The bucket name is configuration, not a credential

```bash
helm upgrade litellm helm/litellm-helm -n litellm \
  -f deploy/gke/values.yaml \
  -f deploy/gke/openrouter-values.yaml \
  -f deploy/gke/gcs-archive-values.yaml \
  -f /path/to/private-values.yaml \
  --wait --timeout 15m
```

The PVC is ReadWriteOnce. This overlay uses one replica and the Recreate strategy so an upgrade cannot strand a second pod waiting for the same disk on another node. Upgrades therefore briefly interrupt gateway service. Use a separate spool per replica before scaling across nodes. Preserve the spool PVC during rollback or node replacement. Do not use an `emptyDir` for the queue

Keep `store_prompts_in_spend_logs` disabled and `turn_off_message_logging` disabled as shown in the overlay. The overlay enables `request_correlation_in_logs` so supplied session IDs reach the standard payload. Check for database-backed UI settings overriding the YAML. A global or per-request redaction setting can still redact what reaches the callback

The default PostgreSQL spend metadata contains the request ID and owner needed for UI retrieval. Do not remove owner metadata from archived objects. The built-in log detail endpoint checks ownership before returning content to non-admin users. A GCS read failure surfaces as an error rather than an empty body

Choose bucket lifecycle and PostgreSQL retention together. Deleting a spend-log row removes it from normal UI listings even if its GCS object remains. Deleting an object leaves a log row whose body is no longer available. This plugin does not configure retention or delete archived objects

## Validate and roll back

Run `uv run --no-sync pytest tests/unit/integrations/test_gcs_archive.py tests/unit/repositories/test_archive_spool_repository.py`. Validate a real regular request, a streaming request, a failed request, and UI log detail retrieval. Check the stored object's owner and confirm another user's key cannot read it. Verify PostgreSQL has metadata without duplicate bodies. Interrupt object-store access, confirm records remain queued, restore access, and verify replay. Repeat with a gateway restart while records are pending

To stop new collection, remove this overlay and restore the previous callback settings. Keep the spool and bucket until pending records have been handled. Rollback does not delete stored content
