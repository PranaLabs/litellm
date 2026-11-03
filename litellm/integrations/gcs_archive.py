import asyncio
import hashlib
import os
import sqlite3
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Final, Protocol

from google.api_core.exceptions import GoogleAPIError, NotFound, PreconditionFailed
from google.auth.exceptions import GoogleAuthError
from google.cloud import storage  # pyright: ignore[reportMissingTypeStubs]  # Google Storage SDK ships no type stubs
from google.cloud.storage.exceptions import DataCorruption  # pyright: ignore[reportMissingTypeStubs]  # SDK lacks stubs
from google.cloud.storage.retry import DEFAULT_RETRY  # pyright: ignore[reportMissingTypeStubs]  # SDK has no type stubs
from pydantic import BaseModel, ConfigDict, Field, JsonValue, TypeAdapter, ValidationError
from requests.exceptions import RequestException

from litellm._logging import verbose_logger
from litellm.integrations.additional_logging_utils import AdditionalLoggingUtils
from litellm.integrations.custom_logger import CustomLogger
from litellm.litellm_core_utils.internal_call_metadata import logged_event_key
from litellm.repositories.archive_spool_repository import ArchiveFailure, ArchiveSpool
from litellm.types.integrations.base_health_check import IntegrationHealthCheckStatus


class ArchiveOwner(BaseModel):
    model_config = ConfigDict(frozen=True)
    user_api_key_user_id: str | None = None
    user_api_key_team_id: str | None = None


class ArchiveParameters(BaseModel):
    model_config = ConfigDict(frozen=True)
    tools: JsonValue = None
    tool_choice: JsonValue = None
    response_format: JsonValue = None


class ArchiveRecord(BaseModel):
    model_config = ConfigDict(frozen=True)
    id: str = Field(min_length=1)
    startTime: float
    trace_id: str | None = None
    session_id: str | None = None
    model: str | None = None
    status: str | None = None
    messages: JsonValue = None
    response: JsonValue = None
    model_parameters: ArchiveParameters | None = None
    metadata: ArchiveOwner = Field(default_factory=ArchiveOwner)


_PAYLOAD: Final = TypeAdapter(dict[str, JsonValue])


def archive_object_key(request_id: str) -> str:
    digest: Final = hashlib.sha256(request_id.encode()).hexdigest()
    return f"requests/v1/{digest[:2]}/{digest}.json"


class ArchiveObjects(Protocol):
    def write(self, key: str, payload: bytes) -> ArchiveFailure | None: ...

    def read(self, key: str) -> bytes | ArchiveFailure | None: ...

    def close(self) -> None: ...


class GCSArchiveObjects:
    def __init__(self, client: storage.Client, bucket_name: str) -> None:
        self.client = client
        self.bucket = client.bucket(bucket_name)  # pyright: ignore[reportUnknownMemberType]  # SDK bucket API is untyped
        self.lock = Lock()

    def write(self, key: str, payload: bytes) -> ArchiveFailure | None:
        with self.lock:
            blob: Final = self.bucket.blob(key)  # pyright: ignore[reportUnknownMemberType]  # SDK blob API is untyped
            try:
                blob.upload_from_string(  # pyright: ignore[reportUnknownMemberType]  # SDK upload API is untyped
                    payload,
                    content_type="application/json",
                    if_generation_match=0,
                    timeout=10,
                    retry=DEFAULT_RETRY.with_deadline(20),
                    checksum="crc32c",
                )
            except PreconditionFailed:
                existing: Final = self._read(key)
                return None if existing == payload else ArchiveFailure("object_conflict")
            except (GoogleAPIError, GoogleAuthError, RequestException, DataCorruption):
                return ArchiveFailure("gcs_write_failed")
            else:
                return None

    def _read(self, key: str) -> bytes | ArchiveFailure | None:
        try:
            return self.bucket.blob(key).download_as_bytes(  # pyright: ignore[reportUnknownMemberType]  # SDK is untyped
                timeout=10, retry=DEFAULT_RETRY.with_deadline(20), checksum="crc32c"
            )
        except NotFound:
            return None
        except (GoogleAPIError, GoogleAuthError, RequestException, DataCorruption):
            return ArchiveFailure("gcs_read_failed")

    def read(self, key: str) -> bytes | ArchiveFailure | None:
        with self.lock:
            return self._read(key)

    def close(self) -> None:
        with self.lock:
            self.client.close()


class GCSArchiveLogger(CustomLogger, AdditionalLoggingUtils):
    def __init__(self, objects: ArchiveObjects, spool: ArchiveSpool, flush_interval: float = 10) -> None:
        super().__init__()  # pyright: ignore[reportUnknownMemberType]  # Legacy CustomLogger accepts untyped kwargs
        self.objects = objects
        self.spool = spool
        self.flush_interval = flush_interval
        self.lock = asyncio.Lock()
        self.wakeup = asyncio.Event()
        self.stopping = asyncio.Event()
        self.worker: asyncio.Task[None] | None = None
        self.last_error: str | None = None

    def start(self) -> None:
        if self.worker is None:
            self.worker = asyncio.create_task(self._run())

    async def _run(self) -> None:
        while not self.stopping.is_set():
            self.wakeup.clear()
            await self.flush()
            try:
                await asyncio.wait_for(
                    (self.stopping if self.last_error else self.wakeup).wait(), timeout=self.flush_interval
                )
            except asyncio.TimeoutError:
                continue

    async def _record(self, kwargs: Mapping[str, object]) -> None:
        try:
            record: Final = ArchiveRecord.model_validate(kwargs.get("standard_logging_object"))
            canonical: Final = record.model_copy(update={"id": logged_event_key(kwargs, record.id)})
            result: Final = await asyncio.to_thread(
                self.spool.put, archive_object_key(canonical.id), canonical.model_dump_json(exclude_none=True).encode()
            )
        except (ValidationError, OSError, sqlite3.Error):
            self.last_error = "record_not_persisted"
            verbose_logger.error("GCS archive could not validate or persist a logging event")
            return
        if isinstance(result, ArchiveFailure):
            self.last_error = result.code
            verbose_logger.error("GCS archive could not persist a logging event (%s)", result.code)
            return
        self.wakeup.set()

    async def async_log_success_event(
        self,
        kwargs: Mapping[str, object],
        response_obj: object,
        start_time: datetime | None,
        end_time: datetime | None,
    ) -> None:
        await self._record(kwargs)

    async def async_log_failure_event(
        self,
        kwargs: Mapping[str, object],
        response_obj: object,
        start_time: datetime | None,
        end_time: datetime | None,
    ) -> None:
        await self._record(kwargs)

    async def flush(self) -> int:
        async with self.lock:
            try:
                pending: Final = await asyncio.to_thread(self.spool.pending_keys, 64)
                handled: Final = await self._flush_pending(pending)
                if pending and handled == len(pending):
                    self.last_error = None
            except (OSError, sqlite3.Error):
                self.last_error = "spool_unavailable"
                verbose_logger.error("GCS archive queue unavailable; retained records will be retried")
                return 0
            else:
                return handled

    async def _flush_pending(self, keys: tuple[str, ...]) -> int:
        for index, key in enumerate(keys):
            if self.stopping.is_set():
                return index
            if not await self._upload_pending(key):
                await asyncio.to_thread(self.spool.defer, key)
                return index
        return len(keys)

    async def _upload_pending(self, key: str) -> bool:
        payload: Final = await asyncio.to_thread(self.spool.read, key)
        return True if payload is None else await self._upload(key, payload)

    async def _upload(self, key: str, payload: bytes) -> bool:
        result: Final = await asyncio.to_thread(self.objects.write, key, payload)
        if isinstance(result, ArchiveFailure):
            self.last_error = result.code
            verbose_logger.error("GCS archive upload failed (%s); retained for retry", result.code)
            return False
        await asyncio.to_thread(self.spool.acknowledge, key, payload)
        return True

    async def get_request_response_payload(
        self, request_id: str, start_time_utc: datetime | None, end_time_utc: datetime | None
    ) -> dict[str, JsonValue] | None:  # mutable-ok: AdditionalLoggingUtils requires a dict for the existing UI contract
        key: Final = archive_object_key(request_id)
        pending: Final = await asyncio.to_thread(self.spool.read, key)
        result: Final = pending if pending is not None else await asyncio.to_thread(self.objects.read, key)
        match result:
            case ArchiveFailure(code=code):
                raise RuntimeError(f"GCS archive read failed ({code})")
            case _:
                pass
        if result is None:
            return None
        record: Final = ArchiveRecord.model_validate_json(result)
        if record.id != request_id:
            raise ValueError("Archived request ID does not match the requested log")
        return _PAYLOAD.validate_json(record.model_dump_json(exclude_none=True))

    async def async_health_check(self) -> IntegrationHealthCheckStatus:
        result: Final = await asyncio.to_thread(self.objects.write, "health/v1/probe.json", b'{"version":1}')
        failure: Final = result.code if isinstance(result, ArchiveFailure) else self.last_error
        return {"status": "unhealthy" if failure else "healthy", "error_message": failure}

    async def aclose(self) -> None:
        self.stopping.set()
        self.wakeup.set()
        if self.worker is not None:
            await self.worker
        async with self.lock:
            await asyncio.to_thread(self.objects.close)
            await asyncio.to_thread(self.spool.close)


def create_gcs_archive_logger() -> GCSArchiveLogger:
    bucket: Final = os.environ["GCS_ARCHIVE_BUCKET"]
    spool: Final = ArchiveSpool(Path(os.environ["GCS_ARCHIVE_SPOOL_DIR"]) / "pending.sqlite")
    objects: Final = GCSArchiveObjects(storage.Client(), bucket)
    logger: Final = GCSArchiveLogger(objects, spool)
    logger.start()
    return logger
