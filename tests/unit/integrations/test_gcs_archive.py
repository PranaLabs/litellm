import asyncio
from datetime import datetime, timezone
from threading import Event
from urllib.parse import parse_qs, urlsplit

import pytest
import requests_mock
from google.auth.credentials import AnonymousCredentials
from google.auth.transport.requests import AuthorizedSession
from google.cloud import storage

from litellm.repositories.archive_spool_repository import ArchiveFailure, ArchiveSpool
from litellm.integrations.gcs_archive import GCSArchiveLogger, GCSArchiveObjects, archive_object_key


class MemoryObjects:
    def __init__(self):
        self.objects = {}
        self.failure = None
        self.written = Event()

    def write(self, key, payload):
        if self.failure:
            return self.failure
        self.objects.setdefault(key, payload)
        self.written.set()
        return None

    def read(self, key):
        return self.failure or self.objects.get(key)

    def close(self):
        pass


def event(request_id="response-1"):
    return {
        "standard_logging_object": {
            "id": request_id,
            "startTime": 1704067200.0,
            "trace_id": "trace-1",
            "session_id": "session-1",
            "model": "test-model",
            "status": "success",
            "messages": [{"role": "user", "content": "hello"}],
            "response": {"choices": [{"message": {"content": "world"}}]},
            "metadata": {
                "user_api_key_user_id": "user-1",
                "user_api_key_team_id": "team-1",
                "authorization": "must-not-be-archived",
                "logging": [{"secret": "must-not-be-archived"}],
            },
            "proxy_server_request": {"headers": {"authorization": "must-not-be-archived"}},
            "model_parameters": {"tools": [], "api_key": "must-not-be-archived"},
        }
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("hook", ["async_log_success_event", "async_log_failure_event"])
async def test_records_can_be_read_by_ui_without_database_bodies(tmp_path, hook):
    objects = MemoryObjects()
    logger = GCSArchiveLogger(objects, ArchiveSpool(tmp_path / "pending.sqlite"))
    await getattr(logger, hook)(event(), None, None, None)
    assert await logger.flush() == 1
    payload = await logger.get_request_response_payload("response-1", None, None)
    assert payload["messages"] == event()["standard_logging_object"]["messages"]
    assert payload["response"] == event()["standard_logging_object"]["response"]
    assert payload["metadata"] == {"user_api_key_user_id": "user-1", "user_api_key_team_id": "team-1"}
    assert b"must-not-be-archived" not in next(iter(objects.objects.values()))
    await logger.aclose()


@pytest.mark.asyncio
async def test_failed_upload_survives_reopening_queue(tmp_path):
    path = tmp_path / "pending.sqlite"
    objects = MemoryObjects()
    objects.failure = ArchiveFailure("unavailable")
    first = GCSArchiveLogger(objects, ArchiveSpool(path))
    await first.async_log_success_event(event(), None, None, None)
    assert await first.flush() == 0
    await first.aclose()
    objects.failure = None
    second = GCSArchiveLogger(objects, ArchiveSpool(path))
    assert await second.flush() == 1
    assert await second.flush() == 0
    assert (await second.get_request_response_payload("response-1", None, None))["session_id"] == "session-1"
    await second.aclose()


@pytest.mark.asyncio
async def test_duplicate_delivery_and_concurrent_flushes_are_idempotent(tmp_path):
    objects = MemoryObjects()
    logger = GCSArchiveLogger(objects, ArchiveSpool(tmp_path / "pending.sqlite"))
    await asyncio.gather(*(logger.async_log_success_event(event(), None, None, None) for _ in range(4)))
    await asyncio.gather(logger.flush(), logger.flush())
    assert len(objects.objects) == 1
    assert await logger.flush() == 0
    await logger.aclose()


@pytest.mark.asyncio
async def test_pending_body_is_readable_during_storage_outage(tmp_path):
    objects = MemoryObjects()
    objects.failure = ArchiveFailure("unavailable")
    logger = GCSArchiveLogger(objects, ArchiveSpool(tmp_path / "pending.sqlite"))
    await logger.async_log_success_event(event(), None, None, None)
    assert (await logger.get_request_response_payload("response-1", None, None))["id"] == "response-1"
    assert (await logger.async_health_check())["status"] == "unhealthy"
    await logger.aclose()


@pytest.mark.asyncio
async def test_readback_rejects_payload_with_wrong_request_id(tmp_path):
    objects = MemoryObjects()
    logger = GCSArchiveLogger(objects, ArchiveSpool(tmp_path / "pending.sqlite"))
    objects.objects[archive_object_key("response-1")] = b'{"id":"other","startTime":1704067200}'
    with pytest.raises(ValueError, match="request ID"):
        await logger.get_request_response_payload("response-1", None, None)
    await logger.aclose()


def test_object_name_is_safe_and_independent_of_ui_date():
    key = archive_object_key("../../response/id?token=hidden")
    assert ".." not in key
    assert "hidden" not in key
    assert key != archive_object_key("different-id")


@pytest.mark.asyncio
async def test_stream_chunks_do_not_create_partial_archives(tmp_path):
    objects = MemoryObjects()
    logger = GCSArchiveLogger(objects, ArchiveSpool(tmp_path / "pending.sqlite"))
    await logger.async_log_stream_event(event(), {"delta": "partial"}, None, None)
    assert await logger.flush() == 0
    await logger.async_log_success_event(event(), None, None, None)
    await logger.flush()
    payload = await logger.get_request_response_payload("response-1", datetime(2024, 1, 1, tzinfo=timezone.utc), None)
    assert payload["response"]["choices"][0]["message"]["content"] == "world"
    await logger.aclose()


@pytest.mark.asyncio
async def test_worker_replays_pending_events_without_new_requests(tmp_path):
    spool = ArchiveSpool(tmp_path / "pending.sqlite")
    spool.put("existing", b"payload")
    objects = MemoryObjects()
    logger = GCSArchiveLogger(objects, spool)
    logger.start()
    try:
        assert await asyncio.to_thread(objects.written.wait, 5)
        assert objects.objects["existing"] == b"payload"
    finally:
        await logger.aclose()


@pytest.mark.asyncio
async def test_missing_event_and_full_queue_are_reported_without_leaking_data(tmp_path, caplog):
    logger = GCSArchiveLogger(MemoryObjects(), ArchiveSpool(tmp_path / "pending.sqlite", max_bytes=1))
    await logger.async_log_failure_event({"standard_logging_object": {"secret": "PRIVATE"}}, None, None, None)
    assert (await logger.async_health_check())["error_message"] == "record_not_persisted"
    await logger.async_log_success_event(event(), None, None, None)
    assert (await logger.async_health_check())["error_message"] == "spool_full"
    assert "PRIVATE" not in caplog.text
    assert "must-not-be-archived" not in caplog.text
    await logger.aclose()


def test_google_sdk_upload_is_create_only_and_duplicate_is_content_checked():
    with AuthorizedSession(AnonymousCredentials()) as session, requests_mock.Mocker(session=session) as transport:
        client = storage.Client(project="test-project", credentials=AnonymousCredentials(), _http=session)
        objects = GCSArchiveObjects(client, "test-bucket")
        upload = transport.post(requests_mock.ANY, json={"generation": "1"})
        assert objects.write("requests/example.json", b'{"message":"hello"}') is None
        assert parse_qs(urlsplit(upload.last_request.url).query)["ifGenerationMatch"] == ["0"]
        assert b'{"message":"hello"}' in upload.last_request.body
        transport.post(requests_mock.ANY, status_code=412, json={"error": {"message": "exists"}})
        transport.get(requests_mock.ANY, content=b'{"message":"hello"}')
        assert objects.write("requests/example.json", b'{"message":"hello"}') is None
        assert objects.write("requests/example.json", b"different") == ArchiveFailure("object_conflict")


def test_google_sdk_permission_failure_is_retryable_and_not_a_missing_object():
    with AuthorizedSession(AnonymousCredentials()) as session, requests_mock.Mocker(session=session) as transport:
        objects = GCSArchiveObjects(
            storage.Client(project="test-project", credentials=AnonymousCredentials(), _http=session), "test-bucket"
        )
        transport.get(requests_mock.ANY, status_code=403, json={"error": {"message": "private detail"}})
        assert objects.read("record") == ArchiveFailure("gcs_read_failed")
        transport.post(requests_mock.ANY, status_code=403, json={"error": {"message": "private detail"}})
        assert objects.write("record", b"body") == ArchiveFailure("gcs_write_failed")
        transport.get(requests_mock.ANY, status_code=404, json={"error": {"message": "missing"}})
        assert objects.read("record") is None
