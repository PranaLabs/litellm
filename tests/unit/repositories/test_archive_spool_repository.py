from contextlib import closing

import pytest

from litellm.repositories.archive_spool_repository import ArchiveFailure, ArchiveSpool


@pytest.fixture
def spool(request, tmp_path):
    with closing(ArchiveSpool(tmp_path / "pending.sqlite", max_bytes=getattr(request, "param", 1024))) as queue:
        yield queue


@pytest.mark.parametrize("spool", [5], indirect=True)
def test_queue_capacity_preserves_existing_records_and_recovers_after_ack(spool):
    assert spool.put("one", b"12345") is None
    assert spool.put("two", b"x") == ArchiveFailure("spool_full")
    assert spool.pending_keys(10) == ("one",)
    assert spool.read("one") == b"12345"
    spool.acknowledge("one", b"12345")
    assert spool.put("two", b"x") is None


def test_queue_rejects_conflicting_duplicate_without_replacing_original(spool):
    spool.put("same", b"original")
    assert spool.put("same", b"changed") == ArchiveFailure("object_conflict")
    assert spool.pending_keys(10) == ("same",)
    assert spool.read("same") == b"original"


def test_stale_acknowledgement_cannot_delete_a_new_delivery(spool):
    spool.put("same", b"first")
    spool.acknowledge("same", b"first")
    spool.put("same", b"second")
    spool.acknowledge("same", b"first")
    assert spool.read("same") == b"second"


def test_deferred_record_does_not_starve_other_pending_records(spool):
    spool.put("failing", b"first")
    spool.put("healthy", b"second")
    spool.defer("failing")
    assert spool.pending_keys(1) == ("healthy",)
    assert spool.read("failing") == b"first"
