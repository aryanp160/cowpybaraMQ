import pytest
from internal.compression import compress_payload, decompress_payload


@pytest.mark.unit
def test_compress_none():
    payload = {"data": "hello world", "big": "x" * 600}
    compressed, was_comp = compress_payload(payload, codec="none", threshold=512)
    assert not was_comp
    assert compressed == payload


@pytest.mark.unit
def test_compress_under_threshold():
    payload = {"data": "under threshold"}
    compressed, was_comp = compress_payload(payload, codec="gzip", threshold=512)
    assert not was_comp
    assert compressed == payload


@pytest.mark.unit
def test_compress_over_threshold():
    payload = {"data": "over threshold", "filler": "x" * 600}
    compressed, was_comp = compress_payload(payload, codec="gzip", threshold=500)
    assert was_comp
    assert "_compressed_payload" in compressed
    assert compressed["_compression"] == "gzip"

    # Decompress back
    decompressed = decompress_payload(compressed)
    assert decompressed == payload


@pytest.mark.unit
def test_decompress_fault_tolerance():
    # Not a dict
    assert decompress_payload("plain string") == "plain string"

    # Dict without compressed key
    assert decompress_payload({"key": "val"}) == {"key": "val"}

    # Invalid base64/gzip data
    invalid_record = {
        "_compressed_payload": "!!!invalid base64!!!",
        "_compression": "gzip",
    }
    assert decompress_payload(invalid_record) == invalid_record
