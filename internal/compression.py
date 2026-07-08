import base64
import gzip
import json
from typing import Any, Dict, Tuple


def compress_payload(
    payload: Dict[str, Any], codec: str = "none", threshold: int = 512
) -> Tuple[Dict[str, Any], bool]:
    """Compress the payload if codec is not 'none' and serialized payload size exceeds threshold."""
    if codec == "none":
        return payload, False

    try:
        serialized = json.dumps(payload).encode("utf-8")
        if len(serialized) <= threshold:
            return payload, False

        if codec == "gzip":
            compressed = gzip.compress(serialized)
            encoded = base64.b64encode(compressed).decode("utf-8")
            return {
                "_compressed_payload": encoded,
                "_compression": "gzip",
            }, True
    except Exception:
        # Fallback to none on any failure
        pass

    return payload, False


def decompress_payload(stored_payload: Any) -> Any:
    """Decompress payload if it contains compression metadata."""
    if isinstance(stored_payload, dict) and "_compressed_payload" in stored_payload:
        try:
            codec = stored_payload.get("_compression")
            encoded = stored_payload["_compressed_payload"]
            compressed = base64.b64decode(encoded.encode("utf-8"))
            if codec == "gzip":
                decompressed = gzip.decompress(compressed)
                return json.loads(decompressed.decode("utf-8"))
        except Exception:
            pass
    return stored_payload
