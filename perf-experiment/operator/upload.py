# Created by claude-opus-5
"""PUT an object to a V4 signed URL. The worker side of GCS, in full.

Workers hold no GCP credential and import no Google library. The operator
mints a signed URL per object at launch and passes it down in the Job's
config; the URL is the whole authorisation and `requests` is the whole
client. Shared by runner.py and loader.py, both of which live in the worker
image.
"""

import os
import time

import requests

NDJSON = "application/x-ndjson"
JSON = "application/json"

ATTEMPTS = int(os.environ.get("UPLOAD_ATTEMPTS", "5"))
TIMEOUT = int(os.environ.get("UPLOAD_TIMEOUT", "300"))


def put_object(url, body, content_type, what):
    """PUT one object, or fail by name. Never retried into a false success.

    Worth retrying at all because the benchmark Job is backoffLimit: 0 -- there
    is no second attempt at the pod level, so a dropped connection here would
    discard a step that may have taken an hour to measure. The body is already
    on local disk before this is called, so a retry re-sends known bytes.
    """
    delay = 2
    why = "not attempted"
    for attempt in range(1, ATTEMPTS + 1):
        try:
            response = requests.put(url, data=body, timeout=TIMEOUT,
                                    headers={"Content-Type": content_type})
            if response.status_code < 300:
                print("uploaded %s (%d bytes)" % (what, len(body)), flush=True)
                return
            why = "HTTP %d %s" % (response.status_code, (response.text or "")[:400])
            # A URL that has expired, or was signed for a different content
            # type or method, will never succeed. Say so now rather than
            # spending four more attempts proving it.
            if response.status_code in (400, 403):
                break
        except Exception as exc:              # noqa: BLE001 - reported, then retried
            why = "%s: %s" % (type(exc).__name__, exc)
        print("upload of %s failed (attempt %d/%d) -- %s"
              % (what, attempt, ATTEMPTS, why), flush=True)
        if attempt < ATTEMPTS:
            time.sleep(delay)
            delay *= 2
    raise RuntimeError("could not upload %s: %s" % (what, why))


def put_file(url, path, content_type, what):
    with open(path, "rb") as handle:
        put_object(url, handle.read(), content_type, what)
