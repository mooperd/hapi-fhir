# Created by claude-opus-5
"""Google Cloud Storage: the durable record of every run.

The operator is the only GCP identity in the system. Workers never hold a
credential -- they are handed V4 signed URLs in their config and PUT to them
with plain `requests`. A leaked worker URL is one object path with a TTL,
not a bucket.

Credentials resolve identically in both modes, which is the whole point of
impersonating: `google.auth.default()` finds the base principal -- your user
ADC under docker compose, the mounted service-account key in the cluster --
and that principal's only power is `iam.serviceAccountTokenCreator` on the
writer service account. Storage access belongs to the writer alone, and the
writer has no key anywhere in the world.

Signing therefore goes through the IAM signBlob API in both modes rather than
a local private key. That is slower per URL and irrelevant in practice: URLs
are minted once per shard at step launch, not once per request.

Nothing here deletes. There is no lifecycle rule on the bucket and no object
is ever removed by the operator, so a FhirBenchmark can be deleted without
taking its results with it.
"""

import datetime
import json
import os

BUCKET = os.environ.get("GCS_BUCKET", "fhir-benchmark-results")
PROJECT = os.environ.get("GCS_PROJECT", "teak-mantis-509006-s9")
WRITER = os.environ.get("GCS_WRITER_SA", "fhir-benchmark-writer@teak-mantis-509006-s9.iam.gserviceaccount.com")
SIGNED_URL_HOURS = int(os.environ.get("GCS_SIGNED_URL_HOURS", "24"))

SCOPES = ["https://www.googleapis.com/auth/devstorage.read_write"]

_client = None
_credentials = None


class Unconfigured(RuntimeError):
    """Raised when a GCS operation is attempted with no writer configured."""


def enabled():
    return bool(BUCKET and WRITER)


def require():
    if not enabled():
        raise Unconfigured(
            "GCS is not configured: set GCS_WRITER_SA to the service account the "
            "operator impersonates (and GCS_BUCKET if not %s). Results have nowhere "
            "to go, so no run may start" % BUCKET)


def credentials():
    """The impersonated writer. Base principal differs per mode; this does not."""
    global _credentials
    if _credentials is None:
        import google.auth
        from google.auth import impersonated_credentials

        require()
        source, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"])
        _credentials = impersonated_credentials.Credentials(
            source_credentials=source,
            target_principal=WRITER,
            target_scopes=SCOPES,
            lifetime=3600)
    return _credentials


def client():
    global _client
    if _client is None:
        from google.cloud import storage

        _client = storage.Client(project=PROJECT, credentials=credentials())
    return _client


def bucket():
    return client().bucket(BUCKET)


def uri(path):
    return "gs://%s/%s" % (BUCKET, path.lstrip("/"))


def run_prefix(namespace, name, run_id):
    return "%s/%s/%d" % (namespace, name, int(run_id))


def step_prefix(namespace, name, run_id, step_index):
    return "%s/steps/%d" % (run_prefix(namespace, name, run_id), int(step_index))


def summary_path(namespace, name, run_id):
    return "%s/summary.json" % run_prefix(namespace, name, run_id)


def report_path(namespace, name, run_id):
    return "%s/report.json" % run_prefix(namespace, name, run_id)


# --------------------------------------------------------------------------
# Signed URLs -- the only thing a worker ever receives
# --------------------------------------------------------------------------

def _signed(path, method, content_type=None, hours=None):
    creds = credentials()
    expiry = datetime.timedelta(hours=hours or SIGNED_URL_HOURS)
    return bucket().blob(path).generate_signed_url(
        version="v4",
        expiration=expiry,
        method=method,
        content_type=content_type,
        credentials=creds,
        service_account_email=creds.signer_email)


def upload_url(path, content_type="application/x-ndjson", hours=None):
    """A URL a worker may PUT exactly one object to, once, until it expires."""
    return _signed(path, "PUT", content_type=content_type, hours=hours)


def download_url(path, hours=1):
    return _signed(path, "GET", hours=hours)


# --------------------------------------------------------------------------
# Operator-side reads and writes
# --------------------------------------------------------------------------

def write_json(path, payload):
    blob = bucket().blob(path)
    blob.upload_from_string(json.dumps(payload, indent=2),
                            content_type="application/json")
    return uri(path)


def read_json(path):
    return json.loads(bucket().blob(path).download_as_bytes())


def read_json_or_empty(path):
    """{} when the object does not exist yet. Any other failure still raises.

    A missing summary before the first step has landed is a fact about where
    the run has got to. A permission or network failure is not, and must not
    be quietly turned into an empty result set.
    """
    from google.cloud.exceptions import NotFound
    try:
        return read_json(path)
    except NotFound:
        return {}


def read_summary(namespace, name, run_id):
    """The run's merged per-case summary. The only place it exists."""
    return read_json_or_empty(summary_path(namespace, name, run_id))


def write_summary(namespace, name, run_id, summary):
    return write_json(summary_path(namespace, name, run_id), summary)


def read_ndjson(path):
    """Every record in one shard object. Blank lines are not records."""
    raw = bucket().blob(path).download_as_bytes().decode("utf-8")
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


def list_prefix(prefix, suffix=""):
    names = [b.name for b in client().list_blobs(BUCKET, prefix=prefix)]
    return sorted(n for n in names if not suffix or n.endswith(suffix))
