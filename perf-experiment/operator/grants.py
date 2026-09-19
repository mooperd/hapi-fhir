# Created by claude-opus-5
"""FhirUploadGrant: signed URLs minted on demand, on cert-manager's pattern.

The operator holds the only credential that can sign a GCS URL. A worker holds
none, and that is the property this module has to preserve while still letting
a worker write thousands of per-case objects whose names nobody knew at launch.

So the worker never asks for a credential. It declares the objects it intends
to write, as a Kubernetes resource, and the operator decides. That is exactly
what a Certificate is: a request for a credential, reconciled by a controller
that checks you own what you are asking for before it hands anything over.

    spec.paths          the CSR -- what is being asked for
    the prefix check    the ACME challenge -- proof it is yours to ask for
    the grant Secret    the TLS Secret -- where the consumer reads it
    the renew timer     renewBefore -- re-minted before it expires, in place

Three things are load-bearing:

  * The prefix check. A grant may only name objects under the run prefix of
    the benchmark it references, at its own step and shard. Without it a
    worker could ask for a URL to any object in the bucket, including another
    run's results, and the operator would sign it. The CRD pins benchmarkRef,
    runId, stepIndex and shard as immutable so the prefix cannot be moved
    after the grant is admitted.

  * Minting happens off the event loop. generate_signed_url with impersonated
    credentials makes a blocking IAM signBlob call. kopf runs every handler in
    one asyncio loop, so minting 256 URLs inline would stall stack readiness
    and dataset reconciliation along with everything else. asyncio.to_thread
    keeps that in a worker thread.

  * One grant carries many paths. The signing round trip is per URL; the
    Kubernetes round trip is per grant. Batching amortises the second against
    the first, and it is why the worker requests in pages rather than one
    object at a time.
"""

import asyncio
import datetime
import hashlib

import kopf

import gcs

GROUP = "perf.fhir"
VERSION = "v1alpha1"
PLURAL = "fhiruploadgrants"

DEFAULT_TTL = 900

# cert-manager's renewBefore. A third of the lifetime, so a grant is re-minted
# twice over before anything it handed out can expire, and the 2 s timer has
# 300 attempts to get it done rather than one.
RENEW_FRACTION = 3.0

_dyn = None


def init(dyn):
    global _dyn
    _dyn = dyn


def _now():
    return datetime.datetime.now(datetime.timezone.utc)


def _stamp(when):
    return when.isoformat(timespec="seconds")


# --------------------------------------------------------------------------
# Names. Every one of these is computable by benchmark.py at launch and by the
# worker from its config, without anybody looking anything up -- which is what
# lets the worker's RBAC name the Secret it may read by exact resourceName
# instead of being handed the whole namespace.
# --------------------------------------------------------------------------

def grant_name(name, run_id, step_index, shard):
    return "bm-%s-%d-%d-%d-grant" % (name, int(run_id), int(step_index), int(shard))


def secret_name(grant):
    return "%s-urls" % grant


def key_for(path):
    """Secret key for an object path.

    Object paths contain slashes and run past the 253-character key limit, so
    the key is a digest of the path. The worker computes the same digest from
    the same path; nothing has to transmit the mapping.
    """
    return hashlib.sha1(path.encode("utf-8")).hexdigest()[:16]


# --------------------------------------------------------------------------
# Minting
# --------------------------------------------------------------------------

def _prefix(namespace, spec):
    return gcs.step_prefix(namespace, spec["benchmarkRef"], int(spec["runId"]),
                           int(spec["stepIndex"])) + "/"


def _mint(namespace, spec):
    """Sign every requested path, after checking every one of them is in scope.

    A path outside the grant's own step prefix is a permanent error, not a
    skipped entry: a worker asking for it is either broken or not the worker
    it claims to be, and quietly signing the rest would hide both.
    """
    allowed = _prefix(namespace, spec)
    ttl = int(spec.get("ttlSeconds") or DEFAULT_TTL)
    data = {}
    for item in spec.get("paths") or []:
        path = item["path"]
        if not path.startswith(allowed) or ".." in path:
            raise kopf.PermanentError(
                "grant asks for %s, which is outside %s. A grant may only sign "
                "objects belonging to the step and shard it was issued for"
                % (path, allowed))
        data[key_for(path)] = gcs.upload_url(
            path, content_type=item.get("contentType") or "application/json",
            seconds=ttl)
    return data


def _write_secret(namespace, grant, data):
    doc = {
        "apiVersion": "v1", "kind": "Secret",
        "metadata": {"name": secret_name(grant), "namespace": namespace},
        "type": "Opaque",
        # stringData, wholesale: the operator owns every key, so a server-side
        # apply drops the ones that rolled off the window. Expired URLs left
        # lying around are just a bigger Secret and a worse audit trail.
        "stringData": data,
    }
    kopf.adopt(doc)
    resource = _dyn().resources.get(api_version="v1", kind="Secret")
    _dyn().server_side_apply(resource=resource, body=doc, namespace=namespace,
                             field_manager="fhir-operator", force_conflicts=True)


def _issue(namespace, grant, spec):
    data = _mint(namespace, spec)
    _write_secret(namespace, grant, data)
    return len(data)


async def _reconcile(namespace, name, spec, meta, patch, logger):
    ttl = int(spec.get("ttlSeconds") or DEFAULT_TTL)
    minted = await asyncio.to_thread(_issue, namespace, name, spec)
    now = _now()
    patch.status["secretName"] = secret_name(name)
    patch.status["minted"] = minted
    patch.status["mintedAt"] = _stamp(now)
    patch.status["notAfter"] = _stamp(now + datetime.timedelta(seconds=ttl))
    patch.status["observedGeneration"] = meta.get("generation")
    patch.status["phase"] = "Issued"
    logger.info("grant %s: minted %d url(s), valid %ds", name, minted, ttl)


# --------------------------------------------------------------------------
# Handlers
# --------------------------------------------------------------------------

@kopf.on.create(GROUP, VERSION, PLURAL, id="mint")
async def mint(spec, meta, namespace, name, patch, logger, **_):
    """Mints on the create event, not on the timer.

    The whole point of the fast loop is that a worker blocked on its first URL
    is a worker not measuring anything. An event-driven handler answers in a
    watch round trip; the timer below is repair and renewal, never the path a
    healthy request takes.
    """
    await _reconcile(namespace, name, spec, meta, patch, logger)


@kopf.on.field(GROUP, VERSION, PLURAL, field="spec.paths", id="repage")
async def repage(spec, meta, namespace, name, patch, logger, **_):
    """The worker turned the page. Same latency budget as create."""
    await _reconcile(namespace, name, spec, meta, patch, logger)


@kopf.timer(GROUP, VERSION, PLURAL, interval=2, id="renew")
async def renew(spec, meta, status, namespace, name, patch, logger, **_):
    """renewBefore, and the repair loop for a mint that never landed.

    2 s rather than the 15 s the benchmark timer runs at: this one is in a
    worker's critical path. It does nothing at all unless the grant is stale,
    so the cost of the interval is a dict comparison per grant per two seconds.
    """
    generation = meta.get("generation")
    if status.get("observedGeneration") != generation:
        logger.info("grant %s: generation %s not yet issued", name, generation)
        await _reconcile(namespace, name, spec, meta, patch, logger)
        return

    not_after = status.get("notAfter")
    if not not_after:
        await _reconcile(namespace, name, spec, meta, patch, logger)
        return

    ttl = int(spec.get("ttlSeconds") or DEFAULT_TTL)
    try:
        expiry = datetime.datetime.fromisoformat(not_after)
    except ValueError:
        await _reconcile(namespace, name, spec, meta, patch, logger)
        return
    if _now() >= expiry - datetime.timedelta(seconds=ttl / RENEW_FRACTION):
        await _reconcile(namespace, name, spec, meta, patch, logger)
