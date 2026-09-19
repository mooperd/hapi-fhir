# Created by claude-opus-5
"""Per-case evidence: what was asked, what came back, and the first page.

A measurement record says a case took 412 ms and returned 20 rows. When that
number looks wrong, every useful question is about something the record does
not contain -- which URL was actually issued after binding substitution, what
the server sent back in its headers, what the Bundle really held. This module
captures that and gets it to GCS without perturbing the thing being measured.

Three constraints shape it:

  * Capture must not cost latency. The measuring thread only ever writes to
    the pod's emptyDir and appends to a queue. Signing, HTTP and retries all
    happen on other threads. Nothing in the measure loop blocks on the network
    except the FHIR request being timed.

  * Nothing accumulates in memory. Thirty repetitions across hundreds of cases
    with megabyte-scale Bundles would exceed the worker's 2 GiB limit long
    before the step ended, and an OOM-killed worker loses the measurements too,
    not just the evidence. Bodies go to disk immediately and are read back by
    the uploader.

  * The worker has no credential. It cannot sign a URL and must never be able
    to. It asks the operator for URLs by declaring a FhirUploadGrant and reads
    the answer out of a Secret whose name is the only one its RBAC permits.

The page is stored as its own object because it is the only unbounded thing
here. An exchange is a few hundred bytes and always worth reading; a first
page can be tens of megabytes, and putting them in one object would mean the
detail view could not list a step's exchanges without dragging every body
along with it.
"""

import base64
import hashlib
import json
import os
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests

import upload

SA_DIR = "/var/run/secrets/kubernetes.io/serviceaccount"
API = "https://kubernetes.default.svc"
GROUP_VERSION = "perf.fhir/v1alpha1"

# A first page past this is stored truncated rather than skipped: knowing the
# first 8 MiB of what came back is worth far more than a note saying it was
# large. The exchange records the true length either way.
PAGE_MAX_BYTES = int(os.environ.get("EXCHANGE_PAGE_MAX_BYTES", str(8 * 1024 * 1024)))

# emptyDir lives on the node's filesystem and is shared with everything else
# scheduled there. Past this, pages stop being written and the exchange says
# so. A benchmark that filled a node's disk with its own diagnostics would be
# a worse failure than a missing body.
DISK_BUDGET = int(os.environ.get("EXCHANGE_DISK_BUDGET", str(2 * 1024 * 1024 * 1024)))

UPLOADERS = int(os.environ.get("EXCHANGE_UPLOADERS", "4"))
GRANT_BATCH = int(os.environ.get("EXCHANGE_GRANT_BATCH", "128"))
GRANT_WAIT = float(os.environ.get("EXCHANGE_GRANT_WAIT", "60"))
GRANT_POLL = float(os.environ.get("EXCHANGE_GRANT_POLL", "0.25"))

# Nothing here sends credentials today. The redaction is so that the day
# something does, the thing that leaks it is not the diagnostic capture.
REDACT = ("authorization", "cookie", "set-cookie", "proxy-authorization",
          "x-api-key")

JSON = "application/json"


def key_for(path):
    """Must match grants.key_for. The mapping is never transmitted."""
    return hashlib.sha1(path.encode("utf-8")).hexdigest()[:16]


def _headers(mapping):
    out = {}
    for name, value in (mapping or {}).items():
        out[str(name)] = ("<redacted>" if str(name).lower() in REDACT
                          else str(value))
    return out


# --------------------------------------------------------------------------
# Talking to the API server
#
# Raw requests with the projected service account token, the same way
# loader.py reports dataset status. The worker image carries no kubernetes
# client and does not need one for two endpoints.
# --------------------------------------------------------------------------

class Grants:
    """Asks the operator for signed URLs and reads back what it granted."""

    def __init__(self, namespace, grant, secret, ttl):
        self.namespace = namespace
        self.grant = grant
        self.secret = secret
        self.ttl = float(ttl)
        self.session = requests.Session()
        # path -> (url, minted at). A granted URL is a bearer credential with
        # a deadline, so the cache has to know when each one was issued: a
        # step can easily outlive a grant's TTL, and an expired URL comes back
        # as a 403, which upload.put_object correctly refuses to retry.
        self.cache = {}
        with open(os.path.join(SA_DIR, "token"), encoding="utf-8") as handle:
            self.session.headers["Authorization"] = "Bearer " + handle.read().strip()
        self.verify = os.path.join(SA_DIR, "ca.crt")
        self.grant_url = "%s/apis/%s/namespaces/%s/fhiruploadgrants/%s" % (
            API, GROUP_VERSION, namespace, grant)
        self.secret_url = "%s/api/v1/namespaces/%s/secrets/%s" % (
            API, namespace, secret)

    def _read_secret(self):
        response = self.session.get(self.secret_url, verify=self.verify, timeout=30)
        if response.status_code == 404:
            return {}
        response.raise_for_status()
        data = (response.json() or {}).get("data") or {}
        return {k: base64.b64decode(v).decode("utf-8") for k, v in data.items()}

    def fresh(self, path):
        """A cached URL, if it will still be valid by the time it is used.

        Two thirds of the lifetime, matching the renewBefore the operator
        renews grants on. Anything older is re-requested rather than gambled
        on.
        """
        held = self.cache.get(path)
        if held is None:
            return None
        url, minted = held
        return url if time.time() - minted < self.ttl * (2.0 / 3.0) else None

    def urls_for(self, wanted):
        """wanted: [(path, contentType)]. Returns {path: signed url}.

        One PATCH for the whole batch, then a poll until the operator's mint
        lands. The grant's spec.paths is a rolling window, so the batch we ask
        for is the batch we replace -- by the time it rolls off, every URL in
        it has been used or re-requested.
        """
        missing = [(p, c) for p, c in wanted if self.fresh(p) is None]
        if missing:
            body = {"spec": {"paths": [{"path": p, "contentType": c}
                                       for p, c in missing]}}
            response = self.session.patch(
                self.grant_url, json=body,
                headers={"Content-Type": "application/merge-patch+json"},
                verify=self.verify, timeout=60)
            if response.status_code >= 300:
                raise RuntimeError("grant %s rejected the page: HTTP %d %s"
                                   % (self.grant, response.status_code,
                                      (response.text or "")[:300]))

            keys = {key_for(p): p for p, _ in missing}
            deadline = time.time() + GRANT_WAIT
            while True:
                found = self._read_secret()
                if all(k in found for k in keys):
                    now = time.time()
                    for k, path in keys.items():
                        self.cache[path] = (found[k], now)
                    break
                if time.time() >= deadline:
                    raise RuntimeError(
                        "grant %s did not mint %d url(s) within %.0fs; secret "
                        "%s never carried them"
                        % (self.grant, len(missing), GRANT_WAIT, self.secret))
                time.sleep(GRANT_POLL)
        return {p: self.fresh(p) for p, _ in wanted}


# --------------------------------------------------------------------------
# Capture
# --------------------------------------------------------------------------

class Uploads:
    """Stages exchanges on disk, uploads them on background threads.

    Disabled cleanly when the step config carries no uploads block, so a
    worker launched by an older operator still measures.
    """

    def __init__(self, config, results_dir, shard, node):
        self.settings = config.get("uploads") or {}
        self.enabled = bool(self.settings.get("casePrefix"))
        self.config = config
        self.shard = shard
        self.node = node
        self.index = {}
        self.failures = []
        self.staged = 0
        self.uploaded = 0
        self.bytes_on_disk = 0
        self._pending = []
        self._prefetch = []
        self._queue = queue.Queue()
        self._dir = os.path.join(results_dir, "exchanges")
        self._pump = None
        if not self.enabled:
            print("exchange capture is off: step config carries no uploads block",
                  flush=True)
            return
        # Keyed by shard, the same shape config["results"] uses: one grant
        # and one Secret per shard, so two workers never patch one object.
        mine = (self.settings.get("grants") or {}).get(str(shard))
        if not mine:
            self.enabled = False
            print("exchange capture is off: no grant for shard %d" % shard,
                  flush=True)
            return
        os.makedirs(self._dir, exist_ok=True)
        try:
            self.grants = Grants(self.settings["namespace"],
                                 mine["grantName"], mine["secretName"],
                                 self.settings.get("ttlSeconds") or 900)
        except Exception as exc:
            self.enabled = False
            print("exchange capture is off: %s: %s" % (type(exc).__name__, exc),
                  flush=True)
            return
        self._pump = threading.Thread(target=self._run, daemon=True,
                                      name="exchange-upload")
        self._pump.start()
        print("exchange capture on: grant %s, secret %s, page cap %d bytes"
              % (mine["grantName"], mine["secretName"], PAGE_MAX_BYTES),
              flush=True)

    def prime(self, cases):
        """Queue the deterministic paths this shard will need, for prefetch.

        Every case produces a repetition 0, and the two objects it writes are
        named before the run starts. Asking for them in pages of GRANT_BATCH,
        alongside the first upload that needs one, is what keeps the grant
        round trip amortised: without it each object costs its own PATCH and
        its own poll, because uploads trickle out one case at a time and the
        queue is never deep enough to batch on its own.

        Only a failed repetition's paths are genuinely unpredictable, and
        those are requested on demand -- which is rare, and is meant to be.
        """
        if not self.enabled:
            return
        for case in cases:
            self._prefetch.append((self._object(case["id"], "page.json", None), JSON))
            self._prefetch.append((self._object(case["id"], "exchange.json", None), JSON))
        print("exchange prefetch: %d paths queued for %d cases"
              % (len(self._prefetch), len(cases)), flush=True)

    # ---------------------------------------------------------------- paths

    def _object(self, case_id, suffix, rep):
        stem = "%d" % self.shard if rep is None else "%d-r%d" % (self.shard, rep)
        return "%s/%s/%s.%s" % (self.settings["casePrefix"], case_id, stem, suffix)

    # -------------------------------------------------------------- capture

    def record(self, case, record, sent, response, exc):
        """Called from the measuring thread. Touches disk, never the network.

        Which repetitions are kept: repetition 0 of every case, always, plus
        any later one that already failed. Thirty identical bodies from a
        pinned binding tell nobody anything; the one that failed tells you
        everything.

        Repetition 0 is unconditional rather than "the first valid one"
        because validity is not settled yet. _reconcile can invalidate a
        repetition after the loop has moved on -- an unattributable engine, a
        row count outside tolerance -- and by then the body is gone. Capturing
        on a decision that has not been made yet would leave the cases most
        worth looking at as the ones with no evidence.
        """
        if not self.enabled:
            return
        rep = record.get("rep")
        invalid = not record.get("valid")
        if rep != 0 and not invalid:
            return
        case_id = case["id"]
        stem_rep = None if rep == 0 else rep

        page = None
        if response is not None:
            page = self._stage_page(case_id, stem_rep, response)

        exchange = {
            "caseId": case_id, "family": case.get("family"),
            "shard": self.shard, "rep": rep, "node": self.node,
            "request": {
                "method": sent.get("method"),
                "url": sent.get("url"),
                "headers": _headers(sent.get("headers")),
                "body": sent.get("body"),
            },
            "response": None,
            "error": None if exc is None else "%s: %s" % (type(exc).__name__, exc),
            "page": page,
            # Filled at flush. The measurement is the ndjson row verbatim, not
            # a copy of some of its fields: a second copy that drifts from the
            # first is worse than no copy at all.
            "record": None,
        }
        if response is not None:
            exchange["response"] = {
                "status": response.status_code,
                "reason": response.reason,
                "headers": _headers(response.headers),
                "contentLength": len(response.content or b""),
                "elapsedMs": round(response.elapsed.total_seconds() * 1000.0, 3)
                             if response.elapsed is not None else None,
            }
        self._pending.append((self._object(case_id, "exchange.json", stem_rep),
                              exchange, record))

    def flush_case(self):
        """Write and enqueue the case's exchanges, after _reconcile has run.

        _reconcile attaches the engine attribution and the PostgreSQL deltas
        to every record in place, and can still flip one to invalid. Held
        until here, the exchange carries the measurement as it was finally
        judged rather than as it looked mid-loop.
        """
        if not self.enabled:
            return
        for path, exchange, record in self._pending:
            exchange["record"] = record
            body = json.dumps(exchange, indent=2).encode("utf-8")
            local = os.path.join(self._dir, key_for(path) + ".json")
            with open(local, "wb") as handle:
                handle.write(body)
            self.bytes_on_disk += len(body)
            self.staged += 1
            self.index.setdefault(exchange["caseId"], []).append(path)
            self._queue.put((local, path, JSON))
        self._pending = []

    def _stage_page(self, case_id, rep, response):
        content = response.content or b""
        path = self._object(case_id, "page.json", rep)
        info = {"object": path, "bytes": len(content), "truncated": False}

        if self.bytes_on_disk + min(len(content), PAGE_MAX_BYTES) > DISK_BUDGET:
            return {"object": None, "bytes": len(content), "truncated": False,
                    "omitted": "disk budget of %d bytes reached" % DISK_BUDGET}

        body = content
        if len(content) > PAGE_MAX_BYTES:
            body = content[:PAGE_MAX_BYTES]
            info["truncated"] = True
            info["storedBytes"] = len(body)

        local = os.path.join(self._dir, key_for(path) + ".page")
        with open(local, "wb") as handle:
            handle.write(body)
        self.bytes_on_disk += len(body)
        self.staged += 1
        self.index.setdefault(case_id, []).append(path)
        self._queue.put((local, path, JSON))
        return info

    # --------------------------------------------------------------- upload

    def _run(self):
        pool = ThreadPoolExecutor(max_workers=UPLOADERS, thread_name_prefix="put")
        try:
            while True:
                batch = self._take()
                if batch is None:
                    return
                if batch:
                    self._send(pool, batch)
        finally:
            pool.shutdown(wait=True)

    def _take(self):
        """Block for one item, then sweep up whatever else is already waiting.

        Batching is what makes the grant affordable: one API round trip and
        one mint for up to GRANT_BATCH objects, instead of one per object.
        """
        batch = []
        item = self._queue.get()
        if item is None:
            return None
        batch.append(item)
        while len(batch) < GRANT_BATCH:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            if item is None:
                self._queue.put(None)
                break
            batch.append(item)
        return batch

    def _take_prefetch(self, room):
        """Fill the rest of a grant page with paths we know we will want."""
        out = []
        while self._prefetch and len(out) < room:
            path, ctype = self._prefetch.pop(0)
            if self.grants.fresh(path) is None:
                out.append((path, ctype))
        return out

    def _send(self, pool, batch):
        wanted = [(path, ctype) for _, path, ctype in batch]
        wanted += self._take_prefetch(GRANT_BATCH - len(wanted))
        try:
            urls = self.grants.urls_for(wanted)
        except Exception as exc:
            self._fail([path for _, path, _ in batch],
                       "%s: %s" % (type(exc).__name__, exc))
            return
        results = list(pool.map(lambda item: self._put(urls, item), batch))
        self.uploaded += sum(1 for ok in results if ok)

    def _put(self, urls, item):
        local, path, ctype = item
        url = urls.get(path)
        if not url:
            self._fail([path], "no signed url was granted")
            return False
        try:
            upload.put_file(url, local, ctype, path)
        except Exception as exc:
            self._fail([path], "%s: %s" % (type(exc).__name__, exc))
            return False
        try:
            os.unlink(local)
        except OSError:
            pass
        return True

    def _fail(self, paths, why):
        """Evidence that did not land is recorded, never fatal.

        A shard body is a measurement and a truncated one is a corrupted
        result. An exchange is a diagnostic, and losing one is not a reason to
        void a forty-minute cold/warm/hot run. The failure is named in the
        marker and shows up in the journal.
        """
        for path in paths:
            self.failures.append({"object": path, "error": why})
            print("exchange upload failed for %s -- %s" % (path, why), flush=True)

    # ----------------------------------------------------------------- close

    def close(self):
        """Drain before the shard marker is written. Returns marker extras."""
        if not self.enabled:
            return {}
        self._queue.put(None)
        self._pump.join(timeout=float(os.environ.get("EXCHANGE_DRAIN", "900")))
        if self._pump.is_alive():
            self._fail(["<drain>"], "uploader did not finish draining")
        print("exchanges: %d objects staged, %d uploaded, %d failed, %d bytes"
              % (self.staged, self.uploaded, len(self.failures),
                 self.bytes_on_disk), flush=True)
        return {
            "exchanges": self.index,
            "exchangeCount": self.staged,
            "exchangeUploaded": self.uploaded,
            "exchangeFailures": self.failures[:50],
        }
