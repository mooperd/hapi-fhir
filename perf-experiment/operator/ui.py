# Created by claude-opus-5
"""Flask UI that CRUDs every CRD in the operator's API group.

Nothing here is FhirStack-specific. It discovers the CRDs at request time,
generates a form from each one's OpenAPI schema, and applies what you submit.
Add a CRD to the cluster and a tab for it appears -- no change to this file.

A raw YAML box sits under every form as the escape hatch for anything the
generated form cannot express (arrays of objects, preserve-unknown-fields).
YAML wins when both are filled in.

Runs in a thread inside the operator process; started from
fhir_operator.startup() via start().
"""

import threading

import yaml
from flask import Flask, redirect, render_template_string, request, url_for
from kubernetes import client

GROUP = "perf.pkb"

app = Flask(__name__)

_dyn = None


def start(port, dyn):
    """Serve the UI on a daemon thread. Called once, at operator startup."""
    global _dyn
    _dyn = dyn
    thread = threading.Thread(
        target=lambda: app.run(host="0.0.0.0", port=port, threaded=True, debug=False),
        daemon=True, name="ui")
    thread.start()
    return thread


def _api():
    return client.ApiextensionsV1Api(_dyn().client)


def _custom():
    return client.CustomObjectsApi(_dyn().client)


def _core():
    return client.CoreV1Api(_dyn().client)


# --------------------------------------------------------------------------
# Discovery
# --------------------------------------------------------------------------

def crds():
    """Every CRD in GROUP, as {plural, kind, version, schema, scope}."""
    out = []
    for item in _api().list_custom_resource_definition().items:
        if item.spec.group != GROUP:
            continue
        served = [v for v in item.spec.versions if v.storage] or list(item.spec.versions)
        version = served[0]
        schema = {}
        if version.schema and version.schema.open_apiv3_schema:
            full = version.schema.open_apiv3_schema.to_dict()
            schema = (full.get("properties") or {}).get("spec") or {}
        out.append({
            "plural": item.spec.names.plural,
            "kind": item.spec.names.kind,
            "version": version.name,
            "scope": item.spec.scope,
            "schema": schema,
        })
    return sorted(out, key=lambda c: c["kind"])


def find(plural):
    for crd in crds():
        if crd["plural"] == plural:
            return crd
    return None


# --------------------------------------------------------------------------
# Schema -> flat field list, and back again
# --------------------------------------------------------------------------

def fields(schema, prefix="", depth=0):
    """Flatten an OpenAPI object schema into renderable rows.

    Dotted paths become the form field names, which is what lets build()
    reassemble an arbitrarily nested spec without knowing the CRD.
    """
    out = []
    props = (schema or {}).get("properties") or {}
    for name in sorted(props):
        sub = props[name] or {}
        path = "%s.%s" % (prefix, name) if prefix else name
        kind = sub.get("type") or "string"

        if kind == "object" and sub.get("properties"):
            out.append({"path": path, "name": name, "type": "group", "depth": depth,
                        "desc": sub.get("description") or ""})
            out.extend(fields(sub, path, depth + 1))
            continue
        if kind == "object" or kind == "array":
            # No usable sub-schema: hand it to the YAML box instead.
            kind = "yaml"

        out.append({
            "path": path, "name": name, "type": kind, "depth": depth,
            "enum": sub.get("enum"), "default": sub.get("default"),
            "desc": sub.get("description") or "",
        })
    return out


def coerce(raw, kind):
    if kind in ("number",):
        return float(raw)
    if kind in ("integer",):
        return int(raw)
    if kind == "boolean":
        return raw == "true"
    if kind == "yaml":
        return yaml.safe_load(raw)
    return raw


def build(form, flat):
    """Reassemble a nested spec from dotted form keys."""
    spec = {}
    for field in flat:
        if field["type"] == "group":
            continue
        raw = (form.get(field["path"]) or "").strip()
        if raw == "":
            continue
        value = coerce(raw, field["type"])
        if value is None:
            continue
        node = spec
        parts = field["path"].split(".")
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        node[parts[-1]] = value
    return spec


def lookup(spec, path):
    """Current value at a dotted path, or '' if absent."""
    node = spec or {}
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return ""
        node = node[part]
    if isinstance(node, (dict, list)):
        return yaml.safe_dump(node, sort_keys=False).strip()
    if isinstance(node, bool):
        return "true" if node else "false"
    return node


# --------------------------------------------------------------------------
# Objects
# --------------------------------------------------------------------------

def objects(crd):
    try:
        found = _custom().list_cluster_custom_object(GROUP, crd["version"], crd["plural"])
    except client.ApiException as exc:
        return [], "%s %s" % (exc.status, exc.reason)
    out = []
    for item in found.get("items", []):
        spec = item.get("spec") or {}
        status = item.get("status") or {}
        out.append({
            "namespace": item["metadata"].get("namespace", ""),
            "name": item["metadata"]["name"],
            "created": item["metadata"].get("creationTimestamp", ""),
            "phase": status.get("phase", ""),
            "summary": ", ".join(
                "%s=%s" % (k, v) for k, v in sorted((status.get("components") or {}).items())),
            "spec": spec,
            "yaml": yaml.safe_dump(spec, sort_keys=False, default_flow_style=False).strip(),
        })
    return sorted(out, key=lambda o: (o["namespace"], o["name"])), None


def apply_object(crd, namespace, name, spec):
    """Create the object, or replace its spec outright if it exists.

    Replace rather than server-side apply. SSA only removes fields that the
    *applying* manager already owned, so clearing a box in the form left the
    value in place whenever some other manager -- adopt.py's create, kubectl,
    a previous kopf write -- owned that field. Replace makes the form the
    single source of truth: what you submit is the whole spec, and an emptied
    field really is absent.
    """
    if crd["scope"] == "Namespaced":
        try:
            _core().create_namespace({"metadata": {"name": namespace}})
        except client.ApiException as exc:
            if exc.status != 409:
                raise

    try:
        existing = _custom().get_namespaced_custom_object(
            GROUP, crd["version"], namespace, crd["plural"], name)
    except client.ApiException as exc:
        if exc.status != 404:
            raise
        existing = None

    if existing is None:
        _custom().create_namespaced_custom_object(
            GROUP, crd["version"], namespace, crd["plural"],
            {
                "apiVersion": "%s/%s" % (GROUP, crd["version"]),
                "kind": crd["kind"],
                "metadata": {"name": name, "namespace": namespace},
                "spec": spec,
            })
        return

    # Carries resourceVersion, so a concurrent edit loses rather than silently
    # clobbering.
    existing["spec"] = spec
    _custom().replace_namespaced_custom_object(
        GROUP, crd["version"], namespace, crd["plural"], name, existing)


# --------------------------------------------------------------------------
# Routes
# --------------------------------------------------------------------------

@app.route("/")
def index():
    available = crds()
    if not available:
        return render_template_string(PAGE, crds=[], crd=None, objects=[], flat=[],
                                      editing=None, error="No CRDs in group %s." % GROUP,
                                      message=None)
    return redirect(url_for("kind", plural=available[0]["plural"]))


@app.route("/healthz")
def healthz():
    return "ok"


@app.route("/<plural>", methods=["GET", "POST"])
def kind(plural):
    crd = find(plural)
    if crd is None:
        return redirect(url_for("index"))
    flat = fields(crd["schema"])
    message = None

    if request.method == "POST":
        namespace = (request.form.get("namespace") or "").strip()
        name = (request.form.get("name") or "").strip()
        try:
            if request.form.get("action") == "delete":
                _custom().delete_namespaced_custom_object(
                    GROUP, crd["version"], namespace, plural, name)
                message = "deleted %s %s/%s" % (crd["kind"], namespace, name)
            else:
                if not name:
                    raise ValueError("name is required")
                raw = (request.form.get("__yaml") or "").strip()
                spec = yaml.safe_load(raw) if raw else build(request.form, flat)
                if not isinstance(spec, dict):
                    raise ValueError("spec must be a mapping")
                apply_object(crd, namespace, name, spec)
                message = "applied %s %s/%s" % (crd["kind"], namespace, name)
        except client.ApiException as exc:
            message = "%s %s: %s" % (exc.status, exc.reason, exc.body)
        except (ValueError, yaml.YAMLError) as exc:
            message = "%s: %s" % (type(exc).__name__, exc)

    found, error = objects(crd)
    editing = None
    want = request.args.get("edit")
    if want:
        editing = next((o for o in found if "%s/%s" % (o["namespace"], o["name"]) == want), None)

    return render_template_string(
        PAGE, crds=crds(), crd=crd, objects=found, flat=flat,
        editing=editing, error=error, message=message, lookup=lookup)


PAGE = """
<!doctype html>
<html lang="en"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>FHIR operator</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
<style>
  .depth-1 { padding-left: 1.5rem; } .depth-2 { padding-left: 3rem; }
  .depth-3 { padding-left: 4.5rem; }
  .group-row td { background: var(--bs-tertiary-bg); font-weight: 600; }
</style>
</head><body class="bg-body-tertiary">

<nav class="navbar navbar-expand bg-dark" data-bs-theme="dark"><div class="container-fluid">
  <span class="navbar-brand">FHIR operator</span>
  <ul class="navbar-nav me-auto">
  {% for c in crds %}
    <li class="nav-item"><a class="nav-link {{ 'active' if crd and c.plural == crd.plural }}"
       href="/{{ c.plural }}">{{ c.kind }}</a></li>
  {% endfor %}
  </ul>
  <span class="navbar-text small" id="tick"></span>
</div></nav>

<main class="container-fluid py-4">

{% if error %}<div class="alert alert-danger"><pre class="mb-0">{{ error }}</pre></div>{% endif %}
{% if message %}<div class="alert alert-secondary"><pre class="mb-0 small">{{ message }}</pre></div>{% endif %}

{% if crd %}
<div id="objects">
<div class="card mb-4">
  <div class="card-header d-flex justify-content-between align-items-center">
    <span>{{ crd.kind }} <span class="text-body-secondary small">{{ crd.plural }}.{{ crd.version }}</span></span>
    <a class="btn btn-sm btn-primary" href="#editor">+ New {{ crd.kind }}</a>
  </div>
  <table class="table table-sm mb-0 align-middle">
    <thead><tr><th>Namespace</th><th>Name</th><th>Phase</th><th>Status</th><th>Spec</th><th class="text-end">Actions</th></tr></thead>
    <tbody>
    {% for o in objects %}
      <tr>
        <td class="font-monospace">{{ o.namespace }}</td>
        <td class="font-monospace">{{ o.name }}</td>
        <td>{% if o.phase == 'Ready' %}<span class="badge text-bg-success">Ready</span>
            {% elif o.phase %}<span class="badge text-bg-secondary">{{ o.phase }}</span>
            {% else %}-{% endif %}</td>
        <td class="small font-monospace">{{ o.summary or '-' }}</td>
        <td><details><summary class="small text-body-secondary">yaml</summary>
            <pre class="small mb-0 mt-2">{{ o.yaml }}</pre></details></td>
        <td class="text-end text-nowrap">
          <a class="btn btn-sm btn-outline-secondary"
             href="/{{ crd.plural }}?edit={{ o.namespace }}/{{ o.name }}#editor">Edit</a>
          <form method="post" class="d-inline">
            <input type="hidden" name="namespace" value="{{ o.namespace }}">
            <input type="hidden" name="name" value="{{ o.name }}">
            <input type="hidden" name="action" value="delete">
            <button class="btn btn-sm btn-outline-danger"
                    onclick="return confirm('Delete {{ crd.kind }} {{ o.namespace }}/{{ o.name }}? Anything it owns goes too.')">Delete</button>
          </form>
        </td>
      </tr>
    {% else %}
      <tr><td colspan="6" class="text-body-secondary">
        No {{ crd.kind }} objects. Use the form below.</td></tr>
    {% endfor %}
    </tbody>
  </table>
</div>
</div>

<div class="card" id="editor">
  <div class="card-header">
    {% if editing %}Edit <span class="font-monospace">{{ editing.namespace }}/{{ editing.name }}</span>
    {% else %}New {{ crd.kind }}{% endif %}
  </div>
  <div class="card-body">
  <form method="post">
    <div class="row g-2 mb-3">
      <div class="col-md-4"><label class="form-label" for="namespace">Namespace</label>
        <input class="form-control font-monospace" id="namespace" name="namespace"
               value="{{ editing.namespace if editing else '' }}" placeholder="perf-s"
               {{ 'readonly' if editing }} {{ 'required' if crd.scope == 'Namespaced' }}>
        <div class="form-text">Created if absent.</div></div>
      <div class="col-md-4"><label class="form-label" for="name">Name</label>
        <input class="form-control font-monospace" id="name" name="name"
               value="{{ editing.name if editing else '' }}" placeholder="perf-s"
               {{ 'readonly' if editing }} required></div>
    </div>

    <table class="table table-sm align-middle mb-3">
      <tbody>
      {% for f in flat %}
        {% if f.type == 'group' %}
          <tr class="group-row"><td colspan="2" class="depth-{{ f.depth }}">{{ f.name }}</td></tr>
        {% else %}
          <tr>
            <td class="depth-{{ f.depth }}" style="width: 40%">
              <label class="form-label mb-0 small" for="f-{{ f.path }}">{{ f.name }}</label>
              {% if f.desc %}<div class="form-text small">{{ f.desc }}</div>{% endif %}
            </td>
            <td>
              {% set current = lookup(editing.spec, f.path) if editing else '' %}
              {% if f.enum %}
                <select class="form-select form-select-sm" id="f-{{ f.path }}" name="{{ f.path }}">
                  <option value="">(unset{% if f.default %}, default {{ f.default }}{% endif %})</option>
                  {% for option in f.enum %}
                    <option value="{{ option }}" {{ 'selected' if current == option }}>{{ option }}</option>
                  {% endfor %}
                </select>
              {% elif f.type == 'boolean' %}
                <select class="form-select form-select-sm" id="f-{{ f.path }}" name="{{ f.path }}">
                  <option value="">(unset)</option>
                  <option value="true" {{ 'selected' if current == 'true' }}>true</option>
                  <option value="false" {{ 'selected' if current == 'false' }}>false</option>
                </select>
              {% elif f.type == 'yaml' %}
                <textarea class="form-control form-control-sm font-monospace" rows="3"
                          id="f-{{ f.path }}" name="{{ f.path }}">{{ current }}</textarea>
              {% elif f.type in ('number', 'integer') %}
                <input class="form-control form-control-sm font-monospace" type="number"
                       step="{{ '1' if f.type == 'integer' else 'any' }}"
                       id="f-{{ f.path }}" name="{{ f.path }}" value="{{ current }}"
                       placeholder="{{ f.default if f.default is not none else '' }}">
              {% else %}
                <input class="form-control form-control-sm font-monospace" type="text"
                       id="f-{{ f.path }}" name="{{ f.path }}" value="{{ current }}"
                       placeholder="{{ f.default if f.default is not none else '' }}">
              {% endif %}
            </td>
          </tr>
        {% endif %}
      {% else %}
        <tr><td class="text-body-secondary">
          This CRD has no structural schema. Use the YAML box below.</td></tr>
      {% endfor %}
      </tbody>
    </table>

    <details {{ 'open' if not flat }}>
      <summary class="small text-body-secondary mb-2">Raw spec YAML (overrides the form when filled in)</summary>
      <textarea class="form-control font-monospace mt-2" rows="10" name="__yaml"
                placeholder="cpuLimitPolicy: strict&#10;resources:&#10;  postgres:&#10;    memory: {min: 16, max: 16}">{{ '' }}</textarea>
    </details>

    <div class="mt-3">
      <button class="btn btn-primary" name="action" value="apply">Apply</button>
      <a class="btn btn-outline-secondary" href="/{{ crd.plural }}">Clear</a>
    </div>
  </form>
  </div>
</div>
{% endif %}

</main>

<script>
// Refresh the object list only. The editor is outside #objects, so anything
// half-typed survives. A meta refresh used to reload the whole page and wipe
// the form, which left Apply blocked on its own required fields.
async function refresh() {
  if (document.querySelector('#editor :focus')) { return; }   // never interrupt typing
  try {
    var response = await fetch(window.location.pathname, {headers: {'X-Requested-With': 'refresh'}});
    var doc = new DOMParser().parseFromString(await response.text(), 'text/html');
    var fresh = doc.getElementById('objects');
    if (fresh && document.getElementById('objects')) {
      document.getElementById('objects').innerHTML = fresh.innerHTML;
      document.getElementById('tick').textContent = new Date().toLocaleTimeString();
    }
  } catch (err) {
    document.getElementById('tick').textContent = 'stale';
  }
}
if (document.getElementById('objects')) { setInterval(refresh, 10000); }
</script>
</body></html>
"""
