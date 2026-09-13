"""#184 E0 live-evidence helpers (pre-implementation; runs NO code under test).

Imports the MCP server from the PRISTINE worktree detached at the #184 branch
point cbab28f, never from the main working tree. Every evidence file goes under
.claude/agent-memory/boomi-qa-tester/evidence/issue-184/<capture-id>/.
"""
import os, sys, json, hashlib, datetime, time, subprocess, re, glob

WT = ("/private/tmp/claude-501/-Users-gleb-Documents-Projects-Renera-boomi-mcp-server/"
      "b66f0571-3d38-4a20-97c2-88c3a2c8ab8f/scratchpad/wt-cbab28f")
BRANCH_POINT = "cbab28ffc176ddb2378283cf7132eb9c37afc374"
REPO = "/Users/gleb/Documents/Projects/Renera/boomi-mcp-server"
HERE = os.path.dirname(os.path.abspath(__file__))
EVD = os.path.join(REPO, ".claude/agent-memory/boomi-qa-tester/evidence/issue-184")
STATE = os.path.join(HERE, "state")
os.makedirs(STATE, exist_ok=True)

for p in (os.path.join(WT, "src"), WT):
    if p not in sys.path:
        sys.path.insert(0, p)
os.environ["BOOMI_LOCAL"] = "true"

P = "renera"
ACCOUNT = "trainingglebbochkarov-16926N"
ATOM = "a40a3313-1537-47c4-91c7-573a703537c7"
ENV = "b173ce8a-2d7f-41e3-9d3f-06e45d5b0de9"
REST_CONN = "2fe488e4-3169-4529-9515-d854570c8ffc"
REST_SUB = "officialboomi-X3979C-rest-prod"
MOCK_CONTAINER = "cds-mock"

_server = None


def srv():
    global _server
    if _server is None:
        import server
        f = os.path.realpath(server.__file__)
        assert f.startswith(os.path.realpath(WT)), f"server imported from {f}, not the worktree"
        import boomi_mcp
        bf = os.path.realpath(boomi_mcp.__file__)
        assert bf.startswith(os.path.realpath(WT)), f"boomi_mcp imported from {bf}"
        _server = server
    return _server


def call(_tool_name, **kw):
    t = getattr(srv(), _tool_name)
    fn = getattr(t, "fn", t)
    return fn(**kw)


def utc():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def rfc():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha(b):
    if isinstance(b, str):
        b = b.encode("utf-8")
    return hashlib.sha256(b).hexdigest()


def tree_stamp():
    """sha256 over the worktree's tracked-code bytes (src/**/*.py + server.py) + its HEAD."""
    h = hashlib.sha256()
    files = sorted(glob.glob(os.path.join(WT, "src", "**", "*.py"), recursive=True)) + [os.path.join(WT, "server.py")]
    for f in files:
        h.update(os.path.relpath(f, WT).encode())
        with open(f, "rb") as fh:
            h.update(fh.read())
    head = subprocess.run(["git", "-C", WT, "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip()
    dirty = subprocess.run(["git", "-C", WT, "status", "--porcelain", "--untracked-files=no"],
                           capture_output=True, text=True).stdout.strip()
    return {"wt_head": head, "wt_head_is_branch_point": head == BRANCH_POINT,
            "wt_tracked_dirty": bool(dirty), "code_files": len(files), "code_sha256": h.hexdigest()}


def redact(s):
    s = re.sub(r'(createdBy|modifiedBy)="[^"]*"', r'\1="<redacted>"', s)
    s = re.sub(r'(<bns:encryptedValues>).*?(</bns:encryptedValues>)', r'\1<redacted>\2', s, flags=re.S)
    return s


# ------------------------------------------------------------------ evidence
def evd_dir(cap):
    d = os.path.join(EVD, cap)
    os.makedirs(d, exist_ok=True)
    return d


def archive(cap, fname, data):
    d = evd_dir(cap)
    if not isinstance(data, (bytes, str)):
        data = json.dumps(data, indent=2, default=str)
    if isinstance(data, str):
        data = data.encode("utf-8")
    p = os.path.join(d, fname)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "wb") as f:
        f.write(data)
    return {"file": fname, "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}


def save_state(name, obj):
    with open(os.path.join(STATE, name + ".json"), "w") as f:
        json.dump(obj, f, indent=2, default=str)


def load_state(name, default=None):
    p = os.path.join(STATE, name + ".json")
    if not os.path.exists(p):
        return default
    with open(p) as f:
        return json.load(f)


# ------------------------------------------------------------------ platform
def api(endpoint, method="GET", payload=None, accept=None):
    kw = dict(profile=P, endpoint=endpoint, method=method)
    if payload is not None:
        kw["payload"] = payload if isinstance(payload, str) else json.dumps(payload)
    if accept:
        kw["accept"] = accept
    if method not in ("GET",):
        kw["confirm_write"] = True
    return call("invoke_boomi_api", **kw)


def api_query(object_name, prop, value, op="EQUALS"):
    payload = {"QueryFilter": {"expression": {"argument": [value], "operator": op, "property": prop}}}
    return call("invoke_boomi_api", profile=P, endpoint=f"{object_name}/query", method="POST",
                payload=json.dumps(payload))


def component_xml(cid):
    r = call("invoke_boomi_api", profile=P, endpoint=f"/Component/{cid}", accept="xml")
    return r.get("raw_response") or ""


HDR = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
       '<bns:Component xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
       'xmlns:bns="http://api.platform.boomi.com/" name="{name}" type="{type}"{sub}>'
       '<bns:encryptedValues/><bns:description></bns:description>'
       '<bns:object>{obj}</bns:object></bns:Component>')


def comp_xml(name, ctype, obj, sub=""):
    return HDR.format(name=name, type=ctype, sub=(f' subType="{sub}"' if sub else ""), obj=obj)


def create_component(name, ctype, obj, sub=""):
    xml = comp_xml(name, ctype, obj, sub)
    r = call("manage_component", profile=P, action="create", config=json.dumps({"xml": xml}))
    cid = r.get("component_id") or (r.get("component") or {}).get("id") or (r.get("component") or {}).get("component_id")
    return cid, xml, r


def update_component_raw(cid, name, ctype, obj, version, sub=""):
    """POST /Component/<id> with explicit componentId+version (platform UPDATE)."""
    xml = comp_xml(name, ctype, obj, sub).replace(
        f'name="{name}"', f'componentId="{cid}" version="{version}" name="{name}"', 1)
    r = call("invoke_boomi_api", profile=P, endpoint=f"/Component/{cid}", method="POST",
             payload=xml, content_type="xml", accept="xml", confirm_write=True)
    return xml, r


def package_and_deploy(cid, notes):
    pkgv = "1.0." + time.strftime("%m%d%H%M%S", time.gmtime())
    pkg = call("manage_deployment", profile=P, action="create_package",
               config=json.dumps({"component_id": cid, "component_type": "process",
                                  "package_version": pkgv, "notes": notes}))
    pid = (pkg.get("package") or {}).get("package_id") or pkg.get("package_id")
    if not pid:
        return {"package": pkg, "deploy": None, "package_id": None, "deployment_id": None}
    dep = call("manage_deployment", profile=P, action="deploy", package_id=pid, environment_id=ENV,
               config=json.dumps({"notes": notes}))
    did = (dep.get("deployment") or {}).get("deployment_id") or dep.get("deployment_id")
    return {"package": pkg, "deploy": dep, "package_id": pid, "deployment_id": did}


def undeploy(deployment_id):
    return call("manage_deployment", profile=P, action="undeploy",
                config=json.dumps({"deployment_id": deployment_id}))


def execute(pid, timeout=240):
    t0 = utc()
    ex = call("execute_process", profile=P, process_id=pid, atom_id=ATOM, environment_id=ENV,
              config=json.dumps({"wait": True, "timeout": timeout}))
    er = ex.get("execution_result") or {}
    return {"started_utc": t0, "execute": ex, "execution_id": er.get("execution_id") or ex.get("execution_id"),
            "wait_status": er.get("status")}


def poll_record(execution_id, tries=24, delay=5):
    rows = []
    r = None
    for _ in range(tries):
        r = api_query("ExecutionRecord", "executionId", execution_id)
        rows = ((r.get("data") or {}).get("result")) or []
        if rows and rows[0].get("status") not in ("INPROCESS", "STARTED", None):
            return rows
        time.sleep(delay)
    return rows


def records_for_process(process_id, since_iso):
    """ExecutionRecord rows for a process since a UTC instant (child records)."""
    payload = {"QueryFilter": {"expression": {"operator": "and", "nestedExpression": [
        {"argument": [process_id], "operator": "EQUALS", "property": "processId"},
        {"argument": [since_iso, "2099-01-01T00:00:00Z"], "operator": "BETWEEN", "property": "executionTime"}]}}}
    r = call("invoke_boomi_api", profile=P, endpoint="ExecutionRecord/query", method="POST",
             payload=json.dumps(payload))
    return ((r.get("data") or {}).get("result")) or [], r


def exec_logs(execution_id):
    r = call("monitor_platform", profile=P, action="execution_logs",
             config=json.dumps({"execution_id": execution_id}))
    files = r.get("files") or {}
    text = "\n".join(v for v in files.values() if isinstance(v, str))
    return text, r


# ------------------------------------------------------------------ counterparty
def mock_log(since):
    out = subprocess.run(["docker", "logs", "--since", since, MOCK_CONTAINER], capture_output=True, text=True)
    return (out.stdout or "") + (out.stderr or "")


def mock_lines(since, needle):
    return [l.rstrip() for l in mock_log(since).splitlines() if needle in l]
