"""Exercise the compiled Rust library using CPA's actual native ABI layout."""
import base64
import concurrent.futures
import ctypes as c
import json
import pathlib
import sys
import threading


class Buffer(c.Structure):
    _fields_ = [("ptr", c.c_void_p), ("len", c.c_size_t)]


Call = c.CFUNCTYPE(c.c_int, c.c_char_p, c.c_void_p, c.c_size_t, c.POINTER(Buffer))
Free = c.CFUNCTYPE(None, c.c_void_p, c.c_size_t)
Shutdown = c.CFUNCTYPE(None)


class Host(c.Structure):
    _fields_ = [("abi_version", c.c_uint32), ("host_ctx", c.c_void_p),
                ("call", c.c_void_p), ("free_buffer", c.c_void_p)]


class Plugin(c.Structure):
    _fields_ = [("abi_version", c.c_uint32), ("call", Call),
                ("free_buffer", Free), ("shutdown", Shutdown)]


library = c.CDLL(str(pathlib.Path(sys.argv[1]).resolve()))
library.cliproxy_plugin_init.argtypes = [c.POINTER(Host), c.POINTER(Plugin)]
library.cliproxy_plugin_init.restype = c.c_int
host, plugin = Host(1, None, None, None), Plugin()
assert library.cliproxy_plugin_init(c.byref(host), c.byref(plugin)) == 0
assert plugin.abi_version == 1


def call(method, payload):
    raw = json.dumps(payload).encode()
    response = Buffer()
    rc = plugin.call(method.encode(), raw, len(raw), c.byref(response))
    try:
        assert rc == 0
        result = json.loads(c.string_at(response.ptr, response.len))
        assert result["ok"], result
        return result["result"]
    finally:
        plugin.free_buffer(response.ptr, response.len)


config = """credentials:
  default:
    requests_per_minute: 0
    max_concurrency: 2
    max_queue: 100
    queue_timeout_ms: 5000
"""
registered = call("plugin.register", {"schema_version": 6,
                    "config_yaml": base64.b64encode(config.encode()).decode()})
assert registered["metadata"]["Name"] == "cpa-stg"
assert registered["metadata"]["GitHubRepository"] == "https://github.com/csbxd/cpa-stg"
assert registered["capabilities"]["request_lifecycle_plugin"] is True

# Invalid executor RPCs must not be mistaken for an open asynchronous stream.
for method in [b"executor.execute_stream", b"request.intercept_after"]:
    response = Buffer()
    assert plugin.call(method, None, 64 * 1024 * 1024 + 1, c.byref(response)) == 0
    try:
        result = json.loads(c.string_at(response.ptr, response.len))
        if method.startswith(b"request."):
            assert result["ok"] and result["result"]["Terminate"]
            assert result["result"]["StatusCode"] == 413
        else:
            assert not result["ok"] and result["error"]["http_status"] == 413
    finally:
        plugin.free_buffer(response.ptr, response.len)

gate = threading.Barrier(16)
lock = threading.Lock()
active = maximum = 0


def worker(number):
    global active, maximum
    request = {"RequestID": f"ffi-{number}", "Metadata": {"selected_auth_id": "one"},
               "Body": base64.b64encode(b'{"stream": true}').decode(), "Stream": True}
    assert call("request.intercept_before", request) == {}
    gate.wait(timeout=10)
    assert call("request.intercept_after", request) == {}
    with lock:
        active += 1
        maximum = max(maximum, active)
        assert active <= 2
    # Keep the host's terminal-event ordering: the simulated stream ends first.
    with lock:
        active -= 1
    assert call("request.complete", {"RequestID": request["RequestID"], "Outcome": "succeeded"}) == {}
    # A duplicated completion cannot underflow the counter.
    assert call("request.complete", {"RequestID": request["RequestID"]}) == {}


with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
    list(executor.map(worker, range(16)))
assert 1 <= maximum <= 2
assert active == 0

missing = {"RequestID": "missing", "Metadata": {}}
assert call("request.intercept_before", missing) == {}
assert call("request.intercept_after", missing) == {}
missing["Metadata"]["selected_auth_id"] = ""
denied = call("request.intercept_after", missing)
assert denied["Terminate"] and denied["StatusCode"] == 503
assert json.loads(base64.b64decode(denied["ResponseBody"]))["error"]["code"] == "cpa_invalid_credential"
call("request.complete", missing)
call("plugin.quiesce", {})
stopped = call("request.intercept_before", {"RequestID": "late"})
assert stopped["Terminate"] and stopped["StatusCode"] == 503
plugin.shutdown()
print("Native ABI smoke passed: registration, 16 concurrent callers, buffer release, termination, shutdown")
