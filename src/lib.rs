pub mod config;
pub mod engine;
pub mod error_mapping;
#[cfg(feature = "router")]
mod host;
pub mod protocol;
#[cfg(feature = "router")]
mod router;
pub mod runtime;

use runtime::Runtime;
use std::ffi::{c_char, c_void, CStr};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::sync::OnceLock;

const ABI_VERSION: u32 = 1;
const MAX_REQUEST_BYTES: usize = 64 * 1024 * 1024;
static RUNTIME: OnceLock<Runtime> = OnceLock::new();

#[repr(C)]
pub struct CliproxyBuffer {
    pub ptr: *mut u8,
    pub len: usize,
}

type HostCall =
    unsafe extern "C" fn(*mut c_void, *const c_char, *const u8, usize, *mut CliproxyBuffer) -> i32;
type Free = unsafe extern "C" fn(*mut c_void, usize);
type Call = unsafe extern "C" fn(*const c_char, *const u8, usize, *mut CliproxyBuffer) -> i32;

#[repr(C)]
pub struct CliproxyHostApi {
    pub abi_version: u32,
    pub host_ctx: *mut c_void,
    pub call: Option<HostCall>,
    pub free_buffer: Option<Free>,
}

#[repr(C)]
pub struct CliproxyPluginApi {
    pub abi_version: u32,
    pub call: Option<Call>,
    pub free_buffer: Option<Free>,
    pub shutdown: Option<unsafe extern "C" fn()>,
}

/// # Safety
/// The host must pass valid ABI tables and keep their storage alive for this call.
#[no_mangle]
pub unsafe extern "C" fn cliproxy_plugin_init(
    host: *const CliproxyHostApi,
    plugin: *mut CliproxyPluginApi,
) -> i32 {
    if host.is_null() || plugin.is_null() || (*host).abi_version != ABI_VERSION {
        return 1;
    }
    #[cfg(feature = "router")]
    if host::install(&*host).is_err() {
        return 1;
    }
    RUNTIME.get_or_init(Runtime::default);
    ptr::write(
        plugin,
        CliproxyPluginApi {
            abi_version: ABI_VERSION,
            call: Some(plugin_call),
            free_buffer: Some(plugin_free),
            shutdown: Some(plugin_shutdown),
        },
    );
    0
}

unsafe extern "C" fn plugin_call(
    method: *const c_char,
    request: *const u8,
    request_len: usize,
    response: *mut CliproxyBuffer,
) -> i32 {
    if response.is_null() {
        return 1;
    }
    ptr::write(
        response,
        CliproxyBuffer {
            ptr: ptr::null_mut(),
            len: 0,
        },
    );
    if method.is_null() {
        return 1;
    }
    let method = match CStr::from_ptr(method).to_str() {
        Ok(method) => method,
        Err(_) => return 1,
    };
    let interception = method.starts_with("request.intercept_");
    let result = catch_unwind(AssertUnwindSafe(|| {
        if request_len > MAX_REQUEST_BYTES || (request.is_null() && request_len != 0) {
            return if interception {
                protocol::terminate(
                    413,
                    "cpa_payload_limit",
                    "Plugin payload exceeds the 64 MiB limit or is invalid",
                )
            } else {
                // A successful executor response with no chunks means an async
                // stream. Invalid RPCs must fail instead of opening such a stream.
                serde_json::json!({"ok":false,"error":{"code":"cpa_payload_limit",
                    "message":"Plugin payload exceeds the 64 MiB limit or is invalid","http_status":413}})
            };
        }
        let raw = if request_len == 0 {
            &[]
        } else {
            std::slice::from_raw_parts(request, request_len)
        };
        #[cfg(feature = "router")]
        {
            router::handle(method, raw)
        }
        #[cfg(not(feature = "router"))]
        {
            protocol::handle(RUNTIME.get_or_init(Runtime::default), method, raw)
        }
    }));
    let value = result.unwrap_or_else(|_| {
        RUNTIME.get_or_init(Runtime::default).stop();
        if interception {
            // CPA logs interceptor RPC errors and continues, so policy failures
            // must be successful RPCs containing explicit termination responses.
            protocol::terminate(503, "cpa_internal_error", "Policy plugin stopped after an internal failure")
        } else {
            serde_json::json!({"ok": false, "error": {"code": "cpa_internal_error", "message": "Policy plugin stopped"}})
        }
    });
    let bytes = value.to_string().into_bytes().into_boxed_slice();
    let len = bytes.len();
    (*response).ptr = Box::into_raw(bytes) as *mut u8;
    (*response).len = len;
    0
}

unsafe extern "C" fn plugin_free(buffer: *mut c_void, len: usize) {
    if !buffer.is_null() {
        // Box<[u8]> preserves the exact allocation layout; Vec capacity need
        // not equal its length and must not be guessed across an FFI boundary.
        drop(Box::from_raw(ptr::slice_from_raw_parts_mut(
            buffer.cast::<u8>(),
            len,
        )));
    }
}

unsafe extern "C" fn plugin_shutdown() {
    let _ = catch_unwind(|| {
        #[cfg(feature = "router")]
        router::shutdown();
        if let Some(runtime) = RUNTIME.get() {
            runtime.stop();
        }
    });
}
