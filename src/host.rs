//! Owned copy of CPA's native host callback table.
use crate::{CliproxyBuffer, CliproxyHostApi, Free, HostCall};
use serde_json::{json, Value};
use std::ffi::{c_void, CString};
use std::sync::OnceLock;

struct Host {
    context: usize,
    call: HostCall,
    free: Free,
}
static HOST: OnceLock<Host> = OnceLock::new();

#[derive(Debug)]
pub struct Fault {
    pub status: u16,
    pub message: String,
}

impl Fault {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            status: 502,
            message: message.into(),
        }
    }
    pub fn envelope(&self) -> Value {
        json!({"ok": false, "error": {"code": "cpa_execution_error",
            "message": self.message, "http_status": if self.status == 0 { 502 } else { self.status }}})
    }
}

pub fn install(api: &CliproxyHostApi) -> Result<(), ()> {
    HOST.set(Host {
        context: api.host_ctx as usize,
        call: api.call.ok_or(())?,
        free: api.free_buffer.ok_or(())?,
    })
    .map_err(|_| ())
}

pub fn call(method: &str, request: Value) -> Result<Value, Fault> {
    let host = HOST
        .get()
        .ok_or_else(|| Fault::new("Host callbacks unavailable"))?;
    let method = CString::new(method).map_err(|_| Fault::new("Invalid host method"))?;
    let bytes = serde_json::to_vec(&request).map_err(|_| Fault::new("Invalid host request"))?;
    let mut output = CliproxyBuffer {
        ptr: std::ptr::null_mut(),
        len: 0,
    };
    // CPA keeps the table/context alive until plugin shutdown. Shutdown joins
    // all workers before returning; no callback survives library unloading.
    unsafe {
        let code = (host.call)(
            host.context as *mut c_void,
            method.as_ptr(),
            bytes.as_ptr(),
            bytes.len(),
            &mut output,
        );
        let result = if code != 0 || output.ptr.is_null() || output.len > 64 * 1024 * 1024 {
            Err(Fault::new("Invalid host callback response"))
        } else {
            serde_json::from_slice::<Value>(std::slice::from_raw_parts(output.ptr, output.len))
                .map_err(|_| Fault::new("Invalid host response JSON"))
        };
        if !output.ptr.is_null() {
            (host.free)(output.ptr.cast(), output.len);
        }
        let envelope = result?;
        if envelope["ok"] != true {
            return Err(Fault {
                status: envelope["error"]["http_status"]
                    .as_u64()
                    .filter(|s| (400..=599).contains(s))
                    .unwrap_or(0) as u16,
                message: envelope["error"]["message"]
                    .as_str()
                    .unwrap_or("Host execution failed")
                    .into(),
            });
        }
        Ok(envelope["result"].clone())
    }
}
