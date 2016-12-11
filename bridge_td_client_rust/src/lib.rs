extern crate libc;
extern crate td_client;
use libc::{c_char, c_void};
use std::ffi::CStr;
use std::ptr::null_mut;
use std::str::FromStr;
use std::sync::mpsc::{self, Receiver, RecvError};
use std::thread;
use td_client::client::*;
use td_client::model::*;
use td_client::value::*;

#[repr(C)]
pub struct TdQueryState {
    job_id: u64,
    result_receiver: Receiver<Option<Vec<Value>>>
}

fn convert_str_from_raw_str<'a>(raw: *const c_char) -> &'a str {
    unsafe {
        assert!(!raw.is_null());
        CStr::from_ptr(raw).to_str().unwrap()
    }
}

fn convert_str_opt_from_raw_str<'a>(raw: *const c_char) -> Option<&'a str> {
    unsafe {
        if raw.is_null() {
            None
        }
        else {
            Some(CStr::from_ptr(raw).to_str().unwrap())
        }
    }
}

macro_rules! log {
    ($log_fn:ident, $msg:expr) => ({
        let bytes = $msg.as_bytes();
        $log_fn(bytes.len(), bytes)
    });
    ($log_fn:ident, $fmt:expr, $($arg:tt)*) => ({
        let s = format!($fmt, $($arg)*);
        let bytes = s.as_bytes();
        $log_fn(bytes.len(), bytes)
    });
}

#[no_mangle]
pub extern fn issue_query(
    raw_apikey: *const c_char,
    raw_endpoint: *const c_char,
    raw_query_engine: *const c_char,
    raw_database: *const c_char,
    raw_query: *const c_char,
    debug_log: extern fn(usize, &[u8]),
    error_log: extern fn(usize, &[u8])
    ) -> *mut TdQueryState {
    let apikey = convert_str_from_raw_str(raw_apikey);
    let endpoint = convert_str_opt_from_raw_str(raw_endpoint);
    let query_engine = convert_str_from_raw_str(raw_query_engine);
    let database = convert_str_from_raw_str(raw_database);
    let query = convert_str_from_raw_str(raw_query);

    log!(debug_log, "issue_query: start");
    log!(debug_log, "issue_query: apikey={:?}", apikey);
    log!(debug_log, "issue_query: endpoint={:?}", endpoint);
    log!(debug_log, "issue_query: query_engine={:?}", query_engine);
    log!(debug_log, "issue_query: database={:?}", database);
    log!(debug_log, "issue_query: query={:?}", query);

    let mut client = Client::new(apikey);
    if endpoint.is_some() {
        client.endpoint(endpoint.unwrap());
    }
    let query_type = match QueryType::from_str(query_engine) {
        Ok(query_type) => query_type,
        Err(err) => {
            log!(error_log, "Invalid query engine. query_engine={:?}. error={:?}", 
                    query_engine, err);
            return null_mut()
        }
    };
    log!(debug_log, "issue_query: query_type={:?}", query_type);
    
    let job_id = match client.issue_job(query_type, database, query,
                                None, None, None, None, None) {
        Ok(job_id) => job_id,
        Err(err) => {
            log!(error_log, "Failed to issue query. database={:?}, query={:?}, error={:?}", 
                    database, query, err);
            return null_mut()
        }
    };

    match client.wait_job(job_id, None) {
        Ok(JobStatus::Success) => (),
        Ok(status) => {
            log!(error_log, "Unexpected status. job_id={:?}, status={:?}", job_id, status);
            return null_mut()
        },
        Err(err) => {
            log!(error_log, "Failed to wait query execution. job_id={:?}, error={:?}", 
                     job_id, err);
            return null_mut()
        }
    }

    log!(debug_log, "issue_query: job finished. job_id={:?}", job_id);

    let (tx, rx) = mpsc::channel();

    thread::spawn(move || {
        match client.each_row_in_job_result(
            job_id,
            &|xs: Vec<Value>| match tx.send(Some(xs)) {
                Ok(()) => (),
                Err(err) => {
                    log!(error_log, "Failed to pass results. job_id={:?}, error={:?}",
                             job_id, err)
                    // TODO: How to propagate this error....?
                }
            }
        ) {
            Ok(()) => match tx.send(None) {
                Ok(()) => (),
                Err(err) => {
                    log!(error_log, "Failed put sentinel in the queue. job_id={:?}, error={:?}",
                             job_id, err)
                }
            },
            Err(err) => {
                log!(error_log, "Failed to fetch result. job_id={:?}, error={:?}",
                         job_id, err)
            }
        }
    });

    let query_state = TdQueryState {
        job_id: job_id,
        result_receiver: rx
    };

    let td_query_state = Box::into_raw(Box::new(query_state));

    log!(debug_log, "issue_query: finished");

    td_query_state
}

#[allow(unused_variables)]
#[no_mangle]
pub extern fn fetch_result_row(
    td_query_state: *mut TdQueryState,
    context: *const c_void,
    add_nil: extern fn(*const c_void),
    add_bool: extern fn(*const c_void, bool),
    add_u64: extern fn(*const c_void, u64),
    add_i64: extern fn(*const c_void, i64),
    add_f32: extern fn(*const c_void, f32),
    add_f64: extern fn(*const c_void, f64),
    add_string: extern fn(*const c_void, usize, &[u8]),
    add_bytes: extern fn(*const c_void, usize, &[u8]),
    debug_log: extern fn(usize, &[u8]),
    error_log: extern fn(usize, &[u8])
    ) -> bool {

    let query_state = unsafe { &mut *td_query_state };

    match query_state.result_receiver.recv() {
        Ok(result) => match result {
            Some(xs) => {
                for x in xs.into_iter() {
                    match x {
                        Value::Nil => add_nil(context),
                        Value::Boolean(b) => add_bool(context, b),
                        Value::Integer(Integer::U64(ui)) => add_u64(context, ui),
                        Value::Integer(Integer::I64(si)) => add_i64(context, si),
                        Value::Float(Float::F32(f)) => add_f32(context, f),
                        Value::Float(Float::F64(d)) => add_f64(context, d),
                        Value::String(s) => {
                            let bytes = s.as_bytes();
                            add_string(context, bytes.len(), bytes)
                        },
                        Value::Binary(bs) => {
                            let bytes = bs.as_slice();
                            add_bytes(context, bytes.len(), bytes)
                        },
                        other => {
                            log!(debug_log, "fetch_result_row: {:?} is not supported", other);
                            add_nil(context)
                        },
                    }
                };
                true
            },
            None => {
                drop(td_query_state);
                false
            }
        },
        Err(RecvError) => {
            drop(query_state);
            false
        }
    }
}

#[no_mangle]
pub extern fn release_resource(td_query_state: *mut TdQueryState) {
    unsafe {
        Box::from_raw(td_query_state);
    }
}

