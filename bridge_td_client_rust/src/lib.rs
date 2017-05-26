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
use td_client::table_import::*;

#[repr(C)]
pub struct TdQueryState {
    job_id: u64,
    result_receiver: Receiver<Option<Vec<Value>>>
}

#[repr(C)]
pub struct TdImportState {
    td_client: Client<DefaultRequestExecutor>,
    database: String,
    table: String,
    column_size: usize,
    column_types: Vec<ImportColumnType>,
    column_names: Vec<String>,
    writable_chunk: TableImportWritableChunk
}

#[derive(Debug)]
enum ImportColumnType {
    Int,
    Float,
    String,
    Bytes,
    Map,
    Array
}

impl <'a> From<&'a str> for ImportColumnType {
    fn from(s: &'a str) -> Self {
        match s {
            "int" => ImportColumnType::Int,
            "float" => ImportColumnType::Float,
            "string" => ImportColumnType::String,
            "bytes" => ImportColumnType::Bytes,
            "map" => ImportColumnType::Map,
            "array" => ImportColumnType::Array,
            _ => panic!("Unexpected ImportColumnType: {:?}", s)
        }
    }
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

fn create_client<'a>(apikey: &'a str, endpoint: &'a Option<&'a str>) -> Client<DefaultRequestExecutor> {
    let mut client = Client::new(apikey);
    if endpoint.is_some() {
        client.endpoint(endpoint.unwrap());
    }
    client
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
    log!(debug_log, "issue_query: apikey.len={:?}", apikey.len());
    log!(debug_log, "issue_query: endpoint={:?}", endpoint);
    log!(debug_log, "issue_query: query_engine={:?}", query_engine);
    log!(debug_log, "issue_query: database={:?}", database);
    log!(debug_log, "issue_query: query={:?}", query);

    let client = create_client(apikey, &endpoint);
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
    add_bytes: extern fn(*const c_void, usize, &[u8]),
    debug_log: extern fn(usize, &[u8]),
    error_log: extern fn(usize, &[u8])
    ) -> bool {

    let query_state = unsafe { &mut *td_query_state };

    fn encoded_str_of_pg_bool(b: bool) -> &'static str {
        if b { "1" }
        else { "0" }
    }

    fn encoded_string_of_pg_array_elm(x: &Value, debug_log: extern fn(usize, &[u8])) -> String {
        match x {
            &Value::Nil => "NULL".to_string(),
            &Value::Boolean(b) => encoded_str_of_pg_bool(b).to_string(),
            &Value::Integer(Integer::U64(ui)) => ui.to_string(),
            &Value::Integer(Integer::I64(si)) => si.to_string(),
            &Value::Float(Float::F32(f)) => f.to_string(),
            &Value::Float(Float::F64(d)) => d.to_string(),
            &Value::String(ref s) => format!("\"{}\"", s),
            &Value::Array(ref xs) => {
                let s = xs.iter()
                    .map(|x| encoded_string_of_pg_array_elm(x, debug_log))
                    .collect::<Vec<String>>()
                    .join(",");
                format!("{{{}}}", s)
            },
            other => {
                log!(debug_log, "fetch_result_row: {:?} is not supported as an array", other);
                "NULL".to_string()
            }
        }
    }

    let call_add_bytes = |bytes: &[u8]| {
        add_bytes(context, bytes.len(), bytes)
    };

    let encode_value_in_pg = |x: Value| {
        match x {
            Value::Nil =>
                add_nil(context),
            Value::Boolean(b) =>
                call_add_bytes(encoded_str_of_pg_bool(b).as_bytes()),
            Value::Integer(Integer::U64(ui)) =>
                call_add_bytes(ui.to_string().as_bytes()),
            Value::Integer(Integer::I64(si)) =>
                call_add_bytes(si.to_string().as_bytes()),
            Value::Float(Float::F32(f)) =>
                call_add_bytes(f.to_string().as_bytes()),
            Value::Float(Float::F64(d)) =>
                call_add_bytes(d.to_string().as_bytes()),
            Value::String(s) =>
                call_add_bytes(s.as_bytes()),
            Value::Binary(bs) =>
                call_add_bytes(bs.as_slice()),
            Value::Array(_) =>
                call_add_bytes(encoded_string_of_pg_array_elm(&x, debug_log).as_bytes()),
            other => {
                log!(debug_log, "fetch_result_row: {:?} is not supported", other);
                add_nil(context)
            },
        }
    };

    match query_state.result_receiver.recv() {
        Ok(result) => match result {
            Some(xs) => {
                for x in xs.into_iter() {
                    encode_value_in_pg(x)
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
pub extern fn import_begin(
    raw_apikey: *const c_char,
    raw_endpoint: *const c_char,
    raw_database: *const c_char,
    raw_table: *const c_char,
    column_size: usize,
    raw_coltypes: *const *const c_char,
    raw_colnames: *const *const c_char,
    debug_log: extern fn(usize, &[u8]),
    error_log: extern fn(usize, &[u8])
    ) -> *mut TdImportState {

    log!(debug_log, "import_begin: start");

    let apikey = convert_str_from_raw_str(raw_apikey);
    let endpoint = convert_str_opt_from_raw_str(raw_endpoint);
    let database = convert_str_from_raw_str(raw_database);
    let table = convert_str_from_raw_str(raw_table);

    let sliced_raw_coltypes = unsafe {
        std::slice::from_raw_parts(raw_coltypes, column_size)
    };
    let column_types = sliced_raw_coltypes
        .iter()
        .map(|x| 
             ImportColumnType::from(
                 unsafe { CStr::from_ptr(*x) }
                 .to_str().unwrap())
            )
        .collect::<Vec<ImportColumnType>>();

    let sliced_raw_colnames = unsafe {
        std::slice::from_raw_parts(raw_colnames, column_size)
    };
    let column_names = sliced_raw_colnames
        .iter()
        .map(|x| unsafe { CStr::from_ptr(*x) }.to_str().unwrap().to_string())
        .collect::<Vec<String>>();

    log!(debug_log, "import_begin: apikey.len={:?}", apikey.len());
    log!(debug_log, "import_begin: endpoint={:?}", endpoint);
    log!(debug_log, "import_begin: database={:?}", database);
    log!(debug_log, "import_begin: table={:?}", table);
    log!(debug_log, "import_begin: colmun_size={:?}", column_size);
    log!(debug_log, "import_begin: colmun_types={:?}", column_types);
    log!(debug_log, "import_begin: colmun_names={:?}", column_names);

    let client = create_client(apikey, &endpoint);
    let result = TableImportWritableChunk::new();
    if result.is_err() {
        // TODO: Fix ownership problem of `result` here to show the cause
        log!(error_log, "import_begin: Failed to create a writable chunk");
    }
    let writable_chunk = result.unwrap();

    let import_state = TdImportState {
        td_client: client,
        database: database.to_string(),
        table: table.to_string(),
        column_size: column_size,
        column_types: column_types,
        column_names: column_names,
        writable_chunk: writable_chunk
    };

    let td_import_state = Box::into_raw(Box::new(import_state));

    log!(debug_log, "import_begin: finished");

    td_import_state
}

#[no_mangle]
#[allow(unused_variables)]
pub extern fn import_append(
    td_import_state: *mut TdImportState,
    raw_values: *const *const c_char,
    debug_log: extern fn(usize, &[u8]),
    error_log: extern fn(usize, &[u8])) {

    let import_state = unsafe { &mut *td_import_state };
    let column_size = import_state.column_size;
    let column_types = &import_state.column_types;
    let column_names = &import_state.column_names;
    let mut writable_chunk = &mut import_state.writable_chunk;
    writable_chunk.next_row(column_types.len() as u32).unwrap();

    let sliced_raw_values = unsafe {
        std::slice::from_raw_parts(raw_values, column_size)
    };
    let values = sliced_raw_values
        .iter()
        .map(|x| convert_str_opt_from_raw_str(*x))
        .collect::<Vec<Option<&str>>>();

    let mut i = 0;
    for coltype in column_types {
        let ref colname = column_names[i];
        match values[i] {
            Some(value_str) => {
                match coltype {
                    &ImportColumnType::Int => {
                        writable_chunk
                            .write_key_and_i64(colname.as_str(), value_str.parse::<i64>().unwrap())
                            .unwrap();
                        ()
                    },

                    &ImportColumnType::Float => {
                        writable_chunk
                            .write_key_and_f64(colname.as_str(), value_str.parse::<f64>().unwrap())
                            .unwrap();
                        ()
                    },

                    &ImportColumnType::String => {
                        writable_chunk
                            .write_key_and_str(colname.as_str(), value_str)
                            .unwrap();
                        ()
                    },

                    &ImportColumnType::Bytes => {
                        writable_chunk
                            .write_key_and_bin(colname.as_str(), value_str.as_bytes())
                            .unwrap();
                        ()
                    },

                    &ImportColumnType::Map =>
                        log!(error_log, "import_append: MAP type isn't supported yet"),

                    &ImportColumnType::Array =>
                        log!(error_log, "import_append: ARRAY type isn't supported yet"),
                }
            },
            None => {
                writable_chunk.write_key_and_nil(colname.as_str()).unwrap();
            }
        }
        i += 1;
    }
}

#[no_mangle]
pub extern fn import_commit(
    td_import_state: *mut TdImportState,
    debug_log: extern fn(usize, &[u8]),
    error_log: extern fn(usize, &[u8])) {

    log!(debug_log, "import_commit: start");

    let import_state = unsafe { &mut *td_import_state };
    let mut writable_chunk = TableImportWritableChunk::new().unwrap();
    let ref mut orig_writable_chunk = import_state.writable_chunk;
    std::mem::swap(&mut writable_chunk, orig_writable_chunk);
    drop(orig_writable_chunk);
    match writable_chunk.close() {
        Ok(readable_chunk) => {
            let client = &import_state.td_client;
            let result = client.import_msgpack_gz_file_to_table(
                import_state.database.as_str(),
                import_state.table.as_str(),
                readable_chunk.file_path.as_str(), None);
            if result.is_err() {
                drop(readable_chunk);
                unsafe { Box::from_raw(td_import_state) };
                log!(error_log, "import_commit: Failed to import readable chunk: {:?}", result);
            }
        },
        Err(err) => {
            unsafe { Box::from_raw(td_import_state) };
            log!(error_log, "import_commit: Failed to close writable chunk: {:?}", err);
        },
    };

    unsafe { Box::from_raw(td_import_state) };

    log!(debug_log, "import_commit: finished");
}

#[no_mangle]
pub extern fn release_resource(td_query_state: *mut TdQueryState) {
    unsafe {
        Box::from_raw(td_query_state);
    }
}

