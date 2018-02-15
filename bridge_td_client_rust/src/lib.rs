extern crate hyper;
extern crate libc;
extern crate retry;
extern crate time;
extern crate uuid;
extern crate serde;
extern crate serde_json;
extern crate td_client;
use libc::{c_char, c_void, size_t};
use std::mem;
use std::slice;
use std::ffi::CStr;
use std::fs::{self, File};
use std::path::Path;
use std::ptr::null_mut;
use std::str::FromStr;
use std::sync::mpsc::{self, Receiver, RecvError};
use std::thread;
use retry::Retry;
use td_client::client::*;
use td_client::error::*;
use td_client::model::*;
use td_client::value::*;
use td_client::table_import::*;
use uuid::Uuid;

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
    uuid: Uuid,
    approximate_written_size: usize,
    column_size: usize,
    column_types: Vec<ImportColumnType>,
    column_names: Vec<String>,
    current_time: String,
    writable_chunk: TableImportWritableChunk
}

#[repr(C)]
pub struct PgTableSchema {
    name: *const c_void,
    numcols: size_t,
    colnames: *const *const c_void,
    coltypes: *const *const c_void
}

#[repr(C)]
pub struct PgTableSchemas {
    numtables: size_t,
    tables: *const *const PgTableSchema
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

const RETRY_FIXED_INTERVAL_MILLI: u64 = 20000;
const RETRY_COUNT: u64 = 30;
// Very long query can take 24 hours
const RETRY_COUNT_WAIT_JOB: u64 = 4320;

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

fn test_if_needs_to_retry<T>(result: &Result<T, TreasureDataError>) -> bool {
    match result {
        &Ok(_) => true,
        &Err(ref err) => match err {
            &TreasureDataError::ApiError(status_code, _) =>
                status_code.to_u16() / 100 == 4,
            _ => false
        }
    }
}

#[no_mangle]
#[allow(unused_must_use)]
pub extern fn issue_query(
    raw_apikey: *const c_char,
    raw_endpoint: *const c_char,
    raw_query_engine: *const c_char,
    raw_database: *const c_char,
    raw_query: *const c_char,
    raw_query_download_dir: *const c_char,
    debug_log: extern fn(usize, &[u8]),
    error_log: extern fn(usize, &[u8])) -> *mut TdQueryState {

    let apikey = convert_str_from_raw_str(raw_apikey);
    let endpoint = convert_str_opt_from_raw_str(raw_endpoint);
    let query_engine = convert_str_from_raw_str(raw_query_engine);
    let database = convert_str_from_raw_str(raw_database);
    let query = convert_str_from_raw_str(raw_query);
    let query_download_dir = convert_str_opt_from_raw_str(raw_query_download_dir);
    let domain_key = Uuid::new_v4();

    log!(debug_log, "issue_query: entering");
    log!(debug_log, "issue_query: apikey.len={:?}", apikey.len());
    log!(debug_log, "issue_query: endpoint={:?}", endpoint);
    log!(debug_log, "issue_query: query_engine={:?}", query_engine);
    log!(debug_log, "issue_query: database={:?}", database);
    log!(debug_log, "issue_query: query={:?}", query);
    log!(debug_log, "issue_query: query_download_dir={:?}", query_download_dir);
    log!(debug_log, "issue_query: domain_key={:?}", domain_key);

    let client = create_client(apikey, &endpoint);
    let query_type = match QueryType::from_str(query_engine) {
        Ok(query_type) => query_type,
        Err(err) => {
            log!(error_log, "issue_query: Invalid query engine. query_engine={:?}. error={:?}", 
                    query_engine, err);
            return null_mut()
        }
    };
    log!(debug_log, "issue_query: query_type={:?}", query_type);
    
    let job_id = match Retry::new(
            &mut || client.issue_job(query_type.clone(), database, query,
                        None, None, None,
                        Some(domain_key.simple().to_string().as_str()), None),
            &mut |result| test_if_needs_to_retry(result)
        ).try(RETRY_COUNT).wait(RETRY_FIXED_INTERVAL_MILLI).execute() {
            Ok(Ok(job_id)) => job_id,
            Ok(Err(err)) => {
                log!(error_log,
                     "issue_query: Failed to issue a query. database={:?}, query={:?}, error={:?}", 
                     database, query, err);
                return null_mut()
            },
            Err(err) => {
                log!(error_log,
                     "issue_query: Failed to issue a query. database={:?}, query={:?}, error={:?}", 
                     database, query, err);
                return null_mut()
            }
        };

    match Retry::new(
            &mut || client.wait_job(job_id, None),
            &mut |result| test_if_needs_to_retry(result)
        ).try(RETRY_COUNT_WAIT_JOB).wait(RETRY_FIXED_INTERVAL_MILLI).execute() {
            Ok(Ok(JobStatus::Success)) => (),
            Ok(Ok(status)) => {
                log!(error_log,
                     "issue_query: Unexpected status. job_id={:?}, status={:?}",
                     job_id, status);
                return null_mut()
            },
            Ok(Err(err)) => {
                log!(error_log,
                     "issue_query: Failed to wait query execution. job_id={:?}, error={:?}",
                     job_id, err);
                return null_mut()
            },
            Err(err) => {
                log!(error_log,
                     "issue_query: Failed to wait query execution. job_id={:?}, error={:?}",
                     job_id, err);
                return null_mut()
            }
        };

    log!(debug_log, "issue_query: job finished. job_id={:?}", job_id);

    let (tx, rx) = mpsc::channel();

    thread::spawn(move || {
        let f = |xs: Vec<Value>| match tx.send(Some(xs)) {
            Ok(()) => true,
            Err(err) => {
                log!(debug_log,
                     "issue_query: Failed to put a result row into the queue. This error can happen when the query is already finished. job_id={:?}, error={:?}",
                     job_id, err);
                // Stop processing result rows
                false
            }
        };

        let read = match query_download_dir {
            None => client.each_row_in_job_result(job_id, &f),
            Some(dl_dir) => {
                let dir = Path::new(dl_dir);
                // Just in case. Ignore the result since an error occurs when already exists
                fs::create_dir(dir);

                let path = dir.join(format!("{}.msgpack.gz", domain_key));
                match File::create(&path) {
                    Ok(mut file) => {
                        match client.download_job_result(job_id, &mut file) {
                            Ok(()) => (),
                            Err(err) => log!(error_log, "Failed to download query result file. path={:?}, error={:?}", path, err)
                        }
                    },
                    Err(err) => log!(error_log, "Failed to create file. path={:?}, error={:?}", path, err)
                }
                match File::open(&path) {
                    Ok(file) => {
                        let read = client.each_row_in_job_result_file(&file, &f);
                        fs::remove_file(path);
                        read
                    },
                    // TODO: This error isn't 100% correct and is something like workaround...
                    Err(err) => Err(TreasureDataError::IoError(err))
                }
            }
        };

        match read {
            Ok(()) => match tx.send(None) {
                Ok(()) => (),
                Err(err) => log!(debug_log,
                                 "issue_query: Failed to put a sentinel into the queue. This error can happen when the query is already finished. job_id={:?}, error={:?}",
                                 job_id, err)
            },
            Err(err) => log!(error_log, "Failed to fetch result. job_id={:?}, error={:?}", job_id, err)
        }
    });

    let query_state = TdQueryState {
        job_id: job_id,
        result_receiver: rx
    };

    let td_query_state = Box::into_raw(Box::new(query_state));

    log!(debug_log, "issue_query: exiting");

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
    error_log: extern fn(usize, &[u8])) -> bool {

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
pub extern fn create_table(
    raw_apikey: *const c_char,
    raw_endpoint: *const c_char,
    raw_database: *const c_char,
    raw_table: *const c_char,
    debug_log: extern fn(usize, &[u8]),
    error_log: extern fn(usize, &[u8])) {

    let apikey = convert_str_from_raw_str(raw_apikey);
    let endpoint = convert_str_opt_from_raw_str(raw_endpoint);
    let database = convert_str_from_raw_str(raw_database);
    let table = convert_str_from_raw_str(raw_table);

    log!(debug_log, "create_table: entering");
    log!(debug_log, "create_table: apikey.len={:?}", apikey.len());
    log!(debug_log, "create_table: endpoint={:?}", endpoint);
    log!(debug_log, "create_table: database={:?}", database);
    log!(debug_log, "create_table: table={:?}", table);

    let client = create_client(apikey, &endpoint);

    match Retry::new(
        &mut || client.create_table(database, table),
        &mut |result| test_if_needs_to_retry(result)
    ).try(RETRY_COUNT).wait(RETRY_FIXED_INTERVAL_MILLI).execute() {
        Ok(Ok(())) => (),
        Ok(Err(TreasureDataError::ApiError(::hyper::status::StatusCode::Conflict, _))) => (),
        Ok(Err(err)) => log!(error_log, "create_table: Failed to create a table. {:?}", err),
        Err(err) => log!(error_log, "create_table: Failed to create a table. {:?}", err)
    }

    log!(debug_log, "create_table: exiting");
}

#[no_mangle]
pub extern fn copy_table_schema(
    raw_apikey: *const c_char,
    raw_endpoint: *const c_char,
    raw_src_database: *const c_char,
    raw_src_table: *const c_char,
    raw_dst_database: *const c_char,
    raw_dst_table: *const c_char,
    debug_log: extern fn(usize, &[u8]),
    error_log: extern fn(usize, &[u8])) {

    let apikey = convert_str_from_raw_str(raw_apikey);
    let endpoint = convert_str_opt_from_raw_str(raw_endpoint);
    let src_database = convert_str_from_raw_str(raw_src_database);
    let src_table = convert_str_from_raw_str(raw_src_table);
    let dst_database = convert_str_from_raw_str(raw_dst_database);
    let dst_table = convert_str_from_raw_str(raw_dst_table);

    log!(debug_log, "copy_table_schema: entering");
    log!(debug_log, "copy_table_schema: apikey.len={:?}", apikey.len());
    log!(debug_log, "copy_table_schema: endpoint={:?}", endpoint);
    log!(debug_log, "copy_table_schema: src_database={:?}", src_database);
    log!(debug_log, "copy_table_schema: src_table={:?}", src_table);
    log!(debug_log, "copy_table_schema: dst_database={:?}", dst_database);
    log!(debug_log, "copy_table_schema: dst_table={:?}", dst_table);

    let client = create_client(apikey, &endpoint);

    match Retry::new(
        &mut || client.copy_table_schema(src_database, src_table, dst_database, dst_table),
        &mut |result| test_if_needs_to_retry(result)
    ).try(RETRY_COUNT).wait(RETRY_FIXED_INTERVAL_MILLI).execute() {
        Ok(Ok(())) => (),
        Ok(Err(err)) => log!(error_log,
                             "copy_table_schema: Failed to copy table schema {:?}", err),
        Err(err) => log!(error_log,
                         "copy_table_schema: Failed to copy table schema {:?}", err)
    }
        
    log!(debug_log, "copy_table_schema: exiting");
}

#[no_mangle]
pub extern fn append_table_schema(
    raw_apikey: *const c_char,
    raw_endpoint: *const c_char,
    raw_database: *const c_char,
    raw_table: *const c_char,
    column_size: usize,
    raw_coltypes: *const *const c_char,
    raw_colnames: *const *const c_char,
    debug_log: extern fn(usize, &[u8]),
    error_log: extern fn(usize, &[u8])
    ) {

    log!(debug_log, "append_table_schema: entering");

    let apikey = convert_str_from_raw_str(raw_apikey);
    let endpoint = convert_str_opt_from_raw_str(raw_endpoint);
    let database = convert_str_from_raw_str(raw_database);
    let table = convert_str_from_raw_str(raw_table);

    let column_types =
        convert_column_types_from_raw_c_char_array(column_size, raw_coltypes);

    let column_names =
        convert_column_names_from_raw_c_char_array(column_size, raw_colnames);

    log!(debug_log, "append_table_schema: apikey.len={:?}", apikey.len());
    log!(debug_log, "append_table_schema: endpoint={:?}", endpoint);
    log!(debug_log, "append_table_schema: database={:?}", database);
    log!(debug_log, "append_table_schema: table={:?}", table);
    log!(debug_log, "append_table_schema: column_size={:?}", column_size);
    log!(debug_log, "append_table_schema: column_types={:?}", column_types);
    log!(debug_log, "append_table_schema: colmun_names={:?}", column_names);

    let mut schema_types = Vec::new();
    for i in 0..column_size {
        let column_name = &column_names[i];
        if column_name == "time" {
            continue;
        }
        let schema_type = match column_types[i] {
            ImportColumnType::Int => SchemaType::Long,
            ImportColumnType::Float => SchemaType::Double,
            ImportColumnType::String => SchemaType::String,
            ImportColumnType::Bytes => SchemaType::String,
            ImportColumnType::Map => {
                log!(error_log, "import_append_table_schema: MAP type isn't supported yet");
                return
            },
            ImportColumnType::Array => {
                log!(error_log, "import_append_table_schema: ARRAY type isn't supported yet");
                return
            }
        };
        schema_types.push((column_name.as_str(), schema_type));
    }

    log!(debug_log, "append_table_schema: schema_types={:?}", schema_types);

    let client = create_client(apikey, &endpoint);

    match Retry::new(
        &mut || client.append_schema(database, table, &schema_types),
        &mut |result| test_if_needs_to_retry(result)
    ).try(RETRY_COUNT).wait(RETRY_FIXED_INTERVAL_MILLI).execute() {
        Ok(Ok(())) => (),
        Ok(Err(err)) => log!(error_log,
                             "append_table_schema: Failed to append table schema {:?}", err),
        Err(err) => log!(error_log,
                         "append_table_schema: Failed to append table schema {:?}", err)
    }
        
    log!(debug_log, "append_table_schema: exiting");
}

#[no_mangle]
pub extern fn delete_table(
    raw_apikey: *const c_char,
    raw_endpoint: *const c_char,
    raw_database: *const c_char,
    raw_table: *const c_char,
    debug_log: extern fn(usize, &[u8]),
    error_log: extern fn(usize, &[u8])) {

    let apikey = convert_str_from_raw_str(raw_apikey);
    let endpoint = convert_str_opt_from_raw_str(raw_endpoint);
    let database = convert_str_from_raw_str(raw_database);
    let table = convert_str_from_raw_str(raw_table);

    log!(debug_log, "delete_table: entering");
    log!(debug_log, "delete_table: apikey.len={:?}", apikey.len());
    log!(debug_log, "delete_table: endpoint={:?}", endpoint);
    log!(debug_log, "delete_table: database={:?}", database);
    log!(debug_log, "delete_table: table={:?}", table);

    let client = create_client(apikey, &endpoint);

    match Retry::new(
        &mut || client.delete_table(database, table),
        &mut |result| test_if_needs_to_retry(result)
    ).try(RETRY_COUNT).wait(RETRY_FIXED_INTERVAL_MILLI).execute() {
        Ok(Ok(())) => (),
        Ok(Err(TreasureDataError::ApiError(::hyper::status::StatusCode::NotFound, _))) => (),
        Ok(Err(err)) => log!(error_log, "delete_table: Failed to delete table {:?}", err),
        Err(err) => log!(error_log, "delete_table: Failed to delete table {:?}", err)
    }
        
    log!(debug_log, "delete_table: exiting");
}

#[no_mangle]
pub extern fn release_query_resource(td_query_state: *mut TdQueryState) {
    unsafe {
        Box::from_raw(td_query_state);
    }
}

fn convert_column_types_from_raw_c_char_array(
    column_size: usize,
    raw_column_types: *const *const c_char) -> Vec<ImportColumnType> {

    let sliced_raw_coltypes = unsafe {
        std::slice::from_raw_parts(raw_column_types, column_size)
    };

    sliced_raw_coltypes
        .iter()
        .map(|x| 
             ImportColumnType::from(
                 unsafe { CStr::from_ptr(*x) }
                 .to_str().unwrap())
            )
        .collect::<Vec<ImportColumnType>>()
}

fn convert_column_names_from_raw_c_char_array(
    column_size: usize,
    raw_column_names: *const *const c_char) -> Vec<String> {

    let sliced_raw_colnames = unsafe {
        std::slice::from_raw_parts(raw_column_names, column_size)
    };

    sliced_raw_colnames
        .iter()
        .map(|x| unsafe { CStr::from_ptr(*x) }.to_str().unwrap().to_string())
        .collect::<Vec<String>>()
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

    log!(debug_log, "import_begin: entering");

    let apikey = convert_str_from_raw_str(raw_apikey);
    let endpoint = convert_str_opt_from_raw_str(raw_endpoint);
    let database = convert_str_from_raw_str(raw_database);
    let table = convert_str_from_raw_str(raw_table);
    let uuid = Uuid::new_v4();

    let column_types =
        convert_column_types_from_raw_c_char_array(column_size, raw_coltypes);

    let column_names =
        convert_column_names_from_raw_c_char_array(column_size, raw_colnames);

    log!(debug_log, "import_begin: apikey.len={:?}", apikey.len());
    log!(debug_log, "import_begin: endpoint={:?}", endpoint);
    log!(debug_log, "import_begin: database={:?}", database);
    log!(debug_log, "import_begin: table={:?}", table);
    log!(debug_log, "import_begin: uuid={:?}", uuid);
    log!(debug_log, "import_begin: column_size={:?}", column_size);
    log!(debug_log, "import_begin: column_types={:?}", column_types);
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
        uuid: uuid,
        approximate_written_size: 0,
        column_size: column_size,
        column_types: column_types,
        column_names: column_names,
        current_time: time::get_time().sec.to_string(),
        writable_chunk: writable_chunk
    };

    let td_import_state = Box::into_raw(Box::new(import_state));

    log!(debug_log, "import_begin: exiting");

    td_import_state
}

#[no_mangle]
#[allow(unused_variables)]
pub extern fn import_append(
    td_import_state: *mut TdImportState,
    raw_values: *const *const c_char,
    debug_log: extern fn(usize, &[u8]),
    error_log: extern fn(usize, &[u8])) -> usize {

    let mut import_state = unsafe { &mut *td_import_state };
    let column_size = import_state.column_size;
    let column_types = &import_state.column_types;
    let column_names = &import_state.column_names;
    let mut writable_chunk = &mut import_state.writable_chunk;
    let current_time = &import_state.current_time;
    writable_chunk.next_row(column_types.len() as u32).unwrap();

    let sliced_raw_values = unsafe {
        std::slice::from_raw_parts(raw_values, column_size)
    };
    let values = sliced_raw_values
        .iter()
        .map(|x| convert_str_opt_from_raw_str(*x))
        .collect::<Vec<Option<&str>>>();

    let approximate_written_size_of_row = values
        .iter()
        .fold(0, |a, x| a + match x { &Some(s) => s.len(), &None => 0 });

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
                if colname == "time" {
                    let ct = current_time.clone();
                    let time = ct.parse::<i64>().unwrap();
                    writable_chunk.write_key_and_i64(colname.as_str(), time).unwrap();
                }
                else {
                    writable_chunk.write_key_and_nil(colname.as_str()).unwrap();
                }
            }
        }
        i += 1;
    }
    import_state.approximate_written_size += approximate_written_size_of_row;

    import_state.approximate_written_size
}

#[no_mangle]
pub extern fn import_commit(
    td_import_state: *mut TdImportState,
    debug_log: extern fn(usize, &[u8]),
    error_log: extern fn(usize, &[u8])) {

    log!(debug_log, "import_commit: entering");

    let import_state = unsafe { *Box::from_raw(td_import_state) };
    let database = import_state.database.as_str();
    let table = import_state.table.as_str();
    let uuid = &import_state.uuid;
    let client = &import_state.td_client;
    match import_state.writable_chunk.close() {
        Ok(readable_chunk) => {
            match Retry::new(
                &mut || client.import_msgpack_gz_file_to_table(
                            database, table,
                            &readable_chunk.file_path.as_str(),
                            Some(uuid.simple().to_string().as_str())),
                &mut |result| test_if_needs_to_retry(result)
            ).try(RETRY_COUNT).wait(RETRY_FIXED_INTERVAL_MILLI).execute() {
                Ok(Ok(())) => (),
                Ok(Err(err)) =>
                    log!(error_log,
                         "import_commit: Failed to import readable chunk: {:?}", err),
                Err(err) => 
                    log!(error_log,
                         "import_commit: Failed to import readable chunk: {:?}", err)
            };
        },
        Err(err) => {
            log!(error_log,
                 "import_commit: Failed to close writable chunk: {:?}", err);
        },
    };

    log!(debug_log, "import_commit: exiting");
}

#[no_mangle]
pub extern fn get_table_schemas(
    raw_apikey: *const c_char,
    raw_endpoint: *const c_char,
    raw_database: *const c_char,
    pgstrdup: extern fn(usize, &[u8]) -> *mut c_void,
    pgalloc: extern fn(usize) -> *mut c_void,
    debug_log: extern fn(usize, &[u8]),
    error_log: extern fn(usize, &[u8]) -> !) -> *const PgTableSchemas {

    let apikey = convert_str_from_raw_str(raw_apikey);
    let endpoint = convert_str_opt_from_raw_str(raw_endpoint);
    let database = convert_str_from_raw_str(raw_database);

    log!(debug_log, "get_tables: entering");
    log!(debug_log, "get_tables: apikey.len={:?}", apikey.len());
    log!(debug_log, "get_tables: endpoint={:?}", endpoint);
    log!(debug_log, "get_tables: database={:?}", database);

    let client = create_client(apikey, &endpoint);
    let td_tables = match Retry::new(
        &mut || client.tables(database),
        &mut |result| test_if_needs_to_retry(result)
    ).try(RETRY_COUNT).wait(RETRY_FIXED_INTERVAL_MILLI).execute() {
        Ok(Ok(tables)) => tables,
        Ok(Err(err)) => {
            log!(error_log, "get_tables: Failed to get tables. {:?}", err);
        },
        Err(err) => {
            log!(error_log, "get_tables: Failed to get tables. {:?}", err);
        }
    };

    fn get_angle_bracket_indices(td_type: &str) -> Result<Option<(usize, usize)>, String> {
        match td_type.find('<') {
            Some(left_index) =>
                if td_type.ends_with('>') {
                    Ok(Some((left_index, td_type.len() - 1)))
                }
                else {
                    Err(String::from(format!("Unexpected TypeName: {:?}", td_type)))
                },
            None => Ok(None)
        }
    }

    fn parse_td_type(td_type: &str) -> Result<String, String> {
        match get_angle_bracket_indices(td_type) {
            Ok(angle_bracket_indices) => match angle_bracket_indices {
                Some((left_index, right_index)) => {
                    let container_type = &td_type[0..left_index];
                    let inner_type = &td_type[(left_index + 1)..right_index];
                    match container_type {
                        "array" => match parse_td_type(inner_type) {
                            Ok(inner_type) => Ok(format!("{}[]", inner_type)),
                            _ => Err(String::from(format!("Unexpected TypeName: {:?}", td_type)))
                        },
                        _ => Err(String::from(format!("Unexpected TypeName: {:?}", td_type)))
                    }
                },
                None => match td_type {
                    "int" => Ok(String::from("int")),
                    "long" => Ok(String::from("bigint")),
                    "float" => Ok(String::from("real")),
                    "double" => Ok(String::from("double precision")),
                    "string" => Ok(String::from("text")),
                    _ => Err(String::from(format!("Unexpected TypeName: {:?}", td_type)))
                }
            },
            Err(msg) => Err(msg)
        }
    }

    // alloc for PgTableSchemas
    let tbl_schemas_ptr = pgalloc(mem::size_of::<PgTableSchemas>())
        as *mut PgTableSchemas;
    let tbl_schemas = unsafe { &mut *tbl_schemas_ptr };

    let numtables = td_tables.len();
    log!(debug_log, "get_tables: num_tables={}", numtables);
    tbl_schemas.numtables = numtables as size_t;

    // alloc for PgTableSchemas.tables
    let tbls_ptr_size = mem::size_of::<PgTableSchema>() * numtables;
    log!(debug_log, "get_tables: allocated memory for tables: {}", tbls_ptr_size);
    tbl_schemas.tables = pgalloc(tbls_ptr_size) as *const *const PgTableSchema;
    let tables = unsafe {
        slice::from_raw_parts_mut(tbl_schemas.tables as *mut *mut PgTableSchema,
                                  numtables)
    };

    for (i, tab) in td_tables.iter().enumerate() {
        log!(debug_log, "get_tables: table_name={}", tab.name);
        // alloc for a PgTableSchema
        tables[i] = pgalloc(mem::size_of::<PgTableSchema>()) as *mut PgTableSchema;
        let table = unsafe { &mut *tables[i] };

        let pg_table_name_byte = tab.name.as_bytes();
        table.name = pgstrdup(pg_table_name_byte.len(), pg_table_name_byte);

        // I think td_client should decode schema string but now decoding here.
        let td_columns: Vec<Vec<String>> = match serde_json::from_str(&tab.schema) {
            Ok(columns) => columns,
            Err(err) => log!(error_log, "failed to parse table columns: {:?}", err)
        };
        let numcolumns = td_columns.len();
        log!(debug_log, "get_tables: num_colums={}", numcolumns);
        table.numcols = numcolumns as size_t;

        // alloc for PgTableSchema.colnames
        let cols_ptr_size = mem::size_of::<*const c_void>() * numcolumns;
        log!(debug_log, "get_tables: allocating memory for column names: {}",
             cols_ptr_size);
        table.colnames = pgalloc(cols_ptr_size) as *const *const c_void;
        let colnames = unsafe {
            slice::from_raw_parts_mut(table.colnames as *mut *mut c_void,
                                      numcolumns)
        };

        // alloc for PgTableSchema.coltypes
        log!(debug_log, "get_tables: allocating memory for column types: {}",
             cols_ptr_size);
        table.coltypes = pgalloc(cols_ptr_size) as *const *const c_void;
        let coltypes = unsafe {
            slice::from_raw_parts_mut(table.coltypes as *mut *mut c_void,
                                      numcolumns)
        };

        // convert data type from td to postgres
        for (j, col) in td_columns.into_iter().enumerate() {
            if col.len() < 2 {
                log!(error_log, "invalid column definition format: {:?}", col)
            }
            let pg_col_name_byte = col[0].as_bytes();
            let td_type_name = col[1].as_str();
            let pg_type_name = match parse_td_type(td_type_name) {
                Ok(name) => name,
                Err(msg) => log!(error_log, msg)
            };
            let pg_type_name_byte = pg_type_name.as_bytes();
            colnames[j] = pgstrdup(pg_col_name_byte.len(), pg_col_name_byte);
            coltypes[j] = pgstrdup(pg_type_name_byte.len(), pg_type_name_byte);
        }
    }
    tbl_schemas_ptr as *const PgTableSchemas
}
