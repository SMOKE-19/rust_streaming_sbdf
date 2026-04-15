use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray,
    RecordBatch, StringArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow_schema::{DataType, TimeUnit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{
    PyAny, PyBool, PyBytes, PyDate, PyDateAccess, PyDateTime, PyDelta, PyDeltaAccess, PyDict,
    PyFloat, PyInt, PyString, PyTime, PyTimeAccess,
};
use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_void, CString};
use std::fs::File;
use std::ptr::{null, null_mut};

const SBDF_OK: c_int = 0;
const SBDF_BOOLTYPEID: c_int = 1;
const SBDF_INTTYPEID: c_int = 2;
const SBDF_LONGTYPEID: c_int = 3;
const SBDF_FLOATTYPEID: c_int = 4;
const SBDF_DOUBLETYPEID: c_int = 5;
const SBDF_DATETIMETYPEID: c_int = 6;
const SBDF_DATETYPEID: c_int = 7;
const SBDF_TIMETYPEID: c_int = 8;
const SBDF_TIMESPANTYPEID: c_int = 9;
const SBDF_STRINGTYPEID: c_int = 10;
const SBDF_BINARYTYPEID: c_int = 11;

const SBDF_PLAINARRAYENCODINGTYPEID: c_int = 1;
const SBDF_RUNLENGTHENCODINGTYPEID: c_int = 2;
const SBDF_BITARRAYENCODINGTYPEID: c_int = 3;
const UNIX_EPOCH_DAYS_FROM_YEAR_ONE: i64 = 719_162;
const MILLIS_PER_DAY: i64 = 86_400_000;
const UNIX_EPOCH_MILLIS_FROM_YEAR_ONE: i64 = UNIX_EPOCH_DAYS_FROM_YEAR_ONE * MILLIS_PER_DAY;

#[repr(C)]
struct sbdf_valuetype {
    id: c_int,
}

#[repr(C)]
struct sbdf_object {
    type_: sbdf_valuetype,
    count: c_int,
    data: *mut c_void,
}

#[repr(C)]
struct sbdf_metadata_head {
    _private: [u8; 0],
}

#[repr(C)]
struct sbdf_tablemetadata {
    _private: [u8; 0],
}

#[repr(C)]
struct sbdf_valuearray {
    _private: [u8; 0],
}

#[repr(C)]
struct sbdf_columnslice {
    _private: [u8; 0],
}

#[repr(C)]
struct sbdf_tableslice {
    _private: [u8; 0],
}

unsafe extern "C" {
    fn fopen(filename: *const c_char, mode: *const c_char) -> *mut c_void;
    fn fclose(file: *mut c_void) -> c_int;

    fn sbdf_vt_bool() -> sbdf_valuetype;
    fn sbdf_vt_int() -> sbdf_valuetype;
    fn sbdf_vt_long() -> sbdf_valuetype;
    fn sbdf_vt_float() -> sbdf_valuetype;
    fn sbdf_vt_double() -> sbdf_valuetype;
    fn sbdf_vt_datetime() -> sbdf_valuetype;
    fn sbdf_vt_date() -> sbdf_valuetype;
    fn sbdf_vt_time() -> sbdf_valuetype;
    fn sbdf_vt_timespan() -> sbdf_valuetype;
    fn sbdf_vt_string() -> sbdf_valuetype;
    fn sbdf_vt_binary() -> sbdf_valuetype;

    fn sbdf_obj_create_arr(
        type_: sbdf_valuetype,
        count: c_int,
        data: *const c_void,
        lengths: *const c_int,
        out: *mut *mut sbdf_object,
    ) -> c_int;
    fn sbdf_obj_destroy(obj: *mut sbdf_object);

    fn sbdf_md_create(out: *mut *mut sbdf_metadata_head) -> c_int;
    fn sbdf_md_destroy(head: *mut sbdf_metadata_head);
    fn sbdf_tm_create(head: *mut sbdf_metadata_head, out: *mut *mut sbdf_tablemetadata) -> c_int;
    fn sbdf_tm_destroy(meta: *mut sbdf_tablemetadata);
    fn sbdf_tm_add(col_md: *mut sbdf_metadata_head, table: *mut sbdf_tablemetadata) -> c_int;
    fn sbdf_cm_set_values(name: *const c_char, vt: sbdf_valuetype, md: *mut sbdf_metadata_head) -> c_int;

    fn sbdf_fh_write_cur(file: *mut c_void) -> c_int;
    fn sbdf_tm_write(file: *mut c_void, meta: *const sbdf_tablemetadata) -> c_int;
    fn sbdf_ts_create(meta: *mut sbdf_tablemetadata, out: *mut *mut sbdf_tableslice) -> c_int;
    fn sbdf_ts_add(col: *mut sbdf_columnslice, table: *mut sbdf_tableslice) -> c_int;
    fn sbdf_ts_destroy(slice: *mut sbdf_tableslice);
    fn sbdf_ts_write(file: *mut c_void, slice: *const sbdf_tableslice) -> c_int;
    fn sbdf_ts_write_end(file: *mut c_void) -> c_int;

    fn sbdf_va_create(encoding: c_int, obj: *const sbdf_object, out: *mut *mut sbdf_valuearray) -> c_int;
    fn sbdf_va_create_dflt(obj: *const sbdf_object, out: *mut *mut sbdf_valuearray) -> c_int;
    fn sbdf_va_destroy(arr: *mut sbdf_valuearray);

    fn sbdf_cs_create(out: *mut *mut sbdf_columnslice, values: *mut sbdf_valuearray) -> c_int;
    fn sbdf_cs_add_property(out: *mut sbdf_columnslice, name: *const c_char, values: *mut sbdf_valuearray) -> c_int;
    fn sbdf_cs_destroy_all(col: *mut sbdf_columnslice);
}

const SBDF_ISINVALID_VALUEPROPERTY: &[u8] = b"IsInvalid\0";

#[pyclass]
struct SBDFError {}

#[derive(Clone, Copy)]
enum ValueType {
    Bool,
    Int,
    Long,
    Float,
    Double,
    DateTime,
    Date,
    Time,
    TimeSpan,
    String,
    Binary,
}

impl ValueType {
    fn from_name(name: &str) -> PyResult<Self> {
        match name {
            "Boolean" => Ok(Self::Bool),
            "Integer" => Ok(Self::Int),
            "LongInteger" => Ok(Self::Long),
            "SingleReal" => Ok(Self::Float),
            "Real" => Ok(Self::Double),
            "DateTime" => Ok(Self::DateTime),
            "Date" => Ok(Self::Date),
            "Time" => Ok(Self::Time),
            "TimeSpan" => Ok(Self::TimeSpan),
            "String" => Ok(Self::String),
            "Binary" => Ok(Self::Binary),
            other => Err(PyValueError::new_err(format!("unknown Spotfire type: {other}"))),
        }
    }

    fn infer(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        if value.is_instance_of::<PyBool>() {
            return Ok(Self::Bool);
        }
        if value.is_instance_of::<PyInt>() {
            return Ok(Self::Long);
        }
        if value.is_instance_of::<PyFloat>() {
            return Ok(Self::Double);
        }
        if value.is_instance_of::<PyDateTime>() {
            return Ok(Self::DateTime);
        }
        if value.is_instance_of::<PyTime>() {
            return Ok(Self::Time);
        }
        if value.is_instance_of::<PyDelta>() {
            return Ok(Self::TimeSpan);
        }
        if value.is_instance_of::<PyDate>() {
            return Ok(Self::Date);
        }
        if value.is_instance_of::<PyString>() {
            return Ok(Self::String);
        }
        if value.is_instance_of::<PyBytes>() {
            return Ok(Self::Binary);
        }
        Err(PyTypeError::new_err(format!(
            "unsupported value type for SBDF export: {}",
            value.get_type().name()?
        )))
    }

    fn type_id(self) -> c_int {
        match self {
            Self::Bool => SBDF_BOOLTYPEID,
            Self::Int => SBDF_INTTYPEID,
            Self::Long => SBDF_LONGTYPEID,
            Self::Float => SBDF_FLOATTYPEID,
            Self::Double => SBDF_DOUBLETYPEID,
            Self::DateTime => SBDF_DATETIMETYPEID,
            Self::Date => SBDF_DATETYPEID,
            Self::Time => SBDF_TIMETYPEID,
            Self::TimeSpan => SBDF_TIMESPANTYPEID,
            Self::String => SBDF_STRINGTYPEID,
            Self::Binary => SBDF_BINARYTYPEID,
        }
    }

    fn sbdf_type(self) -> sbdf_valuetype {
        unsafe {
            match self {
                Self::Bool => sbdf_vt_bool(),
                Self::Int => sbdf_vt_int(),
                Self::Long => sbdf_vt_long(),
                Self::Float => sbdf_vt_float(),
                Self::Double => sbdf_vt_double(),
                Self::DateTime => sbdf_vt_datetime(),
                Self::Date => sbdf_vt_date(),
                Self::Time => sbdf_vt_time(),
                Self::TimeSpan => sbdf_vt_timespan(),
                Self::String => sbdf_vt_string(),
                Self::Binary => sbdf_vt_binary(),
            }
        }
    }
}

enum ColumnBuffer {
    Bool(Vec<u8>),
    Int(Vec<i32>),
    Long(Vec<i64>),
    Float(Vec<f32>),
    Double(Vec<f64>),
    TimeLike(Vec<i64>),
    String {
        values: Vec<Vec<u8>>,
        ptrs: Vec<*const c_char>,
        lengths: Vec<c_int>,
    },
    Binary {
        values: Vec<Vec<u8>>,
        ptrs: Vec<*const c_char>,
        lengths: Vec<c_int>,
    },
}

impl ColumnBuffer {
    fn create_object(&self, value_type: ValueType) -> PyResult<*mut sbdf_object> {
        let mut obj = null_mut();
        let error = unsafe {
            match self {
                Self::Bool(values) => sbdf_obj_create_arr(
                    value_type.sbdf_type(),
                    values.len() as c_int,
                    values.as_ptr().cast(),
                    null(),
                    &mut obj,
                ),
                Self::Int(values) => sbdf_obj_create_arr(
                    value_type.sbdf_type(),
                    values.len() as c_int,
                    values.as_ptr().cast(),
                    null(),
                    &mut obj,
                ),
                Self::Long(values) => sbdf_obj_create_arr(
                    value_type.sbdf_type(),
                    values.len() as c_int,
                    values.as_ptr().cast(),
                    null(),
                    &mut obj,
                ),
                Self::Float(values) => sbdf_obj_create_arr(
                    value_type.sbdf_type(),
                    values.len() as c_int,
                    values.as_ptr().cast(),
                    null(),
                    &mut obj,
                ),
                Self::Double(values) => sbdf_obj_create_arr(
                    value_type.sbdf_type(),
                    values.len() as c_int,
                    values.as_ptr().cast(),
                    null(),
                    &mut obj,
                ),
                Self::TimeLike(values) => sbdf_obj_create_arr(
                    value_type.sbdf_type(),
                    values.len() as c_int,
                    values.as_ptr().cast(),
                    null(),
                    &mut obj,
                ),
                Self::String { ptrs, lengths, .. } | Self::Binary { ptrs, lengths, .. } => sbdf_obj_create_arr(
                    value_type.sbdf_type(),
                    ptrs.len() as c_int,
                    ptrs.as_ptr().cast(),
                    lengths.as_ptr(),
                    &mut obj,
                ),
            }
        };
        if error != SBDF_OK {
            return Err(PyRuntimeError::new_err("failed to create SBDF object"));
        }
        Ok(obj)
    }
}

enum NativeColumnBuffer {
    Bool(Vec<u8>),
    Int(Vec<i32>),
    Long(Vec<i64>),
    Float(Vec<f32>),
    Double(Vec<f64>),
    TimeLike(Vec<i64>),
    String {
        values: Vec<Vec<u8>>,
        ptrs: Vec<*const c_char>,
        lengths: Vec<c_int>,
    },
    Binary {
        values: Vec<Vec<u8>>,
        ptrs: Vec<*const c_char>,
        lengths: Vec<c_int>,
    },
}

impl NativeColumnBuffer {
    fn create_object(&self, value_type: ValueType) -> PyResult<*mut sbdf_object> {
        let mut obj = null_mut();
        let error = unsafe {
            match self {
                Self::Bool(values) => sbdf_obj_create_arr(
                    value_type.sbdf_type(),
                    values.len() as c_int,
                    values.as_ptr().cast(),
                    null(),
                    &mut obj,
                ),
                Self::Int(values) => sbdf_obj_create_arr(
                    value_type.sbdf_type(),
                    values.len() as c_int,
                    values.as_ptr().cast(),
                    null(),
                    &mut obj,
                ),
                Self::Long(values) => sbdf_obj_create_arr(
                    value_type.sbdf_type(),
                    values.len() as c_int,
                    values.as_ptr().cast(),
                    null(),
                    &mut obj,
                ),
                Self::Float(values) => sbdf_obj_create_arr(
                    value_type.sbdf_type(),
                    values.len() as c_int,
                    values.as_ptr().cast(),
                    null(),
                    &mut obj,
                ),
                Self::Double(values) => sbdf_obj_create_arr(
                    value_type.sbdf_type(),
                    values.len() as c_int,
                    values.as_ptr().cast(),
                    null(),
                    &mut obj,
                ),
                Self::TimeLike(values) => sbdf_obj_create_arr(
                    value_type.sbdf_type(),
                    values.len() as c_int,
                    values.as_ptr().cast(),
                    null(),
                    &mut obj,
                ),
                Self::String { ptrs, lengths, .. } | Self::Binary { ptrs, lengths, .. } => sbdf_obj_create_arr(
                    value_type.sbdf_type(),
                    ptrs.len() as c_int,
                    ptrs.as_ptr().cast(),
                    lengths.as_ptr(),
                    &mut obj,
                ),
            }
        };
        if error != SBDF_OK {
            return Err(PyRuntimeError::new_err("failed to create SBDF object"));
        }
        Ok(obj)
    }
}

fn is_missing(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    if value.is_none() {
        return Ok(true);
    }
    if let Ok(v) = value.extract::<f64>() {
        return Ok(v.is_nan());
    }
    Ok(false)
}

fn days_since_epoch(year: i32, month: u8, day: u8) -> i64 {
    let adjust = if month <= 2 { 1 } else { 0 };
    let y = year - adjust;
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let m = month as i32;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + day as i32 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    (era * 146097 + doe - 306) as i64
}

fn millis_from_datetime(value: &Bound<'_, PyDateTime>) -> i64 {
    let days = days_since_epoch(value.get_year(), value.get_month(), value.get_day());
    let day_ms = (value.get_hour() as i64 * 3600
        + value.get_minute() as i64 * 60
        + value.get_second() as i64)
        * 1000
        + (value.get_microsecond() as i64 / 1000);
    days * MILLIS_PER_DAY + day_ms
}

fn millis_from_date(value: &Bound<'_, PyDate>) -> i64 {
    days_since_epoch(value.get_year(), value.get_month(), value.get_day()) * MILLIS_PER_DAY
}

fn millis_from_time(value: &Bound<'_, PyTime>) -> i64 {
    (value.get_hour() as i64 * 3600 + value.get_minute() as i64 * 60 + value.get_second() as i64) * 1000
        + (value.get_microsecond() as i64 / 1000)
}

fn millis_from_timedelta(value: &Bound<'_, PyDelta>) -> i64 {
    value.get_days() as i64 * MILLIS_PER_DAY
        + value.get_seconds() as i64 * 1000
        + value.get_microseconds() as i64 / 1000
}

fn sbdf_millis_from_unix_days(days_since_unix_epoch: i64) -> i64 {
    (days_since_unix_epoch + UNIX_EPOCH_DAYS_FROM_YEAR_ONE) * MILLIS_PER_DAY
}

fn sbdf_millis_from_unix_millis(millis_since_unix_epoch: i64) -> i64 {
    millis_since_unix_epoch + UNIX_EPOCH_MILLIS_FROM_YEAR_ONE
}

fn build_column_buffer(value_type: ValueType, values: &[Bound<'_, PyAny>], invalids: &[u8]) -> PyResult<ColumnBuffer> {
    match value_type {
        ValueType::Bool => Ok(ColumnBuffer::Bool(
            values
                .iter()
                .zip(invalids.iter())
                .map(|(value, invalid)| if *invalid == 1 { 0 } else { u8::from(value.extract::<bool>().unwrap_or(false)) })
                .collect(),
        )),
        ValueType::Int => Ok(ColumnBuffer::Int(
            values
                .iter()
                .zip(invalids.iter())
                .map(|(value, invalid)| if *invalid == 1 { 0 } else { value.extract::<i32>().unwrap_or(0) })
                .collect(),
        )),
        ValueType::Long => Ok(ColumnBuffer::Long(
            values
                .iter()
                .zip(invalids.iter())
                .map(|(value, invalid)| if *invalid == 1 { 0 } else { value.extract::<i64>().unwrap_or(0) })
                .collect(),
        )),
        ValueType::Float => Ok(ColumnBuffer::Float(
            values
                .iter()
                .zip(invalids.iter())
                .map(|(value, invalid)| if *invalid == 1 { 0.0 } else { value.extract::<f32>().unwrap_or(0.0) })
                .collect(),
        )),
        ValueType::Double => Ok(ColumnBuffer::Double(
            values
                .iter()
                .zip(invalids.iter())
                .map(|(value, invalid)| if *invalid == 1 { 0.0 } else { value.extract::<f64>().unwrap_or(0.0) })
                .collect(),
        )),
        ValueType::DateTime => Ok(ColumnBuffer::TimeLike(
            values
                .iter()
                .zip(invalids.iter())
                .map(|(value, invalid)| {
                    if *invalid == 1 {
                        0
                    } else {
                        millis_from_datetime(value.cast::<PyDateTime>().unwrap())
                    }
                })
                .collect(),
        )),
        ValueType::Date => Ok(ColumnBuffer::TimeLike(
            values
                .iter()
                .zip(invalids.iter())
                .map(|(value, invalid)| if *invalid == 1 { 0 } else { millis_from_date(value.cast::<PyDate>().unwrap()) })
                .collect(),
        )),
        ValueType::Time => Ok(ColumnBuffer::TimeLike(
            values
                .iter()
                .zip(invalids.iter())
                .map(|(value, invalid)| if *invalid == 1 { 0 } else { millis_from_time(value.cast::<PyTime>().unwrap()) })
                .collect(),
        )),
        ValueType::TimeSpan => Ok(ColumnBuffer::TimeLike(
            values
                .iter()
                .zip(invalids.iter())
                .map(|(value, invalid)| {
                    if *invalid == 1 {
                        0
                    } else {
                        millis_from_timedelta(value.cast::<PyDelta>().unwrap())
                    }
                })
                .collect(),
        )),
        ValueType::String => {
            let values_buf: Vec<Vec<u8>> = values
                .iter()
                .zip(invalids.iter())
                .map(|(value, invalid)| {
                    if *invalid == 1 {
                        Vec::new()
                    } else {
                        value.cast::<PyString>().unwrap().to_str().unwrap().as_bytes().to_vec()
                    }
                })
                .collect();
            let ptrs = values_buf.iter().map(|value| value.as_ptr().cast::<c_char>()).collect();
            let lengths = values_buf.iter().map(|value| value.len() as c_int).collect();
            Ok(ColumnBuffer::String {
                values: values_buf,
                ptrs,
                lengths,
            })
        }
        ValueType::Binary => {
            let values_buf: Vec<Vec<u8>> = values
                .iter()
                .zip(invalids.iter())
                .map(|(value, invalid)| {
                    if *invalid == 1 {
                        Vec::new()
                    } else {
                        value.cast::<PyBytes>().unwrap().as_bytes().to_vec()
                    }
                })
                .collect();
            let ptrs = values_buf.iter().map(|value| value.as_ptr().cast::<c_char>()).collect();
            let lengths = values_buf.iter().map(|value| value.len() as c_int).collect();
            Ok(ColumnBuffer::Binary {
                values: values_buf,
                ptrs,
                lengths,
            })
        }
    }
}

fn value_encoding(value_type: ValueType, encoding_rle: bool) -> c_int {
    match value_type {
        ValueType::Bool => SBDF_BITARRAYENCODINGTYPEID,
        ValueType::Binary => SBDF_PLAINARRAYENCODINGTYPEID,
        _ if encoding_rle => SBDF_RUNLENGTHENCODINGTYPEID,
        _ => SBDF_PLAINARRAYENCODINGTYPEID,
    }
}

fn ensure_supported_arrow_type(column_name: &str, data_type: &DataType) -> PyResult<ValueType> {
    match data_type {
        DataType::Boolean => Ok(ValueType::Bool),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::UInt8 | DataType::UInt16 => {
            Ok(ValueType::Int)
        }
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => Ok(ValueType::Long),
        DataType::Float32 => Ok(ValueType::Float),
        DataType::Float64 | DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            Ok(ValueType::Double)
        }
        DataType::Timestamp(_, _) => Ok(ValueType::DateTime),
        DataType::Date32 | DataType::Date64 => Ok(ValueType::Date),
        DataType::Time32(_) | DataType::Time64(_) => Ok(ValueType::Time),
        DataType::Duration(_) | DataType::Interval(_) => Ok(ValueType::TimeSpan),
        DataType::Utf8 | DataType::LargeUtf8 => Ok(ValueType::String),
        DataType::Binary | DataType::LargeBinary => Ok(ValueType::Binary),
        DataType::Null => Err(PyValueError::new_err(format!(
            "column '{column_name}' has Arrow type Null; provide column_types or cast upstream"
        ))),
        other if other.is_nested() => Err(PyValueError::new_err(format!(
            "nested Parquet types are not supported for SBDF export. column '{column_name}' has Arrow type '{other:?}'"
        ))),
        other => Err(PyValueError::new_err(format!(
            "automatic Spotfire type mapping is not available for column '{column_name}' with Arrow type '{other:?}'"
        ))),
    }
}

fn infer_arrow_schema(schema: &arrow_schema::Schema) -> PyResult<(Vec<String>, HashMap<String, ValueType>)> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    let column_types = schema
        .fields()
        .iter()
        .map(|field| ensure_supported_arrow_type(field.name(), field.data_type()).map(|vt| (field.name().clone(), vt)))
        .collect::<PyResult<HashMap<_, _>>>()?;
    Ok((columns, column_types))
}

fn get_column_types(
    columns: &[String],
    provided: Option<HashMap<String, String>>,
    schema: &arrow_schema::Schema,
) -> PyResult<HashMap<String, ValueType>> {
    if let Some(column_types) = provided {
        return columns
            .iter()
            .map(|column| {
                column_types
                    .get(column)
                    .ok_or_else(|| PyValueError::new_err(format!("missing type for column '{column}'")))
                    .and_then(|value| ValueType::from_name(value).map(|typed| (column.clone(), typed)))
            })
            .collect();
    }
    infer_arrow_schema(schema).map(|(_, inferred)| inferred)
}

fn timestamp_array_to_millis(array: &dyn Array, unit: TimeUnit) -> PyResult<Vec<i64>> {
    match unit {
        TimeUnit::Second => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .ok_or_else(|| PyTypeError::new_err("failed to read timestamp(second) column"))?;
            Ok((0..typed.len()).map(|i| if typed.is_null(i) { 0 } else { typed.value(i) * 1_000 }).collect())
        }
        TimeUnit::Millisecond => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .ok_or_else(|| PyTypeError::new_err("failed to read timestamp(millisecond) column"))?;
            Ok((0..typed.len()).map(|i| if typed.is_null(i) { 0 } else { typed.value(i) }).collect())
        }
        TimeUnit::Microsecond => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| PyTypeError::new_err("failed to read timestamp(microsecond) column"))?;
            Ok((0..typed.len()).map(|i| if typed.is_null(i) { 0 } else { typed.value(i) / 1_000 }).collect())
        }
        TimeUnit::Nanosecond => {
            let typed = array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| PyTypeError::new_err("failed to read timestamp(nanosecond) column"))?;
            Ok((0..typed.len()).map(|i| if typed.is_null(i) { 0 } else { typed.value(i) / 1_000_000 }).collect())
        }
    }
}

fn time_array_to_millis(array: &dyn Array, unit: TimeUnit) -> PyResult<Vec<i64>> {
    match unit {
        TimeUnit::Second => {
            let typed = array
                .as_any()
                .downcast_ref::<Time32SecondArray>()
                .ok_or_else(|| PyTypeError::new_err("failed to read time(second) column"))?;
            Ok((0..typed.len()).map(|i| if typed.is_null(i) { 0 } else { typed.value(i) as i64 * 1_000 }).collect())
        }
        TimeUnit::Millisecond => {
            let typed = array
                .as_any()
                .downcast_ref::<Time32MillisecondArray>()
                .ok_or_else(|| PyTypeError::new_err("failed to read time(millisecond) column"))?;
            Ok((0..typed.len()).map(|i| if typed.is_null(i) { 0 } else { typed.value(i) as i64 }).collect())
        }
        TimeUnit::Microsecond => {
            let typed = array
                .as_any()
                .downcast_ref::<Time64MicrosecondArray>()
                .ok_or_else(|| PyTypeError::new_err("failed to read time(microsecond) column"))?;
            Ok((0..typed.len()).map(|i| if typed.is_null(i) { 0 } else { typed.value(i) / 1_000 }).collect())
        }
        TimeUnit::Nanosecond => {
            let typed = array
                .as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .ok_or_else(|| PyTypeError::new_err("failed to read time(nanosecond) column"))?;
            Ok((0..typed.len()).map(|i| if typed.is_null(i) { 0 } else { typed.value(i) / 1_000_000 }).collect())
        }
    }
}

fn build_native_column_buffer(column_name: &str, array: &dyn Array) -> PyResult<(NativeColumnBuffer, Vec<u8>)> {
    let invalids = (0..array.len())
        .map(|index| u8::from(array.is_null(index)))
        .collect::<Vec<_>>();

    let buffer = match array.data_type() {
        DataType::Boolean => {
            let typed = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| PyTypeError::new_err(format!("failed to read boolean column '{column_name}'")))?;
            NativeColumnBuffer::Bool(
                (0..typed.len())
                    .map(|i| if typed.is_null(i) { 0 } else { u8::from(typed.value(i)) })
                    .collect(),
            )
        }
        DataType::Int8 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| PyTypeError::new_err(format!("failed to read int8 column '{column_name}'")))?;
            NativeColumnBuffer::Int(
                (0..typed.len())
                    .map(|i| if typed.is_null(i) { 0 } else { typed.value(i) as i32 })
                    .collect(),
            )
        }
        DataType::Int16 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| PyTypeError::new_err(format!("failed to read int16 column '{column_name}'")))?;
            NativeColumnBuffer::Int(
                (0..typed.len())
                    .map(|i| if typed.is_null(i) { 0 } else { typed.value(i) as i32 })
                    .collect(),
            )
        }
        DataType::Int32 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| PyTypeError::new_err(format!("failed to read int32 column '{column_name}'")))?;
            NativeColumnBuffer::Int(
                (0..typed.len())
                    .map(|i| if typed.is_null(i) { 0 } else { typed.value(i) })
                    .collect(),
            )
        }
        DataType::UInt8 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| PyTypeError::new_err(format!("failed to read uint8 column '{column_name}'")))?;
            NativeColumnBuffer::Int(
                (0..typed.len())
                    .map(|i| if typed.is_null(i) { 0 } else { typed.value(i) as i32 })
                    .collect(),
            )
        }
        DataType::UInt16 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| PyTypeError::new_err(format!("failed to read uint16 column '{column_name}'")))?;
            NativeColumnBuffer::Int(
                (0..typed.len())
                    .map(|i| if typed.is_null(i) { 0 } else { typed.value(i) as i32 })
                    .collect(),
            )
        }
        DataType::Int64 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| PyTypeError::new_err(format!("failed to read int64 column '{column_name}'")))?;
            NativeColumnBuffer::Long(
                (0..typed.len())
                    .map(|i| if typed.is_null(i) { 0 } else { typed.value(i) })
                    .collect(),
            )
        }
        DataType::UInt32 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| PyTypeError::new_err(format!("failed to read uint32 column '{column_name}'")))?;
            NativeColumnBuffer::Long(
                (0..typed.len())
                    .map(|i| if typed.is_null(i) { 0 } else { typed.value(i) as i64 })
                    .collect(),
            )
        }
        DataType::UInt64 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| PyTypeError::new_err(format!("failed to read uint64 column '{column_name}'")))?;
            NativeColumnBuffer::Long(
                (0..typed.len())
                    .map(|i| if typed.is_null(i) { 0 } else { typed.value(i) as i64 })
                    .collect(),
            )
        }
        DataType::Float32 => {
            let typed = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| PyTypeError::new_err(format!("failed to read float32 column '{column_name}'")))?;
            NativeColumnBuffer::Float(
                (0..typed.len())
                    .map(|i| if typed.is_null(i) { 0.0 } else { typed.value(i) })
                    .collect(),
            )
        }
        DataType::Float64 | DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            let values = if let Some(typed) = array.as_any().downcast_ref::<Float64Array>() {
                (0..typed.len())
                    .map(|i| if typed.is_null(i) { 0.0 } else { typed.value(i) })
                    .collect()
            } else {
                return Err(PyTypeError::new_err(format!(
                    "column '{column_name}' requires an explicit cast before SBDF export"
                )));
            };
            NativeColumnBuffer::Double(values)
        }
        DataType::Date32 => {
            let typed = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| PyTypeError::new_err(format!("failed to read date32 column '{column_name}'")))?;
            NativeColumnBuffer::TimeLike(
                (0..typed.len())
                    .map(|i| {
                        if typed.is_null(i) {
                            0
                        } else {
                            sbdf_millis_from_unix_days(typed.value(i) as i64)
                        }
                    })
                    .collect(),
            )
        }
        DataType::Date64 => {
            let typed = array
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| PyTypeError::new_err(format!("failed to read date64 column '{column_name}'")))?;
            NativeColumnBuffer::TimeLike(
                (0..typed.len())
                    .map(|i| {
                        if typed.is_null(i) {
                            0
                        } else {
                            sbdf_millis_from_unix_millis(typed.value(i))
                        }
                    })
                    .collect(),
            )
        }
        DataType::Timestamp(unit, _) => NativeColumnBuffer::TimeLike(
            timestamp_array_to_millis(array, *unit)?
                .into_iter()
                .map(sbdf_millis_from_unix_millis)
                .collect(),
        ),
        DataType::Time32(unit) | DataType::Time64(unit) => NativeColumnBuffer::TimeLike(time_array_to_millis(array, *unit)?),
        DataType::Utf8 => {
            let typed = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| PyTypeError::new_err(format!("failed to read utf8 column '{column_name}'")))?;
            let values = (0..typed.len())
                .map(|i| if typed.is_null(i) { Vec::new() } else { typed.value(i).as_bytes().to_vec() })
                .collect::<Vec<_>>();
            let ptrs = values.iter().map(|value| value.as_ptr().cast::<c_char>()).collect();
            let lengths = values.iter().map(|value| value.len() as c_int).collect();
            NativeColumnBuffer::String { values, ptrs, lengths }
        }
        DataType::LargeUtf8 => {
            let typed = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| PyTypeError::new_err(format!("failed to read large utf8 column '{column_name}'")))?;
            let values = (0..typed.len())
                .map(|i| if typed.is_null(i) { Vec::new() } else { typed.value(i).as_bytes().to_vec() })
                .collect::<Vec<_>>();
            let ptrs = values.iter().map(|value| value.as_ptr().cast::<c_char>()).collect();
            let lengths = values.iter().map(|value| value.len() as c_int).collect();
            NativeColumnBuffer::String { values, ptrs, lengths }
        }
        DataType::Binary => {
            let typed = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| PyTypeError::new_err(format!("failed to read binary column '{column_name}'")))?;
            let values = (0..typed.len())
                .map(|i| if typed.is_null(i) { Vec::new() } else { typed.value(i).to_vec() })
                .collect::<Vec<_>>();
            let ptrs = values.iter().map(|value| value.as_ptr().cast::<c_char>()).collect();
            let lengths = values.iter().map(|value| value.len() as c_int).collect();
            NativeColumnBuffer::Binary { values, ptrs, lengths }
        }
        DataType::LargeBinary => {
            let typed = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| PyTypeError::new_err(format!("failed to read large binary column '{column_name}'")))?;
            let values = (0..typed.len())
                .map(|i| if typed.is_null(i) { Vec::new() } else { typed.value(i).to_vec() })
                .collect::<Vec<_>>();
            let ptrs = values.iter().map(|value| value.as_ptr().cast::<c_char>()).collect();
            let lengths = values.iter().map(|value| value.len() as c_int).collect();
            NativeColumnBuffer::Binary { values, ptrs, lengths }
        }
        other => {
            return Err(PyTypeError::new_err(format!(
                "unsupported Arrow type '{other:?}' for column '{column_name}'"
            )))
        }
    };
    Ok((buffer, invalids))
}

fn write_record_batch_to_sbdf(
    writer: &mut StreamingSbdfWriter,
    batch: &RecordBatch,
) -> PyResult<()> {
    if batch.num_rows() == 0 {
        return Ok(());
    }
    if batch.num_columns() != writer.columns.len() {
        return Err(PyValueError::new_err("record batch columns do not match writer schema"));
    }

    let mut table_slice = null_mut();
    let error = unsafe { sbdf_ts_create(writer.table_meta, &mut table_slice) };
    if error != SBDF_OK {
        return Err(PyRuntimeError::new_err("failed to create SBDF table slice"));
    }

    let mut owned_columns: Vec<*mut sbdf_columnslice> = Vec::with_capacity(writer.columns.len());
    for (index, column_name) in writer.columns.iter().enumerate() {
        let array = batch.column(index).as_ref();
        let (buffer, invalids) = build_native_column_buffer(column_name, array)?;
        let values_obj = buffer.create_object(writer.column_types[index])?;
        let mut value_array = null_mut();
        let error = unsafe {
            sbdf_va_create(
                value_encoding(writer.column_types[index], writer.encoding_rle),
                values_obj,
                &mut value_array,
            )
        };
        unsafe { sbdf_obj_destroy(values_obj) };
        if error != SBDF_OK {
            unsafe { sbdf_ts_destroy(table_slice) };
            return Err(PyRuntimeError::new_err(format!(
                "failed to create value array for column '{column_name}'"
            )));
        }

        let mut column_slice = null_mut();
        let error = unsafe { sbdf_cs_create(&mut column_slice, value_array) };
        if error != SBDF_OK {
            unsafe {
                sbdf_va_destroy(value_array);
                sbdf_ts_destroy(table_slice);
            }
            return Err(PyRuntimeError::new_err(format!(
                "failed to create column slice for column '{column_name}'"
            )));
        }

        if invalids.iter().any(|flag| *flag == 1) {
            let mut invalid_obj = null_mut();
            let error = unsafe {
                sbdf_obj_create_arr(
                    sbdf_vt_bool(),
                    invalids.len() as c_int,
                    invalids.as_ptr().cast(),
                    null(),
                    &mut invalid_obj,
                )
            };
            if error != SBDF_OK {
                unsafe {
                    sbdf_cs_destroy_all(column_slice);
                    sbdf_ts_destroy(table_slice);
                }
                return Err(PyRuntimeError::new_err(format!(
                    "failed to create invalid markers for column '{column_name}'"
                )));
            }
            let mut invalid_array = null_mut();
            let error = unsafe { sbdf_va_create_dflt(invalid_obj, &mut invalid_array) };
            unsafe { sbdf_obj_destroy(invalid_obj) };
            if error != SBDF_OK {
                unsafe {
                    sbdf_cs_destroy_all(column_slice);
                    sbdf_ts_destroy(table_slice);
                }
                return Err(PyRuntimeError::new_err(format!(
                    "failed to create invalid array for column '{column_name}'"
                )));
            }
            let error = unsafe {
                sbdf_cs_add_property(
                    column_slice,
                    SBDF_ISINVALID_VALUEPROPERTY.as_ptr().cast(),
                    invalid_array,
                )
            };
            if error != SBDF_OK {
                unsafe {
                    sbdf_va_destroy(invalid_array);
                    sbdf_cs_destroy_all(column_slice);
                    sbdf_ts_destroy(table_slice);
                }
                return Err(PyRuntimeError::new_err(format!(
                    "failed to attach invalid array for column '{column_name}'"
                )));
            }
        }

        let error = unsafe { sbdf_ts_add(column_slice, table_slice) };
        if error != SBDF_OK {
            unsafe {
                sbdf_cs_destroy_all(column_slice);
                sbdf_ts_destroy(table_slice);
            }
            return Err(PyRuntimeError::new_err(format!(
                "failed to add column '{column_name}' to SBDF slice"
            )));
        }
        owned_columns.push(column_slice);
    }

    let error = unsafe { sbdf_ts_write(writer.output_file, table_slice) };
    unsafe { sbdf_ts_destroy(table_slice) };
    for column_slice in owned_columns {
        unsafe { sbdf_cs_destroy_all(column_slice) };
    }
    if error != SBDF_OK {
        return Err(PyRuntimeError::new_err("failed to write SBDF table slice"));
    }
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (parquet_path, sbdf_path, batch_size=50_000, column_types=None, encoding_rle=true))]
fn parquet_to_sbdf_streaming(
    parquet_path: String,
    sbdf_path: String,
    batch_size: usize,
    column_types: Option<HashMap<String, String>>,
    encoding_rle: bool,
) -> PyResult<()> {
    if batch_size == 0 {
        return Err(PyValueError::new_err("batch_size must be greater than zero"));
    }

    let input = File::open(&parquet_path).map_err(|error| {
        PyRuntimeError::new_err(format!("failed to open Parquet file '{parquet_path}': {error}"))
    })?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(input).map_err(|error| {
        PyRuntimeError::new_err(format!("failed to read Parquet metadata '{parquet_path}': {error}"))
    })?;
    let schema = builder.schema().as_ref().clone();
    let (columns, _) = infer_arrow_schema(&schema)?;
    let typed_columns = get_column_types(&columns, column_types, &schema)?;
    let mut reader = builder
        .with_batch_size(batch_size)
        .build()
        .map_err(|error| PyRuntimeError::new_err(format!("failed to build Parquet batch reader: {error}")))?;

    let mut writer = StreamingSbdfWriter::new(
        sbdf_path,
        Some(columns.clone()),
        Some(
            typed_columns
                .iter()
                .map(|(column, value_type)| {
                    let name = match value_type {
                        ValueType::Bool => "Boolean",
                        ValueType::Int => "Integer",
                        ValueType::Long => "LongInteger",
                        ValueType::Float => "SingleReal",
                        ValueType::Double => "Real",
                        ValueType::DateTime => "DateTime",
                        ValueType::Date => "Date",
                        ValueType::Time => "Time",
                        ValueType::TimeSpan => "TimeSpan",
                        ValueType::String => "String",
                        ValueType::Binary => "Binary",
                    };
                    (column.clone(), name.to_string())
                })
                .collect(),
        ),
        encoding_rle,
    )?;

    for batch in reader.by_ref() {
        let batch = batch.map_err(|error| {
            PyRuntimeError::new_err(format!("failed while reading Parquet batch '{parquet_path}': {error}"))
        })?;
        write_record_batch_to_sbdf(&mut writer, &batch)?;
    }
    writer.close()?;
    Ok(())
}

#[pyclass(unsendable)]
struct StreamingSbdfWriter {
    output_file: *mut c_void,
    table_meta: *mut sbdf_tablemetadata,
    columns: Vec<String>,
    column_types: Vec<ValueType>,
    initialized: bool,
    closed: bool,
    encoding_rle: bool,
}

#[pymethods]
impl StreamingSbdfWriter {
    #[new]
    #[pyo3(signature = (sbdf_file, columns=None, column_types=None, encoding_rle=true))]
    fn new(
        sbdf_file: String,
        columns: Option<Vec<String>>,
        column_types: Option<HashMap<String, String>>,
        encoding_rle: bool,
    ) -> PyResult<Self> {
        let filename = CString::new(sbdf_file.clone()).map_err(|_| PyValueError::new_err("invalid filename"))?;
        let mode = CString::new("wb").unwrap();
        let output_file = unsafe { fopen(filename.as_ptr(), mode.as_ptr()) };
        if output_file.is_null() {
            return Err(PyRuntimeError::new_err(format!("failed to open SBDF file: {sbdf_file}")));
        }
        let mut writer = Self {
            output_file,
            table_meta: null_mut(),
            columns: Vec::new(),
            column_types: Vec::new(),
            initialized: false,
            closed: false,
            encoding_rle,
        };
        if let Some(columns) = columns {
            let column_types = column_types.ok_or_else(|| PyValueError::new_err("column_types must be provided"))?;
            writer.initialize_schema(
                columns,
                column_types
                    .into_iter()
                    .map(|(key, value)| ValueType::from_name(&value).map(|typed| (key, typed)))
                    .collect::<PyResult<HashMap<_, _>>>()?,
            )?;
        }
        Ok(writer)
    }

    fn write_batch(&mut self, _py: Python<'_>, batch: Bound<'_, PyDict>) -> PyResult<()> {
        if self.closed {
            return Err(PyRuntimeError::new_err("writer already closed"));
        }

        let columns: Vec<String> = batch
            .keys()
            .iter()
            .map(|item| item.extract::<String>())
            .collect::<PyResult<_>>()?;
        if columns.is_empty() {
            return Ok(());
        }

        let row_count = batch
            .get_item(columns[0].as_str())?
            .ok_or_else(|| PyValueError::new_err("missing first column"))?
            .len()?;
        for column in &columns {
            let len = batch
                .get_item(column.as_str())?
                .ok_or_else(|| PyValueError::new_err(format!("missing batch column: {column}")))?
                .len()?;
            if len != row_count {
                return Err(PyValueError::new_err("all batch columns must have the same row count"));
            }
        }
        if row_count == 0 {
            return Ok(());
        }

        if !self.initialized {
            let mut inferred = HashMap::new();
            for column in &columns {
                let list = batch.get_item(column.as_str())?.unwrap();
                for item in list.try_iter()? {
                    let item = item?;
                    if !is_missing(&item)? {
                        inferred.insert(column.clone(), ValueType::infer(&item)?);
                        break;
                    }
                }
            }
            self.initialize_schema(columns.clone(), inferred)?;
        }

        if columns != self.columns {
            return Err(PyValueError::new_err("batch columns do not match writer schema"));
        }

        let mut table_slice = null_mut();
        let error = unsafe { sbdf_ts_create(self.table_meta, &mut table_slice) };
        if error != SBDF_OK {
            return Err(PyRuntimeError::new_err("failed to create SBDF table slice"));
        }

        let mut owned_columns: Vec<*mut sbdf_columnslice> = Vec::with_capacity(self.columns.len());
        for (index, column) in self.columns.iter().enumerate() {
            let list = batch.get_item(column.as_str())?.unwrap();
            let values: Vec<Bound<'_, PyAny>> = list.try_iter()?.collect::<PyResult<_>>()?;
            let invalids: Vec<u8> = values
                .iter()
                .map(|value| Ok(u8::from(is_missing(value)?)))
                .collect::<PyResult<_>>()?;
            let buffer = build_column_buffer(self.column_types[index], &values, &invalids)?;
            let values_obj = buffer.create_object(self.column_types[index])?;
            let mut value_array = null_mut();
            let error = unsafe {
                sbdf_va_create(
                    value_encoding(self.column_types[index], self.encoding_rle),
                    values_obj,
                    &mut value_array,
                )
            };
            unsafe { sbdf_obj_destroy(values_obj) };
            if error != SBDF_OK {
                unsafe { sbdf_ts_destroy(table_slice) };
                return Err(PyRuntimeError::new_err(format!("failed to create value array for column '{column}'")));
            }

            let mut column_slice = null_mut();
            let error = unsafe { sbdf_cs_create(&mut column_slice, value_array) };
            if error != SBDF_OK {
                unsafe {
                    sbdf_va_destroy(value_array);
                    sbdf_ts_destroy(table_slice);
                }
                return Err(PyRuntimeError::new_err(format!("failed to create column slice for column '{column}'")));
            }

            if invalids.iter().any(|flag| *flag == 1) {
                let mut invalid_obj = null_mut();
                let error = unsafe {
                    sbdf_obj_create_arr(
                        sbdf_vt_bool(),
                        invalids.len() as c_int,
                        invalids.as_ptr().cast(),
                        null(),
                        &mut invalid_obj,
                    )
                };
                if error != SBDF_OK {
                    unsafe {
                        sbdf_cs_destroy_all(column_slice);
                        sbdf_ts_destroy(table_slice);
                    }
                    return Err(PyRuntimeError::new_err(format!("failed to create invalid markers for column '{column}'")));
                }
                let mut invalid_array = null_mut();
                let error = unsafe { sbdf_va_create_dflt(invalid_obj, &mut invalid_array) };
                unsafe { sbdf_obj_destroy(invalid_obj) };
                if error != SBDF_OK {
                    unsafe {
                        sbdf_cs_destroy_all(column_slice);
                        sbdf_ts_destroy(table_slice);
                    }
                    return Err(PyRuntimeError::new_err(format!("failed to create invalid array for column '{column}'")));
                }
                let error =
                    unsafe { sbdf_cs_add_property(column_slice, SBDF_ISINVALID_VALUEPROPERTY.as_ptr().cast(), invalid_array) };
                if error != SBDF_OK {
                    unsafe {
                        sbdf_va_destroy(invalid_array);
                        sbdf_cs_destroy_all(column_slice);
                        sbdf_ts_destroy(table_slice);
                    }
                    return Err(PyRuntimeError::new_err(format!("failed to attach invalid array for column '{column}'")));
                }
            }

            let error = unsafe { sbdf_ts_add(column_slice, table_slice) };
            if error != SBDF_OK {
                unsafe {
                    sbdf_cs_destroy_all(column_slice);
                    sbdf_ts_destroy(table_slice);
                }
                return Err(PyRuntimeError::new_err(format!("failed to add column '{column}' to SBDF slice")));
            }
            owned_columns.push(column_slice);
        }

        let error = unsafe { sbdf_ts_write(self.output_file, table_slice) };
        unsafe { sbdf_ts_destroy(table_slice) };
        for column_slice in owned_columns {
            unsafe { sbdf_cs_destroy_all(column_slice) };
        }
        if error != SBDF_OK {
            return Err(PyRuntimeError::new_err("failed to write SBDF table slice"));
        }
        Ok(())
    }

    fn close(&mut self) -> PyResult<()> {
        if self.closed {
            return Ok(());
        }
        if !self.output_file.is_null() && self.initialized {
            let error = unsafe { sbdf_ts_write_end(self.output_file) };
            if error != SBDF_OK {
                return Err(PyRuntimeError::new_err("failed to write SBDF end marker"));
            }
        }
        self.cleanup();
        self.closed = true;
        Ok(())
    }
}

impl StreamingSbdfWriter {
    fn initialize_schema(&mut self, columns: Vec<String>, types: HashMap<String, ValueType>) -> PyResult<()> {
        self.columns = columns;
        self.column_types = self
            .columns
            .iter()
            .map(|column| {
                types
                    .get(column)
                    .copied()
                    .ok_or_else(|| PyValueError::new_err(format!("missing type for column '{column}'")))
            })
            .collect::<PyResult<_>>()?;

        let mut table_md = null_mut();
        let error = unsafe { sbdf_md_create(&mut table_md) };
        if error != SBDF_OK {
            return Err(PyRuntimeError::new_err("failed to create SBDF table metadata"));
        }
        let error = unsafe { sbdf_tm_create(table_md, &mut self.table_meta) };
        unsafe { sbdf_md_destroy(table_md) };
        if error != SBDF_OK {
            return Err(PyRuntimeError::new_err("failed to create SBDF table"));
        }

        for (column, value_type) in self.columns.iter().zip(self.column_types.iter()) {
            let mut column_md = null_mut();
            let error = unsafe { sbdf_md_create(&mut column_md) };
            if error != SBDF_OK {
                return Err(PyRuntimeError::new_err(format!("failed to create metadata for column '{column}'")));
            }
            let name = CString::new(column.as_str()).map_err(|_| PyValueError::new_err("invalid column name"))?;
            let error = unsafe { sbdf_cm_set_values(name.as_ptr(), value_type.sbdf_type(), column_md) };
            if error != SBDF_OK {
                unsafe { sbdf_md_destroy(column_md) };
                return Err(PyRuntimeError::new_err(format!("failed to set metadata for column '{column}'")));
            }
            let error = unsafe { sbdf_tm_add(column_md, self.table_meta) };
            unsafe { sbdf_md_destroy(column_md) };
            if error != SBDF_OK {
                return Err(PyRuntimeError::new_err(format!("failed to attach metadata for column '{column}'")));
            }
        }

        let error = unsafe { sbdf_fh_write_cur(self.output_file) };
        if error != SBDF_OK {
            return Err(PyRuntimeError::new_err("failed to write SBDF file header"));
        }
        let error = unsafe { sbdf_tm_write(self.output_file, self.table_meta) };
        if error != SBDF_OK {
            return Err(PyRuntimeError::new_err("failed to write SBDF metadata"));
        }

        self.initialized = true;
        Ok(())
    }

    fn cleanup(&mut self) {
        if !self.output_file.is_null() {
            unsafe {
                fclose(self.output_file);
            }
            self.output_file = null_mut();
        }
        if !self.table_meta.is_null() {
            unsafe {
                sbdf_tm_destroy(self.table_meta);
            }
            self.table_meta = null_mut();
        }
    }
}

impl Drop for StreamingSbdfWriter {
    fn drop(&mut self) {
        self.cleanup();
    }
}

#[pymodule]
fn streaming_sbdf_rs(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("__doc__", "Rust-backed streaming SBDF writer")?;
    module.add("SBDFError", _py.get_type::<PyRuntimeError>())?;
    module.add_class::<StreamingSbdfWriter>()?;
    module.add_function(wrap_pyfunction!(parquet_to_sbdf_streaming, module)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        days_since_epoch, sbdf_millis_from_unix_days, sbdf_millis_from_unix_millis, MILLIS_PER_DAY,
        UNIX_EPOCH_DAYS_FROM_YEAR_ONE, UNIX_EPOCH_MILLIS_FROM_YEAR_ONE,
    };

    #[test]
    fn year_one_epoch_is_zero_days() {
        assert_eq!(days_since_epoch(1, 1, 1), 0);
    }

    #[test]
    fn unix_epoch_offset_matches_year_one_calendar_days() {
        assert_eq!(days_since_epoch(1970, 1, 1), UNIX_EPOCH_DAYS_FROM_YEAR_ONE);
        assert_eq!(UNIX_EPOCH_MILLIS_FROM_YEAR_ONE, UNIX_EPOCH_DAYS_FROM_YEAR_ONE * MILLIS_PER_DAY);
    }

    #[test]
    fn date32_days_are_shifted_from_unix_epoch_to_sbdf_epoch() {
        assert_eq!(sbdf_millis_from_unix_days(0), UNIX_EPOCH_MILLIS_FROM_YEAR_ONE);
        let days_2025 = days_since_epoch(2025, 1, 1) - days_since_epoch(1970, 1, 1);
        assert_eq!(
            sbdf_millis_from_unix_days(days_2025),
            days_since_epoch(2025, 1, 1) * MILLIS_PER_DAY
        );
    }

    #[test]
    fn timestamp_millis_are_shifted_from_unix_epoch_to_sbdf_epoch() {
        assert_eq!(sbdf_millis_from_unix_millis(0), UNIX_EPOCH_MILLIS_FROM_YEAR_ONE);
        let noon_ms = 12 * 60 * 60 * 1000;
        assert_eq!(
            sbdf_millis_from_unix_millis(noon_ms),
            UNIX_EPOCH_MILLIS_FROM_YEAR_ONE + noon_ms
        );
    }
}
