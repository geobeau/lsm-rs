use core::{hash, str};
use monoio::{io::{AsyncBufRead, AsyncWriteRentExt, BufReader}, time};

#[macro_use]
mod macros;

use std::{borrow::Cow, cmp::Ordering, collections::HashMap};



const SEPARATOR: &[u8] = "\r\n".as_bytes();

pub fn redis_hashable_value_to_bytes(value: &HashableValue, buffer: &mut Vec<u8>) {
    match value {
        HashableValue::Blob(blob) => {
            buffer.push(b'$');
            buffer.extend_from_slice(blob.len().to_string().as_str().as_bytes());
            buffer.extend_from_slice(SEPARATOR);
            buffer.extend_from_slice(blob);
            buffer.extend_from_slice(SEPARATOR);
        },
        HashableValue::String(cow) => {
            buffer.push(b'+');
            buffer.extend_from_slice(cow.as_bytes());
            buffer.extend_from_slice(SEPARATOR);
        },
        HashableValue::Error(prefix, msg) => {
            buffer.push(b'-');
            buffer.extend_from_slice(prefix.as_bytes());
            buffer.push(b'-');
            buffer.extend_from_slice(msg.as_bytes());
            buffer.extend_from_slice(SEPARATOR);
        },
        HashableValue::Integer(i) => {
            buffer.push(b':');
            buffer.extend_from_slice(format!("{i}").as_bytes());
            buffer.extend_from_slice(SEPARATOR);
        },
        HashableValue::Boolean(_) => todo!(),
        HashableValue::BigInteger(_) => todo!(),
    }
}

pub fn redis_non_hashable_value_to_bytes(value: &NonHashableValue, buffer: &mut Vec<u8>) {
    match value {
        NonHashableValue::Array(vec) => {
            buffer.push(b'*');
            // TODO: hopefully this doesn't create an actual string
            buffer.extend_from_slice(vec.len().to_string().as_str().as_bytes());
            buffer.extend_from_slice(SEPARATOR);
            vec.iter().for_each(|val| redis_value_to_bytes(val, buffer));
            // buffer.extend_from_slice(SEPARATOR);
        },
        NonHashableValue::Float(_) => todo!(),
        NonHashableValue::Map(map) => {
            buffer.push(b'%');
            // TODO: hopefully this doesn't create an actual string
            buffer.extend_from_slice(map.len().to_string().as_str().as_bytes());
            buffer.extend_from_slice(SEPARATOR);
            map.iter().for_each(|(key, val)| {
                redis_hashable_value_to_bytes(key, buffer);
                redis_value_to_bytes(val, buffer);
            });
        },
    }
}


pub fn redis_value_to_bytes(value: &Value, buffer: &mut Vec<u8>) {
    match value {
        Value::HashableValue(hashable_value) => redis_hashable_value_to_bytes(hashable_value, buffer),
        Value::NonHashableValue(non_hashable_value) => redis_non_hashable_value_to_bytes(non_hashable_value, buffer),
        Value::Null => {
            buffer.push(b'_');
            buffer.extend_from_slice(SEPARATOR);
        },
    }
}



/// Redis Value.
#[derive(Debug, Clone)]
pub enum Value<'a> {
    /// Values that can hashed
    HashableValue(HashableValue<'a>),
    /// Values that cannot be hashed
    NonHashableValue(NonHashableValue<'a>),
    /// Null
    Null,
}

impl<'a> Value<'a> {
    pub fn try_as_str(&self) -> Option<&'a str> {
        match self {
            Value::HashableValue(hashable_value) => match hashable_value {
                HashableValue::Blob(blob) => Some(str::from_utf8(blob).unwrap()),
                HashableValue::String(cow) => todo!(),
                HashableValue::Error(cow, cow1) => todo!(),
                HashableValue::Integer(_) => todo!(),
                HashableValue::BigInteger(_) => todo!(),
                HashableValue::Boolean(_) => todo!(),
            },
            Value::NonHashableValue(non_hashable_value) => todo!(),
            Value::Null => todo!(),
        }
    }
}

/// Redis Value.
#[derive(Debug, Clone)]
pub enum NonHashableValue<'a> {
    /// Vector of values
    Array(Vec<Value<'a>>),
    /// Float number
    Float(f64),
    /// Map
    Map(HashMap<HashableValue<'a>, Value<'a>>),
}


/// Redis Value.
#[derive(Debug, PartialEq, Eq, Clone, Hash)]
pub enum HashableValue<'a> {
    /// Binary data
    Blob(&'a [u8]),
    /// String. New lines are not allowed
    String(Cow<'a, str>),
    /// Error
    Error(Cow<'a, str>, Cow<'a, str>),
    /// Integer
    Integer(i64),
    /// Big integers
    BigInteger(i128),
    /// Boolean
    Boolean(bool),
}



/// Protocol errors
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Error {
    /// The data is incomplete. This it not an error per-se, but rather a
    /// mechanism to let the caller know they should keep buffering data before
    /// calling the parser again.
    Partial,
    /// Unexpected first byte after a new line
    InvalidPrefix,
    /// Invalid data length
    InvalidLength,
    /// Parsed value is not boolean
    InvalidBoolean,
    /// Parsed data is not a number
    InvalidNumber,
    /// Protocol error
    Protocol(u8, u8),
    /// Missing new line
    NewLine,
}


/// Parses redis values from an stream of bytes. If the data is incomplete
/// Err(Error::Partial) is returned.
///
/// The first value is returned along side with the unconsumed stream of bytes.
pub fn parse(bytes: &[u8]) -> Result<(&[u8], Value), Error> {
    let (bytes, byte) = next!(bytes);
    let var_name = match byte {
        b'*' => parse_array(bytes),
        b'$' => parse_blob(bytes),
        b':' => parse_integer(bytes),
        b'(' => parse_big_integer(bytes),
        b',' => parse_float(bytes),
        b'#' => parse_boolean(bytes),
        b'+' => parse_str(bytes),
        b'-' => parse_error(bytes),
        b'%' => parse_map(bytes),
        _ => Err(Error::InvalidPrefix),
    };
    var_name
}


fn parse_error(bytes: &[u8]) -> Result<(&[u8], Value), Error> {
    let (bytes, err_type) = read_until!(bytes, b' ');
    let (bytes, str) = read_line!(bytes);
    let err_type = String::from_utf8_lossy(err_type);
    let str = String::from_utf8_lossy(str);
    ret!(bytes, Value::HashableValue(HashableValue::Error(err_type, str)))
}

fn parse_str(bytes: &[u8]) -> Result<(&[u8], Value), Error> {
    let (bytes, str) = read_line!(bytes);
    let str = String::from_utf8_lossy(str);
    ret!(bytes, Value::HashableValue(HashableValue::String(str)))
}

fn parse_boolean(bytes: &[u8]) -> Result<(&[u8], Value), Error> {
    let (bytes, byte) = next!(bytes);
    let v = match byte {
        b't' => true,
        b'f' => false,
        _ => return Err(Error::InvalidBoolean),
    };
    ret!(bytes, Value::HashableValue(HashableValue::Boolean(v)))
}

fn parse_big_integer(bytes: &[u8]) -> Result<(&[u8], Value), Error> {
    let (bytes, number) = read_line_number!(bytes, i128);
    ret!(bytes, Value::HashableValue(HashableValue::BigInteger(number)))
}

fn parse_integer(bytes: &[u8]) -> Result<(&[u8], Value), Error> {
    let (bytes, number) = read_line_number!(bytes, i64);
    ret!(bytes, Value::HashableValue(HashableValue::Integer(number)))
}

fn parse_float(bytes: &[u8]) -> Result<(&[u8], Value), Error> {
    let (bytes, number) = read_line_number!(bytes, f64);
    ret!(bytes, Value::NonHashableValue(NonHashableValue::Float(number)))
}

fn parse_blob(bytes: &[u8]) -> Result<(&[u8], Value), Error> {
    let (bytes, len) = read_line_number!(bytes, i64);

    match len.cmp(&0) {
        Ordering::Less => {
            let bytes = assert_nl!(bytes);
            return ret!(bytes, Value::Null);
        }
        Ordering::Equal => {
            let bytes = assert_nl!(bytes);
            return ret!(bytes, Value::HashableValue(HashableValue::Blob(b"")));
        }
        _ => {}
    };

    let len = len.try_into().expect("Positive number");

    let (bytes, blob) = read_len!(bytes, len);
    let bytes = assert_nl!(bytes);

    ret!(bytes, Value::HashableValue(HashableValue::Blob(blob)))
}

fn parse_map(bytes: &[u8]) -> Result<(&[u8], Value), Error> {
    let (bytes, len) = read_line_number!(bytes, i32);
    if len <= 0 {
        return ret!(bytes, Value::Null);
    }
    let mut v: HashMap<HashableValue, Value> = HashMap::with_capacity(len as usize);
    let mut val: Value;
    let mut key: Value;
    let mut bytes = bytes;
    for _ in 0..len {
        (bytes, key) = parse(bytes)?;
        (bytes, val) = parse(bytes)?;
        match key {
            Value::HashableValue(hashable_value) => v.insert(hashable_value, val),
            Value::NonHashableValue(_) => todo!(),
            Value::Null => todo!(),
        };
    }
    return Ok((bytes, Value::NonHashableValue(NonHashableValue::Map(v))))
}

fn parse_array(bytes: &[u8]) -> Result<(&[u8], Value), Error> {
    let (bytes, len) = read_line_number!(bytes, i32);
    if len <= 0 {
        return ret!(bytes, Value::Null);
    }

    let mut v = vec![Value::Null; len as usize];
    let mut bytes = bytes;

    for i in 0..len {
        let r = parse(bytes)?;
        bytes = r.0;
        v[i as usize] = r.1;
    }

    ret!(bytes, Value::NonHashableValue(NonHashableValue::Array(v)))
}
