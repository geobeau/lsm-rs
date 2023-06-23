use futures::{AsyncReadExt, AsyncWriteExt};
use std::time::Duration;
pub mod server;

use glommio::{net::TcpStream, timer::sleep, GlommioError};

use crate::{
    api::{self},
    record::{Key, Record},
};

#[derive(Debug, Clone)]
pub enum Command {
    Set(Set),
    Get(Get),
}

impl Command {
    pub fn to_api_command(self) -> api::Command {
        match self {
            Command::Set(s) => api::Command::Set(api::Set {
                record: Record::new(s.key, s.data),
            }),
            Command::Get(g) => api::Command::Get(api::Get { key: Key::new(g.key) }),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Response {
    Set(SetResp),
    Get(GetResp),
}

impl Response {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Response::Set(s) => s.to_bytes(),
            Response::Get(g) => g.to_bytes(),
        }
    }

    pub fn from_api_response(response: api::Response) -> Response {
        match response {
            api::Response::Get(g) => {
                let maybe_value = match g.record {
                    Some(r) => Some(r.value),
                    None => None,
                };
                Response::Get(GetResp {
                    flags: 0,
                    opcode: OpCode::NoError,
                    cas: 0,
                    value: maybe_value,
                })
            }
            api::Response::Delete(_) => todo!(),
            api::Response::Set(_s) => Response::Set(SetResp {
                opcode: OpCode::NoError,
                cas: 0,
            }),
        }
    }
}

// 0x00    Get
// 0x01    Set
// 0x02    Add
// 0x03    Replace
// 0x04    Delete
// 0x05    Increment
// 0x06    Decrement
// 0x07    Quit
// 0x08    Flush
// 0x09    GetQ
// 0x0A    No-op
// 0x0B    Version
// 0x0C    GetK
// 0x0D    GetKQ
// 0x0E    Append
// 0x0F    Prepend
// 0x10    Stat
// 0x11    SetQ
// 0x12    AddQ
// 0x13    ReplaceQ
// 0x14    DeleteQ
// 0x15    IncrementQ
// 0x16    DecrementQ
// 0x17    QuitQ
// 0x18    FlushQ
// 0x19    AppendQ
// 0x1A    PrependQ

const GET: u8 = 0x0;
const SET: u8 = 0x1;

#[derive(Debug, Clone)]
pub struct Set {
    pub key: String,
    pub flags: u32,
    pub exptime: u32,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Get {
    pub key: String,
}

#[derive(Debug, Clone)]
pub struct SetResp {
    pub opcode: OpCode,
    pub cas: u64,
}

impl SetResp {
    pub fn to_bytes(&self) -> Vec<u8> {
        let h = Header {
            magic: 0x81,
            opcode: self.opcode as u8,
            key_size: 0,
            extra_size: 0,
            status: 0,
            body_length: 0,
            opaque: 0,
            cas: self.cas,
            data_type: 0,
        };
        h.to_be_bytes().to_vec()
    }
}

#[derive(Debug, Clone)]
pub struct GetResp {
    pub flags: u32,
    pub opcode: OpCode,
    pub cas: u64,
    pub value: Option<Vec<u8>>,
}

impl GetResp {
    pub fn to_bytes(&self) -> Vec<u8> {
        let value_size = match &self.value {
            Some(v) => v.len(),
            None => 0,
        } as u32;
        let body_size = 4 + value_size as usize;
        let mut resp = Vec::with_capacity(body_size);
        resp.extend(
            Header {
                magic: 0x81,
                opcode: 0x0,
                key_size: 0,
                extra_size: 0,
                status: 0,
                body_length: body_size as u32,
                opaque: 0,
                cas: self.cas,
                data_type: 0,
            }
            .to_be_bytes()
            .to_vec(),
        );
        resp.extend(self.flags.to_be_bytes());
        match &self.value {
            Some(v) => resp.extend(v.clone()),
            None => (),
        };
        resp
    }
}

// 0x0000 	No error
// 0x0001 	Key not found
// 0x0002 	Key exists
// 0x0003 	Value too large
// 0x0004 	Invalid arguments
// 0x0005 	Item not stored
// 0x0006 	Incr/Decr on non-numeric value.
// 0x0007 	The vbucket belongs to another server
// 0x0008 	Authentication error
// 0x0009 	Authentication continue
// 0x0081 	Unknown command
// 0x0082 	Out of memory
// 0x0083 	Not supported
// 0x0084 	Internal error
// 0x0085 	Busy
// 0x0086 	Temporary failure

#[derive(Debug, Clone, Copy)]
pub enum OpCode {
    NoError = 0,
    KeyNotFound = 1,
    KeyExists = 2,
    ValueTooLarge = 3,
    InvalidArguments = 4,
    ItemNotStored = 5,
    IncrDecrNonNum = 6,
    VBucketBelongsToAnotherServer = 7,
    AuthErr = 8,
    AuthContinue = 9,
    UnknownCommand = 81,
    OOM = 82,
    NotSupported = 83,
    InternalError = 84,
    Busy = 85,
    TemporaryFailure = 86,
}

pub enum GetResult {
    Ok(Vec<u8>),
    Err(OpCode),
}

#[derive(Debug)]
struct Header {
    magic: u8,
    opcode: u8,
    key_size: u16,
    extra_size: u8,
    data_type: u8,
    status: u16,
    body_length: u32,
    opaque: u32,
    cas: u64,
}

impl Header {
    fn get_data_length(&self) -> usize {
        self.body_length as usize - self.key_size as usize - self.extra_size as usize
    }

    fn to_be_bytes(&self) -> [u8; 24] {
        let mut bytes = [0u8; 24];
        bytes[0] = self.magic;
        bytes[1] = self.opcode;
        bytes[2..4].copy_from_slice(&self.key_size.to_be_bytes());
        bytes[4] = self.extra_size;
        bytes[5] = self.data_type;
        bytes[6..8].copy_from_slice(&self.status.to_be_bytes());
        bytes[8..12].copy_from_slice(&self.body_length.to_be_bytes());
        bytes[12..16].copy_from_slice(&self.opaque.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.cas.to_be_bytes());
        bytes
    }

    fn from_be_bytes(bytes: [u8; 24]) -> Header {
        Header {
            magic: bytes[0],
            opcode: bytes[1],
            key_size: u16::from_be_bytes(bytes[2..4].try_into().unwrap()),
            extra_size: bytes[4],
            data_type: bytes[5],
            status: u16::from_be_bytes(bytes[6..8].try_into().unwrap()),
            body_length: u32::from_be_bytes(bytes[8..12].try_into().unwrap()),
            opaque: u32::from_be_bytes(bytes[12..16].try_into().unwrap()),
            cas: u64::from_be_bytes(bytes[16..24].try_into().unwrap()),
        }
    }
}

// pub struct MemcachedAsciiHandler {
//     reader: dyn Read
// }

// impl MemcachedAsciiHandler {

//     // <command name> <key> <flags> <exptime> <bytes> [noreply]\r\n
//     fn parse_set(&mut self) -> Option<Set> {
//         let mut command = Vec::new();
//         self.reader.read(&mut command).unwrap();

//         let mut cmd_iter = command.iter();
//         let key_end = cmd_iter.position(|c| *c == SPACE).unwrap();
//         let key = String::from_utf8(command[1..key_end].to_vec()).unwrap();
//         cmd_iter.next().unwrap();

//         let flags_end = cmd_iter.position(|c| *c == SPACE).unwrap();
//         let flags = u16::from_be_bytes(command[key_end+1..flags_end].try_into().unwrap());
//         cmd_iter.next().unwrap();

//         let exptime_end = cmd_iter.position(|c| *c == SPACE).unwrap();
//         let exptime = u64::from_be_bytes(command[flags_end+1..exptime_end].try_into().unwrap());

//         let data = command[exptime_end+1..command.len()-4].to_vec();
//         Some(Set { key, flags, exptime, data })
//     }
// }

// impl Iterator for MemcachedAsciiHandler {
//     type Item = Command;

//     fn next(&mut self) -> Option<Self::Item> {
//         let mut command_buffer = [0u8; 3];
//         self.reader.read(&mut command_buffer).unwrap();
//         match command_buffer {
//             SET => Some(Command::Set(self.parse_set().unwrap())),
//             GET => todo!(),
//             _ => todo!(),
//         }
//     }
// }

// Byte/     0       |       1       |       2       |       3       |
// /              |               |               |               |
// |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
// +---------------+---------------+---------------+---------------+
// 0| Magic         | Opcode        | Key Length                    |
// +---------------+---------------+---------------+---------------+
// 4| Extras length | Data type     | Status                        |
// +---------------+---------------+---------------+---------------+
// 8| Total body length                                             |
// +---------------+---------------+---------------+---------------+
// 12| Opaque                                                        |
// +---------------+---------------+---------------+---------------+
// 16| CAS                                                           |
// |                                                               |
// +---------------+---------------+---------------+---------------+
// Total 24 bytes

// Header fields:

// Magic               Magic number.
// Opcode              Command code.
// Key length          Length in bytes of the text key that follows the command extras.
// Status              Status of the response (non-zero on error).
// Extras length       Length in bytes of the command extras.
// Data type           Reserved for future use (Sean is using this soon).

pub struct MemcachedBinaryHandler {
    pub stream: TcpStream,
}

impl MemcachedBinaryHandler {
    async fn parse_set(&mut self, header: &Header) -> Option<Set> {
        assert_eq!(header.extra_size, 8u8);

        let mut extra_buf = [0u8; 8];
        self.stream.read_exact(&mut extra_buf).await.unwrap();
        let flags = u32::from_be_bytes(extra_buf[0..4].try_into().unwrap());
        let exptime = u32::from_be_bytes(extra_buf[4..8].try_into().unwrap());

        let mut key_bytes = vec![0u8; header.key_size as usize];
        let mut data = vec![0u8; header.get_data_length()];

        self.stream.read_exact(&mut key_bytes).await.unwrap();
        self.stream.read_exact(&mut data).await.unwrap();
        let key = String::from_utf8(key_bytes.to_owned()).unwrap();

        Some(Set { key, flags, exptime, data })
    }

    async fn parse_get(&mut self, header: &Header) -> Option<Get> {
        assert_eq!(header.extra_size, 0u8);

        let mut key_bytes = vec![0u8; header.key_size as usize];
        self.stream.read_exact(&mut key_bytes).await.unwrap();
        let key = String::from_utf8(key_bytes.to_owned()).unwrap();

        Some(Get { key })
    }

    pub async fn await_new_data(&mut self) -> Result<(), GlommioError<()>> {
        // TODO: Make this a future
        let mut buffer = [0u8; 24];
        loop {
            let res = self.stream.peek(&mut buffer).await;
            match res {
                Ok(b) => {
                    if b > 0 {
                        return Ok(());
                    }
                }
                Err(r) => return Err(r),
            }
            sleep(Duration::from_millis(1)).await;
        }
    }

    pub async fn decode_command(&mut self) -> Option<Command> {
        let mut header_buff = [0u8; 24];
        self.stream.read_exact(&mut header_buff).await.unwrap();
        let header = Header::from_be_bytes(header_buff);
        match header.opcode {
            SET => Some(Command::Set(self.parse_set(&header).await.unwrap())),
            GET => Some(Command::Get(self.parse_get(&header).await.unwrap())),
            _ => todo!(),
        }
    }

    pub async fn write_resp(&mut self, buff: &[u8]) {
        self.stream.write(buff).await.unwrap();
    }
}

// impl Iterator for MemcachedBinaryHandler {
//     type Item = Command;

//     fn get_command(&mut self) -> Option<Self::Item> {
//         let mut header_buff = [0u8; 24];
//         self.reader.read_exact(&mut header_buff).unwrap();
//         let header = self.parse_header(&header_buff);

//         match header.opcode {
//             SET => Some(Command::Set(self.parse_set(&header).unwrap())),
//             GET => Some(Command::Get(self.parse_get(&header).unwrap())),
//             _ => todo!(),
//         }
//     }
// }
