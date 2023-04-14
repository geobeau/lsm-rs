use std::io::Read;

use byteorder::{LittleEndian, ReadBytesExt};

pub enum Command {
    Set(Set),
    Get(Get),
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

const GET: u8 = 0u8;
const SET: u8 = 1u8;

pub struct Set {
    key: String,
    flags: u32,
    exptime: u32,
    data: Vec<u8>,
}

pub struct Get {
    key: String,
}

struct Header {
    magic: u8,
    opcode: u8,
    key_size: u16,
    extra_size: u8,
    status: u8,
    value_size: u32,
    opaque: u32,
    cas: u64,
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
//         let flags = u16::from_le_bytes(command[key_end+1..flags_end].try_into().unwrap());
//         cmd_iter.next().unwrap();

//         let exptime_end = cmd_iter.position(|c| *c == SPACE).unwrap();
//         let exptime = u64::from_le_bytes(command[flags_end+1..exptime_end].try_into().unwrap());

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
    reader: dyn Read,
}

impl MemcachedBinaryHandler {
    fn parse_header(&self, header_bytes: &[u8]) -> Header {
        return Header {
            magic: header_bytes[0],
            opcode: header_bytes[1],
            key_size: u16::from_le_bytes(header_bytes[2..4].try_into().unwrap()),
            extra_size: header_bytes[4],
            status: header_bytes[5],
            value_size: u32::from_le_bytes(header_bytes[8..12].try_into().unwrap()),
            opaque: u32::from_le_bytes(header_bytes[12..16].try_into().unwrap()),
            cas: u64::from_le_bytes(header_bytes[16..24].try_into().unwrap()),
        };
    }

    fn parse_set(&mut self, header: &Header) -> Option<Set> {
        assert_eq!(header.extra_size, 8u8);

        let flags = self.reader.read_u32::<LittleEndian>().unwrap();
        let exptime = self.reader.read_u32::<LittleEndian>().unwrap();

        let mut key_bytes = vec![0u8; header.key_size as usize];
        let mut data = vec![0u8; header.value_size as usize];
        self.reader.read_exact(&mut key_bytes).unwrap();
        self.reader.read_exact(&mut data).unwrap();
        let key = String::from_utf8(key_bytes.to_owned()).unwrap();

        Some(Set { key, flags, exptime, data })
    }

    fn parse_get(&mut self, header: &Header) -> Option<Get> {
        assert_eq!(header.extra_size, 0u8);

        let mut key_bytes = vec![0u8; header.key_size as usize];
        self.reader.read_exact(&mut key_bytes).unwrap();
        let key = String::from_utf8(key_bytes.to_owned()).unwrap();

        Some(Get { key })
    }
}

impl Iterator for MemcachedBinaryHandler {
    type Item = Command;

    fn next(&mut self) -> Option<Self::Item> {
        let mut header_buff = [0u8; 24];
        self.reader.read_exact(&mut header_buff).unwrap();
        let header = self.parse_header(&header_buff);

        match header.opcode {
            SET => Some(Command::Set(self.parse_set(&header).unwrap())),
            GET => Some(Command::Get(self.parse_get(&header).unwrap())),
            _ => todo!(),
        }
    }
}
