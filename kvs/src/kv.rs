use std::collections::HashMap;
use std::mem::size_of;
use std::path::PathBuf;
use std::fs::{ File, OpenOptions };
use std::io::{ Seek, SeekFrom, Read, Write, Error };
use std::str;
use std::sync::{ Mutex, Arc };
use std::ptr;

use serde::{Serialize, Deserialize};
use serde_json;
use super::Result;
use crate::error::KvsError;

// 消息格式： [偏移量 | 消息长度 | 消息 ]
pub trait Disk {
    fn read(&self, offset: usize, buf: &mut [u8]);
    fn write(&self, offset: usize, buf: &[u8]);
}

pub struct BlockFile(Mutex<File>);

impl Disk for BlockFile {
    fn read(&self, offset: usize, buf: &mut [u8]) {
        let mut file = self.0.lock().unwrap();
        file.seek(SeekFrom::Start(offset as u64)).expect("Error when seeking!");
        assert_eq!(file.read(buf).unwrap(), buf.len());
    }

    fn write(&self, offset: usize, buf: &[u8]) {
        let mut file = self.0.lock().unwrap();
        file.seek(SeekFrom::Start(offset as u64)).expect("Error when seeking!");
        assert_eq!(file.write(buf).unwrap(), buf.len());
    }
}

pub struct MemoryData {
    offset: usize, 
    len: usize
}

pub struct KvStore {
    /// 键以及偏移量
    map: HashMap<String, MemoryData>,
    /// 块设备
    device: Arc<BlockFile>,
    /// 磁盘大小
    size: usize
}

impl KvStore {
    /// 新建一个KvStore
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            device: Arc::new(
                BlockFile(
                    Mutex::new({
                        let f = OpenOptions::new()
                            .read(true)
                            .write(true)
                            .create(true)
                            .open("log.img")
                            .unwrap();
                        f
                    })
                )
            ),
            size: 0,
        }
    }

    /// 从内存中获取key以及对应的offset和len，从内存中获取对应的命令
    pub fn get(&self, key: String) -> Result<Option<String>> {
        match self.map.get(&key) {
            Some(memory_data) => {
                let offset = memory_data.offset;
                let len = memory_data.len;

                // 读出数据，此时读出的数据为LogRecord
                let mut buf = vec![0; len];
                let device = Arc::clone(&self.device);
                device.read(offset, &mut buf);

                // 解析LogRecord
                let data = &buf[12..];
                match serde_json::from_slice(data) {
                    Ok(log) => {
                        match log {
                            Command::Set{value,..} => {
                                Ok(Some(value))
                            },

                            _ => {
                                Ok(None)
                            }
                        }
                    },

                    Err(err) => {
                        Err(KvsError::SerdeErr(err))
                    }
                }
                
            },

            None => {
                Ok(None)
            }
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let offset = self.size;
        let command = Command::Set{ key: key.clone(), value: value.clone() };
        let buf: String;
        match serde_json::to_string(&command) {
            Ok(json) => {
                buf = json;
            }
            Err(err) => {
                return Err(KvsError::SerdeErr(err))
            }
        }
        println!("[Debug] command: {}", buf);
        let buf = buf.as_bytes();
        // 将命令写入磁盘
        // 首先生成LogRecord,随后将LogRecord序列化写入磁盘
        let log_record = LogRecord::new(offset, buf);
        let buf = log_record.serialize();
        self.device.write(offset, &buf);

        // 更新内存的值
        let memdata = MemoryData{
            offset,
            len: buf.len()
        };
        self.map.insert(key, memdata);
        // 更新磁盘大小
        self.size += buf.len();
        Ok(())
    }


    pub fn remove(&mut self, key: String) -> Result<()>{
        let offset = self.size;
        let command = Command::Remove{ key: key.clone() };
        let buf: String;
        match serde_json::to_string(&command) {
            Ok(json) => {
                buf = json;
            },
            Err(err) => {
                return Err(KvsError::SerdeErr(err))
            }
        }
        println!("[Debug] command: {}", buf);
        let buf  = buf.as_bytes();

        // 生成LogRecord并将其序列化
        let log_record = LogRecord::new(offset, buf);
        let buf = log_record.serialize();
        // 从磁盘中移除key
        self.map.remove(&key);
        self.device.clone().write(offset, &buf);
        // 更新磁盘size
        self.size += buf.len();
        Ok(())
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        unimplemented!()
    }

    pub fn compact(&self) {
        unimplemented!()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Command {
    Set{ key: String, value: String },
    Remove{ key: String }
}

pub struct LogRecord<'a> {
    pub offset: usize,
    pub memsize: u32,
    pub command: &'a [u8]
}

impl<'a> LogRecord<'a> {
    pub fn new(offset: usize, command: &'a [u8]) -> Self {
        Self {
            offset,
            memsize: command.len() as u32,
            command
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = vec![0u8; self.memsize as usize + size_of::<u32>() + size_of::<usize>()];
        unsafe{
            ptr::write(buf.as_mut_ptr() as *mut usize, self.offset);
            ptr::write(buf.as_mut_ptr().offset(8) as *mut u32, self.memsize);
            for i in 0..self.memsize as usize {
                let offset = size_of::<usize>() + size_of::<u32>() + i;
                ptr::write(
                    buf.as_mut_ptr().offset(offset as isize), 
                    self.command[i]
                );
            }
        }
        buf
    }
 
}
