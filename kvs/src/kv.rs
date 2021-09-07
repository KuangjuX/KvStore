use std::collections::HashMap;
use std::path::PathBuf;
use std::fs::{ File, OpenOptions };
use std::io::{ Seek, SeekFrom, Read, Write, Error };
use std::str;
use std::sync::{ Mutex, Arc };

use serde::{Serialize, Deserialize};
use serde_json;
use super::Result;
use crate::error::SelfError;

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
                            .open("disk")
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
                let mut buf = vec![0; len];
                let device = Arc::clone(&self.device);
                device.read(offset, &mut buf);

                let data: Command = serde_json::from_str(str::from_utf8(&buf).unwrap()).unwrap();
                match data {
                    Command::Set{ value, ..} => {
                        Ok(Some(value))
                    },

                    _ => {
                        Ok(None)
                    }
                }
            },

            None => {
                Ok(None)
            }
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // match self.map.get(&key) {
          
        // }
        unimplemented!()
    }


    pub fn remove(&mut self, key: String) -> Result<()>{
        let offset = self.size;
        let command = Command::Remove{ key: key.clone() };
        let json;
        match serde_json::to_string(&command) {
            Ok(serde) => {
                json = serde;
            },
            Err(err) => {
                return Err(SelfError::SerdeErr(err))
            }
        }
        let buf  = json.as_bytes();
        // 从磁盘中移除key
        self.map.remove(&key);
        self.device.clone().write(offset, buf);
        Ok(())
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        unimplemented!()
    }

    pub fn compact(&self) {
        
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Command {
    Set{ key: String, value: String },
    Remove{ key: String }
}
