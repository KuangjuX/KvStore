use std::collections::HashMap;
use std::path::PathBuf;
use std::fs::{ File, OpenOptions };
use std::io::{ Seek, SeekFrom, Read, Write };
use serde::{Serialize, Deserialize};

use std::sync::RwLock;

use super::Result;

pub trait Disk {
    fn read(&self, offset: usize, buf: &mut [u8]);
    fn write(&self, offset: usize, buf: &[u8]);
}

pub struct BlockFile(RwLock<File>);

impl Disk for BlockFile {
    fn read(&self, offset: usize, buf: &mut [u8]) {
        let mut file = self.0.read().unwrap();
        file.seek(SeekFrom::Start(offset as u64)).expect("Error when seeking!");
        assert_eq!(file.read(buf).unwrap(), buf.len());
    }

    fn write(&self, offset: usize, buf: &[u8]) {
        let mut file = self.0.write().unwrap();
        file.seek(SeekFrom::Start(offset as u64)).expect("Error when seeking!");
        assert_eq!(file.write(buf).unwrap(), buf.len());
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct KvDate {
    key: String,
    value: String
}



pub struct KvStore {
    // map: HashMap<String, String>,
    disk: BlockFile
}

impl KvStore {
    /// Create a KvStore.
    pub fn new() -> Self {
        Self {
            disk: BlockFile(
                RwLock::new({
                    let f = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .open("disk")
                        .unwrap();
                    f
                })
            )
        }
    }

    /// Get value by key.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        // match self.map.get(&key) {
        //     Some(value) => Ok(Some(value.clone())),

        //     None => Ok(None),
        // }
        unimplemented!()
    }

    /// Set key-value
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        
    }

    /// Remove key
    pub fn remove(&mut self, key: String) -> Result<()>{
        unimplemented!()
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        unimplemented!()
    }
}
