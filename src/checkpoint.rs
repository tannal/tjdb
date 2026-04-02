use serde::{Serialize, Deserialize};
use std::{fs, path::PathBuf};

#[derive(Serialize, Deserialize, Debug)]
pub struct Checkpoint {
    pub last_applied_lsn: u64,
    pub timestamp: i64,
}

impl Checkpoint {
    pub fn load(path: &PathBuf) -> Self {
        if let Ok(data) = fs::read_to_string(path) {
            serde_json::from_str(&data).unwrap_or(Checkpoint { last_applied_lsn: 0, timestamp: 0 })
        } else {
            Checkpoint { last_applied_lsn: 0, timestamp: 0 }
        }
    }

    pub fn save(&self, path: &PathBuf) {
        let data = serde_json::to_string(self).unwrap();
        fs::write(path, data).expect("Failed to save checkpoint");
    }
}