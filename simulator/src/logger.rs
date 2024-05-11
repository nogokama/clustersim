use dslab_core::Id;
use lazy_static::lazy_static;
use std::fs::File;
use std::io::{self, Write};
use std::sync::Mutex;

// Define a global Mutex around a File
lazy_static! {
    static ref LOG_FILE: Mutex<File> = {
        let file_path = "load.txt";
        match File::create(file_path) {
            Ok(file) => Mutex::new(file),
            Err(e) => {
                eprintln!("Error creating file: {}", e);
                // You might want to handle the error differently based on your needs
                std::process::exit(1);
            }
        }
    };
}

// Define the global function log_struct
pub fn log_compute_load(time: f64, machine_id: &str, load: f64) {
    if let Ok(mut file) = LOG_FILE.lock() {
        if let Err(e) = writeln!(file, "cpu, {}, {}, {}", time, machine_id, load) {
            eprintln!("Error writing to file: {}", e);
        }
    } else {
        eprintln!("Error acquiring lock on the log file");
    }
}

pub fn log_memory_load(time: f64, machine_id: &str, load: f64) {
    if let Ok(mut file) = LOG_FILE.lock() {
        if let Err(e) = writeln!(file, "mem, {}, {}, {}", time, machine_id, load) {
            eprintln!("Error writing to file: {}", e);
        }
    } else {
        eprintln!("Error acquiring lock on the log file");
    }
}

pub fn log_machine_load() {}
