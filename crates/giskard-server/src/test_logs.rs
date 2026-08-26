use std::io::{self, Write};
use std::sync::{Arc, Mutex};

pub(crate) struct CapturedLogWriter(pub(crate) Arc<Mutex<Vec<u8>>>);

impl Write for CapturedLogWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
