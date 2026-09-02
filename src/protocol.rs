use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientMessage {
    Input(Vec<u8>),
    Resize { rows: u16, cols: u16 },
    Stop,
    Close,
    Detach,
    // Appended so an older supervisor keeps decoding the variants above.
    Restart,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerMessage {
    Hello {
        pid: u32,
        command: Vec<String>,
        running: bool,
    },
    Output {
        bytes: Vec<u8>,
        timestamp_ms: u64,
    },
    Exited(Option<i32>),
    Error(String),
    // Appended so an older client keeps decoding the variants above.
    Restarted {
        pid: u32,
    },
}

pub fn send<T: Serialize>(writer: &mut impl Write, message: &T) -> Result<()> {
    let payload = bincode::serialize(message).context("encode protocol message")?;
    let length = u32::try_from(payload.len()).context("protocol message too large")?;
    writer.write_all(&length.to_be_bytes())?;
    writer.write_all(&payload)?;
    writer.flush()?;
    Ok(())
}

pub fn receive<T: for<'de> Deserialize<'de>>(reader: &mut impl Read) -> Result<T> {
    let mut header = [0_u8; 4];
    reader.read_exact(&mut header)?;
    let length = u32::from_be_bytes(header) as usize;
    if length > 16 * 1024 * 1024 {
        anyhow::bail!("invalid protocol frame length: {length}");
    }
    let mut payload = vec![0; length];
    reader.read_exact(&mut payload)?;
    bincode::deserialize(&payload).context("decode protocol message")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn protocol_round_trip_preserves_terminal_bytes() {
        let expected = ClientMessage::Input(vec![0, 3, 27, 0xff]);
        let mut bytes = Vec::new();
        send(&mut bytes, &expected).unwrap();
        let decoded: ClientMessage = receive(&mut Cursor::new(bytes)).unwrap();
        match decoded {
            ClientMessage::Input(actual) => assert_eq!(actual, vec![0, 3, 27, 0xff]),
            _ => panic!("wrong message variant"),
        }
    }

    #[test]
    fn rejects_unbounded_frames() {
        let bytes = (17_u32 * 1024 * 1024).to_be_bytes();
        assert!(receive::<ClientMessage>(&mut Cursor::new(bytes)).is_err());
    }
}
