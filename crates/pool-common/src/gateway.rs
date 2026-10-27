//! Framing between the Stratum gateway and engines (see STRATUM_GATEWAY.md).
//!
//! `[kind: u8][conn: u64 BE][len: u32 BE][payload]`, one frame per message,
//! over a unix stream socket.

use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Largest payload accepted; a Stratum line is far smaller.
pub const MAX_FRAME_PAYLOAD: usize = 1 << 20;

/// Request ID the gateway uses when replaying a miner's login on a new
/// engine; responses carrying it are consumed by the gateway.
pub const REPLAY_LOGIN_REQUEST_ID: u64 = u64::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    /// Engine → gateway, conn 0: JSON [`EngineHello`].
    Hello = 1,
    /// Gateway → engine, conn 0: [`GatewayRole`] name.
    Role = 2,
    /// Gateway → engine: a miner connected; payload is its `ip:port`.
    Open = 3,
    /// Both ways: one JSON-RPC line without the trailing newline.
    Line = 4,
    /// Both ways: the connection ended.
    Close = 5,
    /// Gateway → engine, conn 0: no connections remain; exit cleanly.
    Retire = 6,
}

impl FrameKind {
    fn from_byte(byte: u8) -> Result<Self> {
        Ok(match byte {
            1 => Self::Hello,
            2 => Self::Role,
            3 => Self::Open,
            4 => Self::Line,
            5 => Self::Close,
            6 => Self::Retire,
            other => bail!("unknown gateway frame kind {other}"),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub kind: FrameKind,
    pub conn: u64,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn new(kind: FrameKind, conn: u64, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            kind,
            conn,
            payload: payload.into(),
        }
    }

    pub fn payload_str(&self) -> Result<&str> {
        std::str::from_utf8(&self.payload)
            .map_err(|_| anyhow!("gateway frame payload is not UTF-8"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EngineHello {
    /// Instance name, e.g. the systemd instance (`a`/`b`).
    pub instance: String,
    pub pid: u32,
    pub started_at_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayRole {
    /// Receives new miners and owns pool-wide singletons (live snapshot).
    Primary,
    /// Finishing work for miners being moved to the primary.
    Draining,
}

impl GatewayRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Primary => "primary",
            Self::Draining => "draining",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "primary" => Ok(Self::Primary),
            "draining" => Ok(Self::Draining),
            other => bail!("unknown engine role {other:?}"),
        }
    }
}

/// Reads one frame; `None` on a clean end of stream between frames.
pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Option<Frame>> {
    let mut header = [0u8; 13];
    match reader.read_exact(&mut header[..1]).await {
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err.into()),
    }
    reader.read_exact(&mut header[1..]).await?;
    let kind = FrameKind::from_byte(header[0])?;
    let conn = u64::from_be_bytes(header[1..9].try_into().expect("8 bytes"));
    let len = u32::from_be_bytes(header[9..13].try_into().expect("4 bytes")) as usize;
    if len > MAX_FRAME_PAYLOAD {
        bail!("gateway frame payload of {len} bytes exceeds {MAX_FRAME_PAYLOAD}");
    }
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await?;
    Ok(Some(Frame {
        kind,
        conn,
        payload,
    }))
}

pub async fn write_frame<W: AsyncWrite + Unpin>(writer: &mut W, frame: &Frame) -> Result<()> {
    if frame.payload.len() > MAX_FRAME_PAYLOAD {
        bail!(
            "gateway frame payload of {} bytes exceeds {MAX_FRAME_PAYLOAD}",
            frame.payload.len()
        );
    }
    let mut buf = Vec::with_capacity(13 + frame.payload.len());
    buf.push(frame.kind as u8);
    buf.extend_from_slice(&frame.conn.to_be_bytes());
    buf.extend_from_slice(&(frame.payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(&frame.payload);
    writer.write_all(&buf).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn frames_round_trip_and_end_cleanly() {
        let frames = vec![
            Frame::new(FrameKind::Hello, 0, br#"{"instance":"a"}"#.to_vec()),
            Frame::new(FrameKind::Open, 7, "203.0.113.9:4444"),
            Frame::new(FrameKind::Line, 7, r#"{"id":1,"method":"login"}"#),
            Frame::new(FrameKind::Close, 7, Vec::new()),
            Frame::new(FrameKind::Retire, 0, Vec::new()),
        ];
        let mut wire = Vec::new();
        for frame in &frames {
            write_frame(&mut wire, frame).await.unwrap();
        }
        let mut reader = wire.as_slice();
        for frame in &frames {
            assert_eq!(read_frame(&mut reader).await.unwrap().as_ref(), Some(frame));
        }
        assert_eq!(read_frame(&mut reader).await.unwrap(), None);
    }

    #[tokio::test]
    async fn rejects_truncated_unknown_and_oversized_frames() {
        let mut wire = Vec::new();
        write_frame(&mut wire, &Frame::new(FrameKind::Line, 1, "abc"))
            .await
            .unwrap();
        let mut truncated = &wire[..wire.len() - 1];
        assert!(read_frame(&mut truncated).await.is_err());

        let mut unknown = wire.clone();
        unknown[0] = 99;
        assert!(read_frame(&mut unknown.as_slice()).await.is_err());

        let mut oversized = wire.clone();
        oversized[9..13].copy_from_slice(&((MAX_FRAME_PAYLOAD as u32) + 1).to_be_bytes());
        assert!(read_frame(&mut oversized.as_slice()).await.is_err());
    }
}
