use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::BoltError;

const MAX_CHUNK_SIZE: usize = 65535;
const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
const READ_TIMEOUT: Duration = Duration::from_secs(30);

async fn read_exact_with_timeout<R: AsyncRead + Unpin>(
    reader: &mut R,
    buf: &mut [u8],
) -> Result<(), BoltError> {
    match tokio::time::timeout(READ_TIMEOUT, reader.read_exact(buf)).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            Err(BoltError::ConnectionClosed)
        }
        Ok(Err(e)) => Err(BoltError::Io(e)),
        Err(_) => Err(BoltError::Protocol("read timeout".into())),
    }
}

pub async fn read_message<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Bytes, BoltError> {
    let mut buf = BytesMut::new();
    loop {
        let mut len_bytes = [0u8; 2];
        read_exact_with_timeout(reader, &mut len_bytes).await?;
        let chunk_len = u16::from_be_bytes(len_bytes) as usize;
        if chunk_len == 0 {
            return Ok(buf.freeze());
        }
        let mut chunk = vec![0u8; chunk_len];
        read_exact_with_timeout(reader, &mut chunk).await?;
        buf.put_slice(&chunk);
        if buf.len() > MAX_MESSAGE_SIZE {
            return Err(BoltError::Protocol("message exceeds 16 MB limit".into()));
        }
    }
}

pub async fn write_message<W: AsyncWrite + Unpin>(
    writer: &mut W,
    data: &[u8],
) -> Result<(), BoltError> {
    for chunk in data.chunks(MAX_CHUNK_SIZE) {
        let len = chunk.len() as u16;
        writer.write_all(&len.to_be_bytes()).await?;
        writer.write_all(chunk).await?;
    }
    writer.write_all(&[0x00, 0x00]).await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn single_chunk_round_trip() {
        let payload = b"hello bolt";
        let (mut client, mut server) = tokio::io::duplex(1024);

        let write_handle = tokio::spawn(async move {
            write_message(&mut client, payload).await.unwrap();
        });

        let result = read_message(&mut server).await.unwrap();
        write_handle.await.unwrap();
        assert_eq!(&result[..], payload);
    }

    #[tokio::test]
    async fn multi_chunk_message() {
        let payload = vec![0xABu8; 100_000];
        let (mut client, mut server) = tokio::io::duplex(256 * 1024);

        let expected = payload.clone();
        let write_handle = tokio::spawn(async move {
            write_message(&mut client, &payload).await.unwrap();
        });

        let result = read_message(&mut server).await.unwrap();
        write_handle.await.unwrap();
        assert_eq!(result.len(), 100_000);
        assert_eq!(&result[..], &expected[..]);
    }

    #[tokio::test]
    async fn empty_message() {
        let (mut client, mut server) = tokio::io::duplex(1024);

        let write_handle = tokio::spawn(async move {
            write_message(&mut client, &[]).await.unwrap();
        });

        let result = read_message(&mut server).await.unwrap();
        write_handle.await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn connection_closed_mid_read() {
        let (client, mut server) = tokio::io::duplex(1024);
        drop(client);

        let result = read_message(&mut server).await;
        assert!(matches!(result, Err(BoltError::ConnectionClosed)));
    }

    #[tokio::test]
    async fn multi_chunk_boundary_exact() {
        let payload = vec![0xFFu8; MAX_CHUNK_SIZE * 2];
        let (mut client, mut server) = tokio::io::duplex(256 * 1024);

        let expected = payload.clone();
        let write_handle = tokio::spawn(async move {
            write_message(&mut client, &payload).await.unwrap();
        });

        let result = read_message(&mut server).await.unwrap();
        write_handle.await.unwrap();
        assert_eq!(result.len(), MAX_CHUNK_SIZE * 2);
        assert_eq!(&result[..], &expected[..]);
    }

    #[tokio::test]
    async fn write_produces_correct_framing() {
        let payload = b"test";
        let (mut client, mut server) = tokio::io::duplex(1024);
        write_message(&mut client, payload).await.unwrap();
        drop(client);

        let mut raw = Vec::new();
        server.read_to_end(&mut raw).await.unwrap();

        assert_eq!(&raw[0..2], &[0x00, 0x04]);
        assert_eq!(&raw[2..6], b"test");
        assert_eq!(&raw[6..8], &[0x00, 0x00]);
    }

    #[tokio::test]
    async fn sequential_messages() {
        let (mut client, mut server) = tokio::io::duplex(1024);

        let write_handle = tokio::spawn(async move {
            write_message(&mut client, b"first").await.unwrap();
            write_message(&mut client, b"second").await.unwrap();
        });

        let msg1 = read_message(&mut server).await.unwrap();
        let msg2 = read_message(&mut server).await.unwrap();
        write_handle.await.unwrap();

        assert_eq!(&msg1[..], b"first");
        assert_eq!(&msg2[..], b"second");
    }
}
