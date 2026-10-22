//! Opt-in STUN tap on the existing IP receive loop. No extra UDP socket/reader.

use std::{
    io,
    net::SocketAddr,
    sync::{Arc, Weak},
};

use bytes::Bytes;
use netwatch::UdpSocket;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

/// A STUN datagram received by one of iroh's IP sockets.
#[derive(Debug, Clone)]
pub struct StunDatagram {
    /// Canonical (non IPv4-mapped) source address.
    pub from: SocketAddr,
    /// Complete UDP payload, including the STUN header.
    pub bytes: Bytes,
}

/// Send STUN and subscribe to responses on one existing iroh UDP socket.
///
/// This is an experimental extension in the pinned repository fork. Receivers
/// are bounded; lag must be treated as a failed probe, never a valid observation.
/// A handle holds a weak socket reference and cannot keep the endpoint alive.
#[derive(Debug, Clone)]
pub struct StunSocket {
    socket: Weak<UdpSocket>,
    packets: broadcast::Sender<StunDatagram>,
    closed: CancellationToken,
}

impl StunSocket {
    pub(crate) fn new(
        socket: &Arc<UdpSocket>,
        packets: broadcast::Sender<StunDatagram>,
        closed: CancellationToken,
    ) -> Self {
        Self {
            socket: Arc::downgrade(socket),
            packets,
            closed,
        }
    }

    fn socket(&self) -> io::Result<Arc<UdpSocket>> {
        if self.closed.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "iroh endpoint closed",
            ));
        }
        self.socket
            .upgrade()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "iroh IP socket dropped"))
    }

    /// Current local binding of the exact UDP socket used for sending.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket()?.local_addr()
    }

    /// Subscribe before sending so even an immediate response cannot be lost.
    pub fn subscribe(&self) -> broadcast::Receiver<StunDatagram> {
        self.packets.subscribe()
    }

    /// Wait for endpoint closure to start.
    pub async fn closed(&self) {
        self.closed.cancelled().await;
    }

    /// Send one STUN datagram without adding a competing reader.
    pub async fn send_to(&self, packet: &[u8], server: SocketAddr) -> io::Result<usize> {
        if !is_stun(packet) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "not a STUN packet",
            ));
        }
        let socket = self.socket()?;
        // Do not poll write-readiness independently of QUIC: netwatch's
        // poll_send_ready stores a single waker. STUN is infrequent, so use
        // nonblocking sends and a short cancellable retry on backpressure.
        let sender = socket.create_sender();
        let transmit = noq_udp::Transmit {
            destination: server,
            contents: packet,
            ecn: None,
            segment_size: None,
            src_ip: None,
        };
        loop {
            if self.closed.is_cancelled() {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "iroh endpoint closed",
                ));
            }
            match sender.try_send(&transmit) {
                Ok(()) => return Ok(packet.len()),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    tokio::select! {
                        biased;
                        _ = self.closed() => return Err(io::Error::new(io::ErrorKind::NotConnected, "iroh endpoint closed")),
                        _ = tokio::time::sleep(std::time::Duration::from_millis(1)) => {},
                    }
                }
                Err(error) => return Err(error),
            }
        }
    }
}

/// RFC 8489 framing discriminator. The QUIC fixed bit is never set by STUN.
pub(super) fn is_stun(packet: &[u8]) -> bool {
    packet.len() >= 20 && packet[0] & 0xc0 == 0 && packet[4..8] == [0x21, 0x12, 0xa4, 0x42]
}
