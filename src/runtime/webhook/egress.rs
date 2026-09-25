use crate::runtime::error::RuntimeError;
use crate::runtime::gateway::GatewayFlowLease;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::task::JoinHandle;

pub(in crate::runtime) trait WebhookIo:
    AsyncRead + AsyncWrite + Unpin + Send
{
}

impl<T> WebhookIo for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

/// One committed webhook connector branch. MPP relays are cancelled when the
/// request owner drops, and the optional balancer lease accounts only for load.
pub(in crate::runtime) struct OpenedWebhookStream {
    io: Box<dyn WebhookIo>,
    relay: Option<JoinHandle<Result<(), RuntimeError>>>,
    _gateway_lease: Option<GatewayFlowLease>,
}

impl OpenedWebhookStream {
    pub(in crate::runtime) fn new(
        io: Box<dyn WebhookIo>,
        relay: Option<JoinHandle<Result<(), RuntimeError>>>,
        gateway_lease: Option<GatewayFlowLease>,
    ) -> Self {
        Self {
            io,
            relay,
            _gateway_lease: gateway_lease,
        }
    }
}

impl AsyncRead for OpenedWebhookStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.io).poll_read(context, buffer)
    }
}

impl AsyncWrite for OpenedWebhookStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.io).poll_write(context, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.io).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut *self.io).poll_shutdown(context)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut *self.io).poll_write_vectored(context, buffers)
    }
}

impl Drop for OpenedWebhookStream {
    fn drop(&mut self) {
        if let Some(relay) = self.relay.take() {
            relay.abort();
        }
    }
}

pub(in crate::runtime) struct WebhookOpenError {
    pub(in crate::runtime) retryable: bool,
    pub(in crate::runtime) stage: &'static str,
}

impl WebhookOpenError {
    pub(in crate::runtime) const fn transport(stage: &'static str) -> Self {
        Self {
            retryable: true,
            stage,
        }
    }

    pub(in crate::runtime) const fn permanent(stage: &'static str) -> Self {
        Self {
            retryable: false,
            stage,
        }
    }
}
