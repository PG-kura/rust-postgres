use crate::config::Host;
use crate::keepalive::KeepaliveConfig;
use crate::{Error, Socket};
use socket2::{SockRef, TcpKeepalive};
use std::future::Future;
use std::io;
use std::time::Duration;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::net::{self, TcpStream};
use tokio::time;
use std::os::unix::io::AsRawFd;
use std::os::unix::prelude::RawFd;

pub(crate) async fn connect_socket(
    host: &Host,
    port: u16,
    connect_timeout: Option<Duration>,
    keepalive_config: Option<&KeepaliveConfig>,
) -> Result<(Socket, RawFd), Error> {
    match host {
        Host::Tcp(host) => {
            let addrs = net::lookup_host((&**host, port))
                .await
                .map_err(Error::connect)?;

            let mut last_err = None;

            for addr in addrs {
                use std::net::TcpStream as StdTcpStream;

                let fut = async {
                    let std_tcp_stream = StdTcpStream::connect(addr)?;
                    let fd = std_tcp_stream.as_raw_fd();
                    io::Result::Ok((TcpStream::from_std(std_tcp_stream)?, fd))
                };

                let (stream, fd) =
                    match connect_with_timeout(fut, connect_timeout).await {
                        Ok(stream) => stream,
                        Err(e) => {
                            last_err = Some(e);
                            continue;
                        }
                    };

                stream.set_nodelay(true).map_err(Error::connect)?;
                if let Some(keepalive_config) = keepalive_config {
                    SockRef::from(&stream)
                        .set_tcp_keepalive(&TcpKeepalive::from(keepalive_config))
                        .map_err(Error::connect)?;
                }

                return Ok((Socket::new_tcp(stream), fd));
            }

            Err(last_err.unwrap_or_else(|| {
                Error::connect(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "could not resolve any addresses",
                ))
            }))
        }
        #[cfg(unix)]
        Host::Unix(path) => {
            unreachable!();
        }
    }
}

async fn connect_with_timeout<F, T>(connect: F, timeout: Option<Duration>) -> Result<T, Error>
where
    F: Future<Output = io::Result<T>>,
{
    match timeout {
        Some(timeout) => match time::timeout(timeout, connect).await {
            Ok(Ok(socket)) => Ok(socket),
            Ok(Err(e)) => Err(Error::connect(e)),
            Err(_) => Err(Error::connect(io::Error::new(
                io::ErrorKind::TimedOut,
                "connection timed out",
            ))),
        },
        None => match connect.await {
            Ok(socket) => Ok(socket),
            Err(e) => Err(Error::connect(e)),
        },
    }
}
