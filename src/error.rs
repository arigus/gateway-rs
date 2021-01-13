use std::net;
use thiserror::Error;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("config error")]
    ConfigError(#[from] config::ConfigError),
    #[error("server error")]
    ServerError(String),
    #[error("http error")]
    HttpError(#[from] http::Error),
    #[error("uri error")]
    UriError(#[from] http::uri::InvalidUri),
    #[error("io error")]
    IOError(#[from] std::io::Error),
    #[error("longfi error")]
    LfcError(#[from] longfi::LfcError),
    #[error("ed25519 error")]
    ED2519Error(#[from] ed25519_dalek::ed25519::Error),
    #[error("base64 decode error")]
    Base64Error(#[from] base64::DecodeError),
    #[error("json error")]
    JSONError(#[from] serde_json::Error),
    #[error("services error")]
    ServiceError(#[from] helium_proto::services::Error),
    #[error("rpc error")]
    RpcError(#[from] tonic::Status),
    #[error("protobuf error")]
    ProstError(#[from] prost::EncodeError),
}

impl From<net::AddrParseError> for Error {
    fn from(v: net::AddrParseError) -> Self {
        Self::ServerError(v.to_string())
    }
}

impl From<tokio::sync::broadcast::RecvError> for Error {
    fn from(v: tokio::sync::broadcast::RecvError) -> Self {
        Self::ServerError(v.to_string())
    }
}

impl From<lorawan::LoraWanError> for Error {
    fn from(v: lorawan::LoraWanError) -> Self {
        Self::ServerError(v.to_string())
    }
}

impl From<semtech_udp::server_runtime::Error> for Error {
    fn from(v: semtech_udp::server_runtime::Error) -> Self {
        Self::ServerError(v.to_string())
    }
}

impl From<prost::DecodeError> for Error {
    fn from(v: prost::DecodeError) -> Self {
        Self::ServerError(v.to_string())
    }
}

impl From<daemonize::DaemonizeError> for Error {
    fn from(v: daemonize::DaemonizeError) -> Self {
        Self::ServerError(v.to_string())
    }
}
