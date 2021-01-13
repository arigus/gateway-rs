use crate::{error::Result, link_packet::LinkPacket, settings::Settings};
use semtech_udp::{
    server_runtime::{Error as SemtechError, Event, UdpRuntime},
    tx_ack,
};
use slog::{debug, info, o, warn, Logger};
use std::time::Duration;
use tokio::sync::mpsc::{Receiver, Sender};

pub const DOWNLINK_TIMEOUT_SECS: u64 = 5;
pub const UPLINK_TIMEOUT_SECS: u64 = 6;

#[derive(Debug)]
pub struct Gateway {
    uplinks: Sender<LinkPacket>,
    downlinks: Receiver<LinkPacket>,
    udp_runtime: UdpRuntime,
}

impl Gateway {
    pub async fn new(
        uplinks: Sender<LinkPacket>,
        downlinks: Receiver<LinkPacket>,
        settings: &Settings,
    ) -> Result<Self> {
        let gateway = Gateway {
            uplinks,
            downlinks,
            udp_runtime: UdpRuntime::new(settings.listen_addr).await?,
        };
        Ok(gateway)
    }

    pub async fn run(&mut self, shutdown: triggered::Listener, logger: &Logger) -> Result {
        let logger = logger.new(o!("module" => "gateway"));
        info!(logger, "starting gateway");
        loop {
            tokio::select! {
                _ = shutdown.clone() => {
                    info!(logger, "shutting down");
                    return Ok(())
                },
                event = self.udp_runtime.recv() =>
                    self.handle_udp_event(logger.clone(), event).await?,
                downlink = self.downlinks.recv() => match downlink {
                    Some(packet) => self.handle_downlink(logger.clone(), packet).await?,
                    None => {
                        debug!(logger, "ignoring closed downlinks channel");
                        continue;
                    }
                }
            }
        }
    }

    async fn handle_udp_event(&mut self, logger: Logger, event: Event) -> Result {
        match event {
            Event::UnableToParseUdpFrame(buf) => {
                info!(logger, "ignoring semtech udp parsing error for {:?}", buf)
            }
            Event::NewClient((mac, addr)) => {
                info!(logger, "new packet forwarder client: {}, {}", mac, addr)
            }
            Event::UpdateClient((mac, addr)) => {
                info!(logger, "mac existed, but IP updated: {}, {}", mac, addr)
            }
            Event::PacketReceived(rxpk, gateway_mac) => {
                match LinkPacket::from_push_data(&rxpk, gateway_mac) {
                    Ok(packet) if packet.is_longfi() => {
                        info!(logger, "ignoring longfi packet");
                    }
                    Ok(packet) => {
                        let _ = self.uplinks.send(packet).await;
                    }
                    Err(err) => {
                        debug!(logger, "ignoring push_data: {:?}", err);
                    }
                }
            }
            Event::NoClientWithMac(_packet, mac) => {
                warn!(logger, "send to client with unknown MAC: {:?}", mac)
            }
            Event::RawPacket(_) => (),
        };
        Ok(())
    }

    async fn handle_downlink(&mut self, logger: Logger, downlink: LinkPacket) -> Result {
        let (mut downlink_rx1, mut downlink_rx2) = (
            // first downlink
            self.udp_runtime
                .prepare_empty_downlink(downlink.gateway_mac),
            // 2nd downlink window if requested by the router response
            self.udp_runtime
                .prepare_empty_downlink(downlink.gateway_mac),
        );
        let pull_resp = downlink.to_pull_resp(false).unwrap();
        debug!(logger, "sending rx1 downlink {:?}", pull_resp);
        downlink_rx1.set_packet(pull_resp);
        match downlink_rx1
            .dispatch(Some(Duration::from_secs(DOWNLINK_TIMEOUT_SECS)))
            .await
        {
            // On a too early or too late error retry on the rx2 slot if available.
            Err(SemtechError::AckError(tx_ack::Error::TOO_EARLY))
            | Err(SemtechError::AckError(tx_ack::Error::TOO_LATE)) => {
                if let Some(pull_resp) = downlink.to_pull_resp(true) {
                    debug!(logger, "sending rx2 downlink {:?}", pull_resp);
                    downlink_rx2.set_packet(pull_resp);
                    match downlink_rx2
                        .dispatch(Some(Duration::from_secs(DOWNLINK_TIMEOUT_SECS)))
                        .await
                    {
                        Err(SemtechError::AckError(tx_ack::Error::NONE)) => Ok(()),
                        Err(err) => {
                            debug!(logger, "ignoring rx2 downlink error: {:?}", err);
                            Ok(())
                        }
                        Ok(()) => Ok(()),
                    }
                } else {
                    Ok(())
                }
            }
            Err(SemtechError::AckError(tx_ack::Error::NONE)) => Ok(()),
            Err(err) => {
                debug!(logger, "ignoring rx1 downlink error: {:?}", err);
                Ok(())
            }
            Ok(()) => Ok(()),
        }
    }
}
