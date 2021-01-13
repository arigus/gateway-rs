use crate::{error::Result, keypair, link_packet::LinkPacket, settings::Settings};
use helium_proto::{
    services::{self, Channel, Endpoint},
    BlockchainStateChannelMessageV1, RoutingInformation, RoutingRequest, RoutingResponse,
};
use http::Uri;
use slog::{debug, info, o, warn, Logger};
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc::{Receiver, Sender};

pub mod filter;
pub mod routing;

pub const CONNECT_TIMEOUT: u64 = 10;

#[derive(Debug, Clone)]
pub struct Message(BlockchainStateChannelMessageV1);

#[derive(Debug, Clone)]
pub struct Response(BlockchainStateChannelMessageV1);

pub use helium_proto::Region;

pub type RouterClient = services::router::Client<Channel>;
pub type ValidatorClient = services::validator::Client<Channel>;

pub fn mk_router_client(uri: Uri) -> Result<RouterClient> {
    let channel = Endpoint::from(uri)
        .timeout(Duration::from_secs(CONNECT_TIMEOUT))
        .connect_lazy()?;
    Ok(RouterClient::new(channel))
}

pub fn mk_validator_client(uri: Uri) -> Result<ValidatorClient> {
    let channel = Endpoint::from(uri)
        .timeout(Duration::from_secs(CONNECT_TIMEOUT))
        .connect_lazy()?;
    Ok(ValidatorClient::new(channel))
}

pub struct Router {
    downlinks: Sender<LinkPacket>,
    uplinks: Receiver<LinkPacket>,
    keypair: Arc<keypair::Keypair>,
    region: Region,
    validator: ValidatorClient,
    routing_height: u64,
    clients: HashMap<u32, routing::Routing>,
    default_clients: Vec<RouterClient>,
}

impl Router {
    pub fn new(
        downlinks: Sender<LinkPacket>,
        uplinks: Receiver<LinkPacket>,
        settings: &Settings,
    ) -> Result<Self> {
        let validator = mk_validator_client(settings.validator.clone())?;
        let default_clients: Vec<RouterClient> = settings
            .routers
            .iter()
            .map(|uri| mk_router_client(uri.clone()))
            .collect::<Result<Vec<RouterClient>>>()?;
        Ok(Self {
            keypair: settings.keypair.clone(),
            region: settings.region,
            uplinks,
            downlinks,
            validator,
            routing_height: 0,
            clients: HashMap::new(),
            default_clients,
        })
    }

    pub async fn run(&mut self, shutdown: triggered::Listener, logger: &Logger) -> Result {
        let logger = logger.new(o!("module" => "router"));
        info!(logger, "starting router");
        let mut routing_stream = self.routing_stream().await?;
        loop {
            tokio::select! {
                _ = shutdown.clone() => {
                    info!(logger.clone(), "shutting down");
                    return Ok(())
                },
                routing = routing_stream.message() => match routing {
                    Ok(Some(routing_response)) => self.handle_routing_update(logger.clone(), &routing_response),
                    Ok(None) => {
                        info!(logger.clone(), "NO ROUTING RESPONSE?")
                    },
                    Err(err) => {
                        //self.validator = mk_validator_client(self.validator.uri.clone())?;
                        info!(logger.clone(), "ROUTING ERROR {:?}", err);
                        panic!("ERROR {:?}", err)
                    }
                },
                uplink = self.uplinks.recv() => match uplink {
                    Some(packet) => match self.handle_uplink(logger.clone(), packet).await {
                        Ok(()) => (),
                        Err(err) => debug!(logger, "ignoring failed uplink {:?}", err)
                    },
                    None => debug!(logger, "ignoring closed downlinks channel"),
                },
            }
        }
    }

    fn handle_routing_update(&mut self, logger: Logger, routing_response: &RoutingResponse) {
        if routing_response.height <= self.routing_height {
            warn!(
                logger,
                "router returned invalid height {:?} while at {:?}",
                routing_response.height,
                self.routing_height
            )
        }
        for routing in &routing_response.routings {
            match routing::Routing::from_proto(routing) {
                Ok(client) => {
                    self.clients.insert(routing.oui, client);
                    ()
                }
                Err(err) => warn!(logger, "failed to construct router client: {:?}", err),
            }
        }
        self.routing_height = routing_response.height;
        info!(
            logger,
            "updated routing to height {:?}", self.routing_height
        )
    }

    async fn handle_uplink(&mut self, logger: Logger, uplink: LinkPacket) -> Result {
        if uplink.packet.routing.is_none() {
            debug!(logger, "ignoring, no routing data");
            return Ok(());
        };
        let gateway_mac = uplink.gateway_mac;
        let message = uplink.to_state_channel_message(&self.keypair, self.region)?;
        for mut client in self.router_clients_for_uplink(&uplink) {
            let mut downlinks = self.downlinks.clone();
            let message = message.clone();
            let logger = logger.clone();
            tokio::spawn(async move {
                match client.route(message).await {
                    Ok(response) => {
                        if let Some(downlink) = LinkPacket::from_state_channel_message(
                            response.into_inner(),
                            gateway_mac,
                        ) {
                            match downlinks.send(downlink).await {
                                Ok(()) => (),
                                Err(_) => {
                                    debug!(logger, "failed to push downlink")
                                }
                            }
                        }
                    }
                    Err(err) => debug!(logger, "ignoring uplink error: {:?}", err),
                }
            });
        }
        Ok(())
    }

    fn router_clients_for_uplink(&self, uplink: &LinkPacket) -> Vec<RouterClient> {
        match &uplink.packet.routing {
            Some(RoutingInformation {
                data: Some(routing_data),
            }) => {
                let found: Vec<RouterClient> = self
                    .clients
                    .values()
                    .filter(|&routing| routing.matches_routing_data(&routing_data))
                    .flat_map(|routing| routing.clients.clone())
                    .collect();
                if found.is_empty() {
                    self.default_clients.clone()
                } else {
                    found
                }
            }
            _ => vec![],
        }
    }

    async fn routing_stream(&mut self) -> Result<tonic::codec::Streaming<RoutingResponse>> {
        let stream = self
            .validator
            .routing(RoutingRequest { height: 1 })
            .await?
            .into_inner();
        Ok(stream)
    }
}
