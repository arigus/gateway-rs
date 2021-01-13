use crate::router::{
    filter::{DevAddrFilter, EuiFilter},
    mk_router_client, RouterClient,
};
use helium_proto::routing_information::Data as RoutingData;
use http::Uri;
use std::convert::TryFrom;

pub struct Routing {
    pub(crate) filters: Vec<EuiFilter>,
    pub(crate) subnets: Vec<DevAddrFilter>,
    pub(crate) clients: Vec<RouterClient>,
}

impl Routing {
    pub fn matches_routing_data(&self, routing_data: &RoutingData) -> bool {
        match routing_data {
            RoutingData::Eui(eui) => self.filters.iter().any(|filter| filter.contains(&eui)),
            RoutingData::Devaddr(dev_addr) => {
                self.subnets.iter().any(|filter| filter.contains(dev_addr))
            }
        }
    }
}

impl From<&helium_proto::Routing> for Routing {
    fn from(r: &helium_proto::Routing) -> Self {
        let filters = r.filters.iter().map(|f| EuiFilter::from_bin(&f)).collect();
        let subnets = r
            .subnets
            .iter()
            .map(|s| DevAddrFilter::from_bin(&s))
            .collect();
        Self {
            filters,
            subnets,
            clients: r
                .addresses
                .iter()
                .filter_map(|address| match Uri::try_from(&address.uri[..]) {
                    Ok(uri) => match mk_router_client(uri.clone()) {
                        Ok(client) => {
                            log::info!("made client for uri {:?}", uri);
                            Some(client)
                        }
                        Err(err) => {
                            log::warn!("failed to make client for uri {:?}: {:?}", uri, err);
                            None
                        }
                    },
                    Err(err) => {
                        log::warn!("invalid uri {:?}", err);
                        None
                    }
                })
                .collect(),
        }
    }
}
