//! Tunnel-server targets and DNS discovery.

use std::collections::HashSet;
use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use futures_util::future::join_all;
use hickory_resolver::net::NetError;
use hickory_resolver::proto::rr::RData;
use hickory_resolver::TokioResolver;

use crate::cloud::ServerAddress;

pub(crate) const DNS_REFRESH_FLOOR: Duration = Duration::from_secs(1);
pub(crate) const DNS_REFRESH_CAP: Duration = Duration::from_secs(30);
pub(crate) const DNS_REFRESH_FALLBACK: Duration = Duration::from_secs(30);

/// How the tunnel server set is discovered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Discovery {
    /// Discover Cloud tunnel servers using an SRV query.
    Srv(String),
    /// A fixed set of explicitly configured tunnel servers.
    Explicit(Vec<Target>),
}

impl From<crate::cloud::Discovery> for Discovery {
    fn from(discovery: crate::cloud::Discovery) -> Self {
        match discovery {
            crate::cloud::Discovery::Srv(name) => Self::Srv(name),
            crate::cloud::Discovery::Explicit(servers) => {
                Self::Explicit(servers.iter().map(Target::from_address).collect())
            }
        }
    }
}

/// The network host of a tunnel target.
///
/// SRV-discovered hosts are IP addresses. Explicit hostnames intentionally
/// remain unresolved so a new dial can observe ordinary A/AAAA changes without
/// changing the fixed target set.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum TargetHost {
    Ip(IpAddr),
    Name(String),
}

impl TargetHost {
    pub(crate) fn as_str(&self) -> String {
        match self {
            Self::Ip(ip) => ip.to_string(),
            Self::Name(name) => name.clone(),
        }
    }
}

/// A dialable tunnel server and all metadata that affects its connection.
///
/// `Eq` and `Hash` deliberately cover SNI and plaintext in addition to the
/// address. A DNS refresh that changes TLS metadata must replace the old slot.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct Target {
    host: TargetHost,
    port: u16,
    server_name: String,
    plaintext: bool,
}

impl Target {
    fn new(host: TargetHost, port: u16, server_name: String, plaintext: bool) -> Self {
        Self {
            host,
            port,
            server_name,
            plaintext,
        }
    }

    fn from_address(address: &ServerAddress) -> Self {
        let host = parse_host(&address.host);
        let server_name = host.as_str();
        Self::new(host, address.port, server_name, address.plaintext)
    }

    pub(crate) fn host(&self) -> &TargetHost {
        &self.host
    }

    pub(crate) fn port(&self) -> u16 {
        self.port
    }

    /// TLS certificate-verification name and SNI value.
    pub(crate) fn server_name(&self) -> &str {
        &self.server_name
    }

    pub(crate) fn is_plaintext(&self) -> bool {
        self.plaintext
    }

    pub(crate) fn socket_addr(&self) -> Option<SocketAddr> {
        match self.host {
            TargetHost::Ip(ip) => Some(SocketAddr::new(ip, self.port)),
            TargetHost::Name(_) => None,
        }
    }
}

impl fmt::Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.host {
            TargetHost::Ip(ip) => SocketAddr::new(*ip, self.port).fmt(f),
            TargetHost::Name(host) => write!(f, "{host}:{}", self.port),
        }
    }
}

fn parse_host(host: &str) -> TargetHost {
    let unbracketed = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    match unbracketed.parse() {
        Ok(ip) => TargetHost::Ip(ip),
        Err(_) => TargetHost::Name(unbracketed.to_owned()),
    }
}

/// The result of one discovery refresh.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Resolution {
    pub(crate) targets: Vec<Target>,
    /// `None` for a fixed explicit target set.
    pub(crate) refresh_after: Option<Duration>,
}

/// System DNS resolver used by both SRV refreshes and explicit hostname dials.
#[derive(Clone)]
pub(crate) struct TargetResolver {
    inner: TokioResolver,
}

impl TargetResolver {
    pub(crate) fn system() -> Result<Self, ResolveError> {
        let builder = TokioResolver::builder_tokio().map_err(ResolveError::Dns)?;
        let inner = builder.build().map_err(ResolveError::Dns)?;
        Ok(Self { inner })
    }

    /// Resolve the current target set.
    ///
    /// An authoritative no-records response becomes a successful empty set,
    /// allowing the supervisor to reconcile old slots away. Transport and
    /// resolver failures remain errors, so the supervisor can retain healthy
    /// existing slots and retry the refresh.
    pub(crate) async fn resolve(&self, discovery: &Discovery) -> Result<Resolution, ResolveError> {
        match discovery {
            Discovery::Explicit(targets) => Ok(Resolution {
                targets: targets.clone(),
                refresh_after: None,
            }),
            Discovery::Srv(name) => self.resolve_srv(name).await,
        }
    }

    /// Resolve all A and AAAA addresses to try for one dial.
    pub(crate) async fn resolve_dial_addresses(
        &self,
        target: &Target,
    ) -> Result<Vec<SocketAddr>, ResolveError> {
        if let Some(address) = target.socket_addr() {
            return Ok(vec![address]);
        }

        let host = match target.host() {
            TargetHost::Name(host) => host,
            TargetHost::Ip(_) => unreachable!("IP targets returned above"),
        };
        let lookup = self
            .inner
            .lookup_ip(as_fqdn(host))
            .await
            .map_err(ResolveError::Dns)?;
        let mut seen = HashSet::new();
        let addresses = lookup
            .iter()
            .map(|ip| SocketAddr::new(ip, target.port()))
            .filter(|address| seen.insert(*address))
            .collect::<Vec<_>>();
        if addresses.is_empty() {
            return Err(ResolveError::NoAddresses(host.clone()));
        }
        Ok(addresses)
    }

    async fn resolve_srv(&self, query_name: &str) -> Result<Resolution, ResolveError> {
        let lookup = match self.inner.srv_lookup(as_fqdn(query_name)).await {
            Ok(lookup) => lookup,
            Err(error) if error.is_no_records_found() => {
                return Ok(Resolution {
                    targets: Vec::new(),
                    refresh_after: Some(refresh_for_negative(&error)),
                });
            }
            Err(error) => return Err(ResolveError::Dns(error)),
        };

        let now = Instant::now();
        let mut refresh_deadline = lookup.valid_until();
        let mut records = lookup
            .answers()
            .iter()
            .filter_map(|record| match &record.data {
                RData::SRV(srv) => Some(SrvRecord {
                    priority: srv.priority,
                    weight: srv.weight,
                    port: srv.port,
                    host: srv.target.to_utf8().trim_end_matches('.').to_owned(),
                }),
                _ => None,
            })
            // A target of `.` explicitly says that the service is unavailable.
            .filter(|record| !record.host.is_empty())
            .collect::<Vec<_>>();
        records.sort_by_key(|record| (record.priority, std::cmp::Reverse(record.weight)));

        let lookups = join_all(records.into_iter().map(|record| async move {
            let result = self.inner.lookup_ip(as_fqdn(&record.host)).await;
            (record, result)
        }))
        .await;

        let mut resolved = Vec::with_capacity(lookups.len());
        for (record, result) in lookups {
            match result {
                Ok(addresses) => {
                    refresh_deadline = refresh_deadline.min(addresses.valid_until());
                    resolved.push((record, addresses.iter().collect::<Vec<_>>()));
                }
                Err(error) if error.is_no_records_found() => {
                    // This SRV target was authoritatively removed. Other records
                    // remain usable and the negative TTL still informs refresh.
                    refresh_deadline = refresh_deadline.min(now + refresh_for_negative(&error));
                    resolved.push((record, Vec::new()));
                }
                Err(error) => return Err(ResolveError::Dns(error)),
            }
        }

        let targets = expand_srv_addresses(query_name, resolved);
        Ok(Resolution {
            targets,
            refresh_after: Some(clamp_refresh(
                refresh_deadline.saturating_duration_since(now),
            )),
        })
    }
}

fn as_fqdn(name: &str) -> String {
    if name.ends_with('.') {
        name.to_owned()
    } else {
        format!("{name}.")
    }
}

#[derive(Clone, Debug)]
struct SrvRecord {
    priority: u16,
    weight: u16,
    port: u16,
    host: String,
}

fn expand_srv_addresses(
    query_name: &str,
    resolved: impl IntoIterator<Item = (SrvRecord, Vec<IpAddr>)>,
) -> Vec<Target> {
    let mut seen = HashSet::new();
    let mut targets = Vec::new();
    for (record, addresses) in resolved {
        for address in addresses {
            let target = Target::new(
                TargetHost::Ip(address),
                record.port,
                query_name.to_owned(),
                false,
            );
            if seen.insert(target.clone()) {
                targets.push(target);
            }
        }
    }
    targets
}

fn refresh_for_negative(error: &NetError) -> Duration {
    use hickory_resolver::net::DnsError;

    let ttl = match error {
        NetError::Dns(DnsError::NoRecordsFound(no_records)) => no_records
            .negative_ttl
            .map(|ttl| Duration::from_secs(ttl.into())),
        _ => None,
    };
    clamp_refresh(ttl.unwrap_or(DNS_REFRESH_FALLBACK))
}

fn clamp_refresh(ttl: Duration) -> Duration {
    ttl.clamp(DNS_REFRESH_FLOOR, DNS_REFRESH_CAP)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ResolveError {
    #[error("DNS resolution failed: {0}")]
    Dns(#[source] NetError),
    #[error("hostname {0:?} has no A or AAAA addresses")]
    NoAddresses(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn converts_explicit_servers() {
        let target = Target::from_address(&ServerAddress {
            host: "example.com".into(),
            port: 9080,
            plaintext: false,
        });
        assert_eq!(target.host(), &TargetHost::Name("example.com".into()));
        assert_eq!(target.port(), 9080);
        assert_eq!(target.server_name(), "example.com");
        assert!(!target.is_plaintext());

        let target = Target::from_address(&ServerAddress {
            host: "127.0.0.1".into(),
            port: 19080,
            plaintext: true,
        });
        assert_eq!(
            target.socket_addr(),
            Some("127.0.0.1:19080".parse().unwrap())
        );
        assert!(target.is_plaintext());

        let target = Target::from_address(&ServerAddress {
            host: "::1".into(),
            port: 9080,
            plaintext: false,
        });
        assert_eq!(target.socket_addr(), Some("[::1]:9080".parse().unwrap()));
    }

    #[test]
    fn expands_every_address_and_deduplicates_complete_targets() {
        let record_a = SrvRecord {
            priority: 0,
            weight: 5,
            port: 19080,
            host: "node-a.internal".into(),
        };
        let record_b = SrvRecord {
            priority: 10,
            weight: 1,
            port: 19081,
            host: "node-b.internal".into(),
        };
        let v4 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
        let v6 = IpAddr::V6(Ipv6Addr::LOCALHOST);

        let targets = expand_srv_addresses(
            "tunnel.eu.restate.cloud",
            [
                (record_a.clone(), vec![v4, v6, v4]),
                (record_a, vec![v4]),
                (record_b, vec![v4]),
            ],
        );

        assert_eq!(targets.len(), 3);
        assert_eq!(targets[0].socket_addr(), Some(SocketAddr::new(v4, 19080)));
        assert_eq!(targets[1].socket_addr(), Some(SocketAddr::new(v6, 19080)));
        assert_eq!(targets[2].socket_addr(), Some(SocketAddr::new(v4, 19081)));
        assert!(targets
            .iter()
            .all(|target| target.server_name() == "tunnel.eu.restate.cloud"));
    }

    #[test]
    fn target_identity_includes_tls_metadata() {
        let base = Target::new(
            TargetHost::Ip(Ipv4Addr::LOCALHOST.into()),
            443,
            "one.example".into(),
            false,
        );
        let different_sni = Target::new(base.host.clone(), base.port, "two.example".into(), false);
        let plaintext = Target::new(base.host.clone(), base.port, base.server_name.clone(), true);
        let set = HashSet::from([base, different_sni, plaintext]);
        assert_eq!(set.len(), 3);
    }

    #[test]
    fn refresh_ttl_has_a_nonzero_floor_and_thirty_second_cap() {
        assert_eq!(clamp_refresh(Duration::ZERO), DNS_REFRESH_FLOOR);
        assert_eq!(
            clamp_refresh(Duration::from_secs(7)),
            Duration::from_secs(7)
        );
        assert_eq!(clamp_refresh(Duration::from_secs(300)), DNS_REFRESH_CAP);
    }

    #[test]
    fn authoritative_empty_and_transport_errors_have_distinct_taxonomy() {
        use hickory_resolver::net::NoRecords;
        use hickory_resolver::proto::op::{Query, ResponseCode};
        use hickory_resolver::proto::rr::{Name, RecordType};

        let query = Query::query(Name::from_ascii("missing.example.").unwrap(), RecordType::A);
        let mut no_records = NoRecords::new(query, ResponseCode::NXDomain);
        no_records.negative_ttl = Some(7);
        let authoritative_empty = NetError::from(no_records);
        assert!(authoritative_empty.is_no_records_found());
        assert_eq!(
            refresh_for_negative(&authoritative_empty),
            Duration::from_secs(7)
        );

        let transport = NetError::Timeout;
        assert!(!transport.is_no_records_found());
        assert_eq!(refresh_for_negative(&transport), DNS_REFRESH_FALLBACK);
    }
}
