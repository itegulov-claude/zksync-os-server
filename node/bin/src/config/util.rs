use alloy::primitives::B256;
use alloy::signers::k256::ecdsa::SigningKey;
use reth_net_nat::net_if::resolve_net_if_ip;
use serde::Deserialize;
use serde_json::Value;
use smart_config::ErrorWithOrigin;
use smart_config::de::{DeserializeContext, DeserializeParam};
use smart_config::metadata::{BasicTypes, ParamMetadata};
use std::net::{IpAddr, Ipv4Addr};
use zksync_os_network::SecretKey;
use zksync_os_operator_signer::SignerConfig;

/// Custom deserializer for [`SignerConfig`].
///
/// Accepts either:
/// - A hex string (with or without `0x` prefix): parsed as a local private key (backward-compatible)
/// - An object `{"type": "gcp_kms", "resource": "projects/..."}`: a GCP KMS key
#[derive(Debug)]
pub struct SignerConfigDeserializer;

impl DeserializeParam<SignerConfig> for SignerConfigDeserializer {
    const EXPECTING: BasicTypes = BasicTypes::STRING.or(BasicTypes::OBJECT);

    fn deserialize_param(
        &self,
        ctx: DeserializeContext<'_>,
        param: &'static ParamMetadata,
    ) -> Result<SignerConfig, ErrorWithOrigin> {
        let deserializer = ctx.current_value_deserializer(param.name)?;
        let value = Value::deserialize(deserializer)?;

        match value {
            Value::String(s) => {
                // Backward-compatible: plain hex string = local private key
                let b256: B256 =
                    serde_json::from_value(Value::String(s)).map_err(ErrorWithOrigin::custom)?;
                let sk =
                    SigningKey::from_slice(b256.as_slice()).map_err(ErrorWithOrigin::custom)?;
                Ok(SignerConfig::Local(sk))
            }
            Value::Object(obj) => {
                let type_str = obj.get("type").and_then(|v| v.as_str()).ok_or_else(|| {
                    ErrorWithOrigin::custom("missing 'type' field in signer config")
                })?;
                match type_str {
                    "gcp_kms" => {
                        let resource =
                            obj.get("resource")
                                .and_then(|v| v.as_str())
                                .ok_or_else(|| {
                                    ErrorWithOrigin::custom(
                                        "missing 'resource' field in gcp_kms signer config",
                                    )
                                })?;
                        Ok(SignerConfig::gcp_kms(resource.to_string()))
                    }
                    other => Err(ErrorWithOrigin::custom(format!(
                        "unknown signer type '{other}', expected 'gcp_kms'"
                    ))),
                }
            }
            _ => Err(ErrorWithOrigin::custom(
                "expected a hex string (local key) or an object with 'type' field",
            )),
        }
    }

    fn serialize_param(&self, param: &SignerConfig) -> Value {
        match param {
            SignerConfig::Local(sk) => {
                let bytes = B256::from_slice(sk.to_bytes().as_slice());
                serde_json::to_value(bytes).expect("failed serializing to JSON")
            }
            SignerConfig::GcpKms { resource_name, .. } => {
                serde_json::json!({"type": "gcp_kms", "resource": resource_name})
            }
        }
    }
}

/// Custom deserializer for `secp256k1::SecretKey`.
///
/// Accepts hex strings both with and without `0x` prefix.
/// The built-in `secp256k1` string parser does not support the `0x` prefix,
/// so we go through `B256` (which uses `const-hex` and strips the prefix automatically).
#[derive(Debug)]
pub struct SecretKeyDeserializer;

impl DeserializeParam<SecretKey> for SecretKeyDeserializer {
    const EXPECTING: BasicTypes = BasicTypes::STRING;

    fn deserialize_param(
        &self,
        ctx: DeserializeContext<'_>,
        param: &'static ParamMetadata,
    ) -> Result<SecretKey, ErrorWithOrigin> {
        let deserializer = ctx.current_value_deserializer(param.name)?;
        let b256 = B256::deserialize(deserializer)?;
        SecretKey::from_slice(b256.as_slice()).map_err(ErrorWithOrigin::custom)
    }

    fn serialize_param(&self, param: &SecretKey) -> Value {
        let bytes = B256::from_slice(&param.secret_bytes());
        serde_json::to_value(bytes).expect("failed serializing to JSON")
    }
}

pub fn resolve_network_interface(interface: &str) -> Result<Ipv4Addr, String> {
    if let Ok(ip) = interface.parse::<Ipv4Addr>() {
        return Ok(ip);
    }

    match resolve_net_if_ip(interface)
        .map_err(|err| format!("failed to resolve network interface '{interface}': {err}"))?
    {
        IpAddr::V4(ip) => Ok(ip),
        IpAddr::V6(ip) => Err(format!(
            "failed to resolve network interface '{interface}': resolved to unsupported IPv6 address {ip}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::resolve_network_interface;
    use reth_network_peers::TrustedPeer;
    use smart_config::{
        ConfigRepository, ConfigSchema, DescribeConfig, DeserializeConfig, Environment, Serde,
        de::Delimited,
    };
    use std::net::Ipv4Addr;

    #[derive(Debug, DescribeConfig, DeserializeConfig)]
    #[config(crate = smart_config)]
    struct TestNetworkConfig {
        #[config(default_t = Ipv4Addr::UNSPECIFIED, with = Serde![str])]
        address: Ipv4Addr,
        #[config(default_t = None)]
        interface: Option<String>,
        #[config(default, with = Delimited::repeat(Serde![str], ","))]
        boot_nodes: Vec<TrustedPeer>,
    }

    fn parse_config<const N: usize>(env_vars: [(&str, &str); N]) -> TestNetworkConfig {
        let mut schema = ConfigSchema::default();
        schema
            .insert(&TestNetworkConfig::DESCRIPTION, "network")
            .unwrap();
        let repo = ConfigRepository::new(&schema).with(Environment::from_iter("", env_vars));
        repo.single::<TestNetworkConfig>().unwrap().parse().unwrap()
    }

    #[test]
    fn network_interface_accepts_ipv4_addresses() {
        assert_eq!(
            resolve_network_interface("172.16.1.12").unwrap(),
            Ipv4Addr::new(172, 16, 1, 12)
        );
    }

    #[test]
    fn network_interface_is_a_separate_config_field() {
        let config = parse_config([("NETWORK_INTERFACE", "172.16.1.12")]);
        assert_eq!(config.address, Ipv4Addr::UNSPECIFIED);
        assert_eq!(config.interface.as_deref(), Some("172.16.1.12"));
    }

    #[test]
    fn network_boot_nodes_accept_dns_names() {
        let config = parse_config([(
            "NETWORK_BOOT_NODES",
            "enode://6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@localhost:30303?discport=30301",
        )]);

        assert_eq!(config.boot_nodes.len(), 1);
        assert_eq!(config.boot_nodes[0].host.to_string(), "localhost");
        let record = config.boot_nodes[0].resolve_blocking().unwrap();
        assert!(record.address.is_loopback());
        assert_eq!(record.tcp_port, 30303);
        assert_eq!(record.udp_port, 30301);
    }
}
