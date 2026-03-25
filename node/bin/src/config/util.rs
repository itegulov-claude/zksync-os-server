use alloy::primitives::B256;
use alloy::signers::k256::ecdsa::SigningKey;
use reth_network_peers::TrustedPeer;
use serde::Deserialize;
use serde_json::Value;
use smart_config::ErrorWithOrigin;
use smart_config::de::{DeserializeContext, DeserializeParam};
use smart_config::metadata::{BasicTypes, ParamMetadata, TypeDescription};
use zksync_os_network::{NodeRecord, SecretKey};
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

/// Custom deserializer for network boot nodes.
///
/// Accepts standard `enode://<pk>@<ip>:<port>` values and DNS-aware
/// `enode://<pk>@<dns_name>:<port>` values. DNS entries are resolved during startup config load.
#[derive(Debug)]
pub struct BootNodeDeserializer;

impl DeserializeParam<NodeRecord> for BootNodeDeserializer {
    const EXPECTING: BasicTypes = BasicTypes::STRING;

    fn describe(&self, description: &mut TypeDescription) {
        description.set_details("enode://<node ID>@<IP-or-DNS host>:<port>");
    }

    fn deserialize_param(
        &self,
        ctx: DeserializeContext<'_>,
        param: &'static ParamMetadata,
    ) -> Result<NodeRecord, ErrorWithOrigin> {
        let deserializer = ctx.current_value_deserializer(param.name)?;
        let raw = String::deserialize(deserializer)?;
        let peer = raw
            .parse::<TrustedPeer>()
            .map_err(ErrorWithOrigin::custom)?;
        peer.resolve_blocking().map_err(|err| {
            ErrorWithOrigin::custom(format!("failed to resolve boot node '{raw}': {err}"))
        })
    }

    fn serialize_param(&self, param: &NodeRecord) -> Value {
        serde_json::to_value(param.to_string()).expect("failed serializing to JSON")
    }
}

#[cfg(test)]
mod tests {
    use super::BootNodeDeserializer;
    use smart_config::{
        ConfigRepository, ConfigSchema, DescribeConfig, DeserializeConfig, Environment, Serde,
        de::Delimited,
    };
    use std::net::Ipv4Addr;
    use zksync_os_network::NodeRecord;

    #[derive(Debug, DescribeConfig, DeserializeConfig)]
    #[config(crate = smart_config)]
    struct TestNetworkConfig {
        #[config(default_t = Ipv4Addr::UNSPECIFIED, with = Serde![str], alias = "interface")]
        address: Ipv4Addr,
        #[config(default, with = Delimited::repeat(BootNodeDeserializer, ","))]
        boot_nodes: Vec<NodeRecord>,
    }

    fn parse_config(env_vars: [(&str, &str); 1]) -> TestNetworkConfig {
        let mut schema = ConfigSchema::default();
        schema
            .insert(&TestNetworkConfig::DESCRIPTION, "network")
            .unwrap();
        let repo = ConfigRepository::new(&schema).with(Environment::from_iter("", env_vars));
        repo.single::<TestNetworkConfig>().unwrap().parse().unwrap()
    }

    #[test]
    fn network_interface_alias_sets_address() {
        let config = parse_config([("NETWORK_INTERFACE", "172.16.1.12")]);
        assert_eq!(config.address, Ipv4Addr::new(172, 16, 1, 12));
    }

    #[test]
    fn network_boot_nodes_accept_dns_names() {
        let config = parse_config([(
            "NETWORK_BOOT_NODES",
            "enode://6f8a80d14311c39f35f516fa664deaaaa13e85b2f7493f37f6144d86991ec012937307647bd3b9a82abe2974e1407241d54947bbb39763a4cac9f77166ad92a0@localhost:30303?discport=30301",
        )]);

        assert_eq!(config.boot_nodes.len(), 1);
        assert!(config.boot_nodes[0].address.is_loopback());
        assert_eq!(config.boot_nodes[0].tcp_port, 30303);
        assert_eq!(config.boot_nodes[0].udp_port, 30301);
    }
}
