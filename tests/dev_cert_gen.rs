use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

/// Development certificate provisioning for live inter-Node mTLS runs.
///
/// `cargo test --test dev_cert_gen` with `WW_CERTS_DIR=<dir>` writes a
/// development CA, one PEM certificate/key pair per requested Node, and a
/// ready-to-use `<node>.env` file containing the full
/// `FINNSTREAM_SUBSCRIPTION_MTLS_*` set. Without the environment variable
/// the test is a no-op so the normal suite stays untouched.
///
/// This material is for development and acceptance clusters only; private
/// keys must never be committed.
#[test]
fn generate_development_mtls_material() {
    let Some(out) = std::env::var_os("WW_CERTS_DIR").map(PathBuf::from) else {
        return;
    };
    let nodes = std::env::var("WW_CERTS_NODES")
        .unwrap_or_else(|_| "control-1,control-2,control-3".to_owned());
    let tls_port = std::env::var("WW_CERTS_PORT").unwrap_or_else(|_| "7271".to_owned());
    std::fs::create_dir_all(&out).unwrap();

    let ca_key = rcgen::KeyPair::generate().unwrap();
    let mut ca_params =
        rcgen::CertificateParams::new(vec!["whitewater-dev-ca".to_owned()]).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![rcgen::KeyUsagePurpose::KeyCertSign];
    let ca = ca_params.self_signed(&ca_key).unwrap();
    std::fs::write(out.join("ca.pem"), ca.pem()).unwrap();

    let mut pins: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut endpoints = Vec::new();
    for node in nodes.split(',').map(str::trim).filter(|n| !n.is_empty()) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec![node.to_owned()]).unwrap();
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
        ];
        let cert = params.signed_by(&key, &ca, &ca_key).unwrap();
        let digest = blake3::hash(cert.der().as_ref()).to_hex().to_string();
        pins.entry(node.to_owned()).or_default().insert(digest);
        endpoints.push(format!("{node}@https://{node}:{tls_port}"));
        std::fs::write(out.join(format!("{node}.pem")), cert.pem()).unwrap();
        std::fs::write(out.join(format!("{node}.key")), key.serialize_pem()).unwrap();
    }
    let pin_line = pins
        .iter()
        .map(|(node, digests)| {
            format!(
                "{node}@{}",
                digests.iter().cloned().collect::<Vec<_>>().join("|")
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let endpoint_line = endpoints.join(",");
    for node in pins.keys() {
        let env = format!(
            "FINNSTREAM_SUBSCRIPTION_MTLS_BIND=0.0.0.0:{tls_port}\n\
             FINNSTREAM_SUBSCRIPTION_MTLS_CERT=/certs/{node}.pem\n\
             FINNSTREAM_SUBSCRIPTION_MTLS_KEY=/certs/{node}.key\n\
             FINNSTREAM_SUBSCRIPTION_MTLS_CA=/certs/ca.pem\n\
             FINNSTREAM_SUBSCRIPTION_MTLS_PEER_PINS={pin_line}\n\
             FINNSTREAM_SUBSCRIPTION_MTLS_ENDPOINTS={endpoint_line}\n"
        );
        std::fs::write(out.join(format!("{node}.env")), env).unwrap();
    }
    println!(
        "development mTLS material written to {}",
        Path::new(&out).display()
    );
}
