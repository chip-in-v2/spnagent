//! # Common Utilities
//!
//! This module provides shared utility functions and configurations used by both
//! the Consumer and Provider endpoints. It handles certificate loading,
//! crypto provider initialization, QUIC endpoint creation, and connection inspection.

use quinn::crypto::rustls::QuicClientConfig;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::error::Error;
use std::sync::Arc;
use tokio::time::Duration;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use x509_parser::oid_registry::OID_X509_COMMON_NAME;
use x509_parser::parse_x509_certificate;

const MAX_CONCURRENT_UNI_STREAMS: u8 = 0;
const DATAGRAM_RECEIVE_BUFFER_SIZE: usize = 1024 * 1024;

const KEEP_ALIVE_INTERVAL_SECS: u64 = 5;
const IDLE_TIMEOUT_SECS: u64 = 20;

pub const GRACEFUL_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
pub const PROXY_BUFFER_SIZE: usize = 16 * 1024;
pub const HUB_CONNECTION_RETRY_INTERVAL: Duration = Duration::from_secs(5);

/// Initializes the tracing subscriber for logging.
pub fn setup_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .json()
        .with_current_span(false)
        .init();
}

/// Installs the default crypto provider.
pub fn initialize_crypto_provider() {
    // rustls::crypto::ring::default_provider()
    //    .install_default()
    //    .expect("could not install default crypto provider");

    // TLS intercept - Temporary workaround; revisit for cleaner implementation in Quinn 0.12.
    crate::tls_kx_intercept::install_intercept_provider();
}

/// Loads client certificates, a private key, and a trust store from PEM files.
///
/// # Arguments
///
/// * `my_cert_path`: Path to the client's certificate PEM file.
/// * `my_key_path`: Path to the client's private key PEM file.
/// * `trust_ca_cert_path`: Path to the trusted CA certificate(s) PEM file.
pub fn load_certs_and_key(
    my_cert_path: &str,
    my_key_path: &str,
    trust_ca_cert_path: &str,
) -> Result<
    (
        Vec<CertificateDer<'static>>,
        PrivateKeyDer<'static>,
        quinn::rustls::RootCertStore,
    ),
    Box<dyn Error>,
> {
    info!("Loading client certificates from '{}'...", my_cert_path);
    let certs = CertificateDer::pem_file_iter(my_cert_path)
        .map_err(|e| {
            format!(
                "Failed to read certificate file at '{}': {}",
                my_cert_path, e
            )
        })?
        .map(|cert_result| {
            let cert = cert_result.map_err(|e| {
                format!("Failed to parse certificate in '{}': {}", my_cert_path, e)
            })?;

            if let Ok((_, parsed_cert)) = parse_x509_certificate(cert.as_ref()) {
                info!("  - Client Cert: {}", parsed_cert.subject());
                let validity = parsed_cert.validity();
                info!("    Validity: {} to {}", validity.not_before, validity.not_after);
            }

            Ok(cert)
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;

    let key = PrivateKeyDer::from_pem_file(my_key_path)
        .map_err(|e| format!("Failed to load private key from '{}': {}", my_key_path, e))?;

    let mut truststore = quinn::rustls::RootCertStore::empty();
    info!("Loading Trusted CAs from '{}'...", trust_ca_cert_path);
    let mut ca_count = 0;

    for cert_result in CertificateDer::pem_file_iter(trust_ca_cert_path).map_err(|e| {
        format!(
            "Failed to read trust store file at '{}': {}",
            trust_ca_cert_path, e
        )
    })? {
        let cert = cert_result.map_err(|e| {
            format!(
                "Failed to parse CA certificate in '{}': {}",
                trust_ca_cert_path, e
            )
        })?;

        if let Ok((_, parsed_cert)) = parse_x509_certificate(cert.as_ref()) {
            info!("  - Trusted CA: {}", parsed_cert.subject());
        }

        truststore.add(cert)?;
        ca_count += 1;
    }
    info!("Successfully loaded {} Trusted CA(s).", ca_count);

    Ok((certs, key, truststore))
}

/// Creates and configures a QUIC client endpoint.
///
/// This function sets up the TLS configuration with client authentication,
/// ALPN protocols, and transport parameters like keep-alive and idle timeout.
///
/// # Arguments
///
/// * `certs`: A vector of `CertificateDer` representing the client's certificate chain.
/// * `key`: The client's private key as a `PrivateKeyDer`.
/// * `truststore`: A `RootCertStore` containing the trusted CA certificates for server verification.
/// * `alpn_protocols`: A slice of byte slices, where each represents a supported ALPN protocol to be advertised to the server.
///
/// # Returns
///
/// A `Result` containing the configured `quinn::Endpoint` on success, or a `Box<dyn Error>` on failure.
pub fn create_quic_client_endpoint(
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    truststore: quinn::rustls::RootCertStore,
    alpn_protocols: &[&[u8]],
) -> Result<quinn::Endpoint, Box<dyn Error>> {
    let mut client_config = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(truststore)
        .with_client_auth_cert(certs, key)
        .expect("invalid client certs/key");
    client_config.alpn_protocols = alpn_protocols.iter().map(|p| p.to_vec()).collect();

    let mut quic_client_config =
        quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_config)?));
    let mut transport_config = quinn::TransportConfig::default();
    transport_config
        .max_concurrent_uni_streams(MAX_CONCURRENT_UNI_STREAMS.into())
        .keep_alive_interval(Some(Duration::from_secs(KEEP_ALIVE_INTERVAL_SECS)))
        .datagram_receive_buffer_size(Some(DATAGRAM_RECEIVE_BUFFER_SIZE))
        .max_idle_timeout(Some(Duration::from_secs(IDLE_TIMEOUT_SECS).try_into()?));
    quic_client_config.transport_config(Arc::new(transport_config));

    let mut endpoint = quinn::Endpoint::client("[::]:0".parse().unwrap()).or_else(|e| {
        warn!(
            "Failed to bind to IPv6 socket '[::]:0': {}. Falling back to IPv4 '0.0.0.0:0'.",
            e
        );
        quinn::Endpoint::client("0.0.0.0:0".parse().unwrap())
    })?;

    endpoint.set_default_client_config(quic_client_config);
    Ok(endpoint)
}

/// Inspects a connected QUIC connection to extract and log peer identity information.
///
/// This function retrieves the peer's certificate chain (if present) to log the Subject, Issuer,
/// Serial, and Common Name (CN). It also logs the negotiated ALPN protocol.
///
/// # Arguments
/// * `connection`: The active QUIC connection to inspect.
///
/// # Returns
/// A tuple containing:
/// * `Option<String>`: The Common Name (CN) from the peer's certificate, if found.
/// * `Option<String>`: The negotiated ALPN protocol, if any.
pub async fn check_and_get_info_connection(
    connection: quinn::Connection,
) -> (Option<String>, Option<String>) {
    let mut cn = None;

    // certificate
    if let Some(identity) = connection.peer_identity() {
        if let Some(certs) = identity.downcast_ref::<Vec<CertificateDer<'static>>>()
            && let Some(client_cert) = certs.first()
        {
            if let Ok((_, parsed_cert)) = parse_x509_certificate(client_cert.as_ref()) {
                info!("  - Subject: {}", parsed_cert.subject());
                info!("  - Issuer:  {}", parsed_cert.issuer());
                info!("  - Serial:  {}", parsed_cert.serial);

                // CN (Common Name)
                cn = parsed_cert
                    .subject()
                    .iter()
                    .flat_map(|rdn| rdn.iter())
                    .find(|attr| attr.attr_type() == &OID_X509_COMMON_NAME)
                    .and_then(|attr| attr.attr_value().as_str().ok())
                    .map(String::from);

                if let Some(cn_val) = &cn {
                    info!("  - CN:      {}", cn_val);
                } else {
                    info!("  - CN:      Not found");
                }
            } else {
                error!("Failed to parse client certificate.");
            }
        }
    } else {
        info!("Client did not present a certificate.");
    }

    // ALPN
    let alpn = connection.handshake_data().and_then(|data| {
        data.downcast_ref::<quinn::crypto::rustls::HandshakeData>()
            .and_then(|h| h.protocol.as_ref())
            .map(|p| String::from_utf8_lossy(p).into_owned())
    });

    if let Some(alpn_val) = &alpn {
        info!("ALPN is {}", alpn_val);
    } else {
        info!("No ALPN protocol negotiated.");
    }

    (cn, alpn)
}
