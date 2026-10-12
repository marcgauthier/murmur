use crate::*;
use rustls::{pki_types::{CertificateDer,PrivateKeyDer,UnixTime,ServerName},client::danger::{ServerCertVerifier,ServerCertVerified,HandshakeSignatureValid}};
use std::sync::Arc;
#[derive(Debug)]
struct NodeVerifier {roots:rustls::RootCertStore,provider:Arc<rustls::crypto::CryptoProvider>,allowed:BTreeSet<NodeId>}
impl ServerCertVerifier for NodeVerifier {
    fn verify_server_cert(&self,cert:&CertificateDer<'_>,intermediates:&[CertificateDer<'_>],_name:&ServerName<'_>,_ocsp:&[u8],now:UnixTime)->std::result::Result<ServerCertVerified,rustls::Error> {
        let parsed=rustls::server::ParsedCertificate::try_from(cert)?;
        rustls::client::verify_server_cert_signed_by_trust_anchor(&parsed,&self.roots,intermediates,now,self.provider.signature_verification_algorithms.all)?;
        let id=node_id(cert.as_ref()).map_err(|e|rustls::Error::General(e.to_string()))?;
        if !self.allowed.is_empty() && !self.allowed.contains(&id) {return Err(rustls::Error::General("peer identity denied".into()));}
        Ok(ServerCertVerified::assertion())
    }
    fn verify_tls12_signature(&self,message:&[u8],cert:&CertificateDer<'_>,dss:&rustls::DigitallySignedStruct)->std::result::Result<HandshakeSignatureValid,rustls::Error> {rustls::crypto::verify_tls12_signature(message,cert,dss,&self.provider.signature_verification_algorithms)}
    fn verify_tls13_signature(&self,message:&[u8],cert:&CertificateDer<'_>,dss:&rustls::DigitallySignedStruct)->std::result::Result<HandshakeSignatureValid,rustls::Error> {rustls::crypto::verify_tls13_signature(message,cert,dss,&self.provider.signature_verification_algorithms)}
    fn supported_verify_schemes(&self)->Vec<rustls::SignatureScheme> {self.provider.signature_verification_algorithms.supported_schemes()}
}
pub fn node_id(der:&[u8])->Result<NodeId> {
    use x509_parser::{prelude::*,extensions::GeneralName};
    let (_,cert)=X509Certificate::from_der(der).map_err(|e|Error::Auth(e.to_string()))?;
    let san=cert.subject_alternative_name().map_err(|e|Error::Auth(e.to_string()))?.ok_or_else(||Error::Auth("missing node URI SAN".into()))?;
    let mut found=None;
    for name in &san.value.general_names {
        if let GeneralName::URI(uri)=name {if let Some(value)=uri.strip_prefix("replicateddb://node/") {
            let digits=value.replace('-',"");if digits.len()!=32 || !digits.is_ascii() {return Err(Error::Auth("invalid node identity".into()));}
            let mut id=[0u8;16];for(i,byte)in id.iter_mut().enumerate(){*byte=u8::from_str_radix(&digits[2*i..2*i+2],16).map_err(|_|Error::Auth("invalid node identity".into()))?;}
            if id==[0;16] || found.replace(id).is_some() {return Err(Error::Auth("ambiguous node identity".into()));}
        }}
    }
    found.ok_or_else(||Error::Auth("missing node URI SAN".into()))
}
pub fn network_allowed(config:&Config,address:SocketAddr)->bool {
    let ip=match address.ip(){std::net::IpAddr::V6(v)=>v.to_ipv4_mapped().map(std::net::IpAddr::V4).unwrap_or(std::net::IpAddr::V6(v)),v=>v};
    config.allowed_networks.is_empty() || config.allowed_networks.iter().any(|n|n.contains(&ip))
}
pub fn peer_id(connection:&quinn::Connection,config:&Config)->Result<NodeId> {
    if !network_allowed(config,connection.remote_address()){return Err(Error::Auth("remote network denied".into()));}
    let identity=connection.peer_identity().ok_or_else(||Error::Auth("missing peer certificate".into()))?;
    let chain=identity.downcast::<Vec<CertificateDer<'static>>>().map_err(|_|Error::Auth("unknown TLS identity".into()))?;
    let id=node_id(chain.first().ok_or_else(||Error::Auth("empty peer chain".into()))?.as_ref())?;
    if !config.allowed_peers.is_empty() && !config.allowed_peers.contains(&id) {return Err(Error::Auth("remote node denied".into()));} Ok(id)
}
pub fn endpoint(config:&Config,node:NodeId)->Result<quinn::Endpoint> {endpoint_with_alpn(config,node,protocol::ALPN)}
pub(crate) fn endpoint_with_alpn(config:&Config,node:NodeId,alpn:&[u8])->Result<quinn::Endpoint> {
    let provider=Arc::new(rustls::crypto::ring::default_provider());
    if node_id(&config.credentials.chain[0])?!=node {return Err(Error::Auth("certificate is not for this node".into()));}
    let mut roots=rustls::RootCertStore::empty();for der in &config.credentials.roots {roots.add(CertificateDer::from(der.clone())).map_err(|e|Error::Auth(e.to_string()))?;}
    let chain:Vec<_>=config.credentials.chain.iter().cloned().map(CertificateDer::from).collect();
    let key=||PrivateKeyDer::try_from(config.credentials.private_key.to_vec()).map_err(|e|Error::Auth(e.to_string()));
    let clients=rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots.clone()),provider.clone()).build().map_err(|e|Error::Auth(e.to_string()))?;
    let mut server=rustls::ServerConfig::builder_with_provider(provider.clone()).with_protocol_versions(&[&rustls::version::TLS13]).map_err(|e|Error::Auth(e.to_string()))?.with_client_cert_verifier(clients).with_single_cert(chain.clone(),key()?).map_err(|e|Error::Auth(e.to_string()))?;
    server.alpn_protocols=vec![alpn.to_vec()];server.max_early_data_size=0;
    let mut client=rustls::ClientConfig::builder_with_provider(provider.clone()).with_protocol_versions(&[&rustls::version::TLS13]).map_err(|e|Error::Auth(e.to_string()))?.dangerous().with_custom_certificate_verifier(Arc::new(NodeVerifier{roots,provider,allowed:config.allowed_peers.clone()})).with_client_auth_cert(chain,key()?).map_err(|e|Error::Auth(e.to_string()))?;
    client.alpn_protocols=vec![alpn.to_vec()];client.enable_early_data=false;
    let mut transport=quinn::TransportConfig::default();transport.max_idle_timeout(Some(Duration::from_secs(30).try_into().unwrap()));transport.keep_alive_interval(Some(Duration::from_secs(10)));transport.max_concurrent_bidi_streams((config.sessions as u32+8).into());transport.max_concurrent_uni_streams(0u32.into());transport.datagram_receive_buffer_size(Some(512*1000));transport.datagram_send_buffer_size(128*1000);
    let transport=Arc::new(transport);
    let mut server=quinn::ServerConfig::with_crypto(Arc::new(quinn::crypto::rustls::QuicServerConfig::try_from(server).map_err(|e|Error::Auth(e.to_string()))?));server.transport_config(transport.clone());
    let mut endpoint=quinn::Endpoint::server(server,config.bind)?;
    let mut client=quinn::ClientConfig::new(Arc::new(quinn::crypto::rustls::QuicClientConfig::try_from(client).map_err(|e|Error::Auth(e.to_string()))?));client.transport_config(transport);endpoint.set_default_client_config(client);Ok(endpoint)
}
