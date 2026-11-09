//! Kalshi request signing. Key material deliberately has no Debug implementation.

use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, ensure};
use base64::{Engine, engine::general_purpose::STANDARD};
use ed25519_dalek::{Signer as _, SigningKey, pkcs8::DecodePrivateKey as _};
use rsa::{
    RsaPrivateKey,
    pkcs1::DecodeRsaPrivateKey as _,
    pkcs8::DecodePrivateKey as _,
    signature::{RandomizedSigner as _, SignatureEncoding as _},
};
use sha2::Sha256;

enum PrivateKey {
    Ed25519(Box<SigningKey>),
    Rsa(Box<RsaPrivateKey>),
}

pub struct Credentials {
    key_id: String,
    key: PrivateKey,
}

impl Credentials {
    pub fn load(key_id: String, path: &Path) -> anyhow::Result<Self> {
        let pem = std::fs::read_to_string(path).context("cannot read private-key file")?;
        Self::from_pem(key_id, &pem)
    }

    pub fn from_pem(key_id: String, pem: &str) -> anyhow::Result<Self> {
        ensure!(!key_id.trim().is_empty(), "Key ID is empty");
        ensure!(
            key_id.bytes().all(|c| c.is_ascii_graphic()),
            "Key ID contains invalid header characters"
        );
        let key = if let Ok(key) = SigningKey::from_pkcs8_pem(pem) {
            PrivateKey::Ed25519(Box::new(key))
        } else {
            let key = RsaPrivateKey::from_pkcs8_pem(pem)
                .or_else(|_| RsaPrivateKey::from_pkcs1_pem(pem))
                .map_err(|_| anyhow::anyhow!("unsupported or malformed private key; expected Ed25519 PKCS#8 or RSA PKCS#8/PKCS#1"))?;
            PrivateKey::Rsa(Box::new(key))
        };
        Ok(Self { key_id, key })
    }

    pub fn headers(&self, method: &str, path: &str) -> anyhow::Result<Vec<(&'static str, String)>> {
        let millis = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
        self.headers_at(method, path, millis)
    }

    fn headers_at(
        &self,
        method: &str,
        path: &str,
        millis: u128,
    ) -> anyhow::Result<Vec<(&'static str, String)>> {
        ensure!(path.starts_with('/'), "sign the request path, not a URL");
        let timestamp = millis.to_string();
        let path = path.split('?').next().unwrap();
        let text = format!("{timestamp}{method}{path}");
        let signature = match &self.key {
            PrivateKey::Ed25519(key) => STANDARD.encode(key.sign(text.as_bytes()).to_bytes()),
            PrivateKey::Rsa(key) => {
                let signer = rsa::pss::BlindedSigningKey::<Sha256>::new((**key).clone());
                STANDARD.encode(
                    signer
                        .try_sign_with_rng(&mut rand::rngs::OsRng, text.as_bytes())?
                        .to_vec(),
                )
            }
        };
        Ok(vec![
            ("KALSHI-ACCESS-KEY", self.key_id.clone()),
            ("KALSHI-ACCESS-TIMESTAMP", timestamp),
            ("KALSHI-ACCESS-SIGNATURE", signature),
        ])
    }
}

impl crate::transport::RequestAuthenticator for Credentials {
    fn authenticate(
        &self,
        request: &mut tokio_tungstenite::tungstenite::http::Request<()>,
    ) -> anyhow::Result<()> {
        let path = request
            .uri()
            .path_and_query()
            .map(|p| p.as_str())
            .unwrap_or("/")
            .to_owned();
        for (name, value) in self.headers("GET", &path)? {
            let mut value = tokio_tungstenite::tungstenite::http::HeaderValue::from_str(&value)?;
            value.set_sensitive(true);
            request.headers_mut().insert(name, value);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Verifier as _, pkcs8::EncodePrivateKey as _};
    use rsa::{
        pkcs1::EncodeRsaPrivateKey as _, pkcs8::EncodePrivateKey as _, signature::Verifier as _,
    };

    #[test]
    fn ed25519_signs_the_path_without_query_parameters() {
        let key = SigningKey::from_bytes(&[7; 32]);
        let pem = key.to_pkcs8_pem(Default::default()).unwrap();
        let auth = Credentials::from_pem("test-id".into(), pem.as_str()).unwrap();
        let headers = auth
            .headers_at("GET", "/trade-api/ws/v2?ignored=1", 123)
            .unwrap();
        let bytes = STANDARD.decode(&headers[2].1).unwrap();
        let signature = ed25519_dalek::Signature::from_slice(&bytes).unwrap();
        key.verifying_key()
            .verify(b"123GET/trade-api/ws/v2", &signature)
            .unwrap();
        assert_eq!(headers[1].1, "123");
    }

    #[test]
    fn rsa_supports_pkcs1_and_pkcs8_using_pss_sha256_digest_length_salt() {
        let key = RsaPrivateKey::new(&mut rand::rngs::OsRng, 2048).unwrap();
        let public = rsa::pss::VerifyingKey::<Sha256>::new(key.to_public_key());
        let pkcs8 = key.to_pkcs8_pem(Default::default()).unwrap();
        let pkcs1 = key.to_pkcs1_pem(Default::default()).unwrap();
        for pem in [pkcs8.as_str(), pkcs1.as_str()] {
            let auth = Credentials::from_pem("test-id".into(), pem).unwrap();
            let headers = auth.headers_at("GET", "/trade-api/ws/v2", 123).unwrap();
            let signature =
                rsa::pss::Signature::try_from(STANDARD.decode(&headers[2].1).unwrap().as_slice())
                    .unwrap();
            public
                .verify(b"123GET/trade-api/ws/v2", &signature)
                .unwrap();
        }
    }

    #[test]
    fn bad_keys_and_header_injection_are_rejected_without_echoing_key_material() {
        let error = match Credentials::from_pem("test-id".into(), "PRIVATE-TEST-MATERIAL") {
            Err(error) => error,
            Ok(_) => panic!("accepted invalid key"),
        };
        assert!(!error.to_string().contains("PRIVATE-TEST-MATERIAL"));
        let pem = SigningKey::from_bytes(&[7; 32])
            .to_pkcs8_pem(Default::default())
            .unwrap();
        assert!(Credentials::from_pem("bad\r\nheader".into(), pem.as_str()).is_err());
    }
}
