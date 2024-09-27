// Copyright (c) 2024 - Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::headers::HeaderMap;
use jsonwebtoken::{DecodingKey, Validation};
use serde::Deserialize;
use std::collections::HashSet;

const SIGNATURE_SCHEME_HEADER: &str = "x-restate-signature-scheme";
const SIGNATURE_SCHEME_V1: &str = "v1";
const SIGNATURE_SCHEME_UNSIGNED: &str = "unsigned";
const SIGNATURE_JWT_V1_HEADER: &str = "x-restate-jwt-v1";
const IDENTITY_V1_PREFIX: &str = "publickeyv1_";

#[derive(Debug, thiserror::Error)]
pub enum KeyError {
    #[error("identity v1 jwt public keys are expected to start with {IDENTITY_V1_PREFIX}")]
    MissingPrefix,
    #[error("cannot decode the public key with base58: {0}")]
    Base58(#[from] bs58::decode::Error),
    #[error("decoded key should have length of 32, was {0}")]
    BadLength(usize),
}

#[derive(Debug, thiserror::Error)]
pub enum VerifyError {
    #[error("cannot read header {0}: {1}")]
    ExtractHeader(
        &'static str,
        #[source] Box<dyn std::error::Error + Sync + Send + 'static>,
    ),
    #[error("missing header: {0}")]
    MissingHeader(&'static str),
    #[error("bad {SIGNATURE_SCHEME_HEADER} header, unexpected value {0}")]
    BadSchemeHeader(String),
    #[error("got unsigned request, expecting only signed requests matching the configured keys")]
    UnsignedRequest,
    #[error("invalid JWT: {0}")]
    InvalidJWT(#[from] jsonwebtoken::errors::Error),
}

pub struct IdentityVerifier {
    validation: Validation,
    keys: Vec<DecodingKey>,
}

impl Default for IdentityVerifier {
    fn default() -> Self {
        let mut validation = Validation::new(jsonwebtoken::Algorithm::EdDSA);
        validation.required_spec_claims =
            HashSet::from(["aud".into(), "exp".into(), "iat".into(), "nbf".into()]);
        validation.leeway = 0;
        validation.reject_tokens_expiring_in_less_than = 0;
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.validate_aud = true;

        Self {
            validation,
            keys: vec![],
        }
    }
}

#[derive(Deserialize)]
struct Claims {}

impl IdentityVerifier {
    pub fn new(keys: &[&str]) -> Result<Self, KeyError> {
        let mut iv = IdentityVerifier::default();
        for k in keys {
            iv = iv.with_key(k)?;
        }
        Ok(iv)
    }

    pub fn with_key(mut self, key: &str) -> Result<Self, KeyError> {
        self.keys.push(Self::parse_key(key)?);
        Ok(Self {
            validation: self.validation,
            keys: self.keys,
        })
    }

    fn parse_key(key: &str) -> Result<DecodingKey, KeyError> {
        if !key.starts_with(IDENTITY_V1_PREFIX) {
            return Err(KeyError::MissingPrefix);
        }

        let decoded_key = bs58::decode(key.split_at(IDENTITY_V1_PREFIX.len()).1).into_vec()?;
        if decoded_key.len() != 32 {
            return Err(KeyError::BadLength(decoded_key.len()));
        }

        Ok(DecodingKey::from_ed_der(&decoded_key))
    }

    fn check_v1_keys(&self, jwt_token: &str, path: &str) -> Result<(), VerifyError> {
        let mut validation = self.validation.clone();
        validation.set_audience(&[path]);
        let mut res = Ok(());
        for k in &self.keys {
            if let Err(e) = jsonwebtoken::decode::<Claims>(jwt_token, k, &validation) {
                res = Err(e);
            } else {
                return Ok(());
            }
        }
        res.map_err(VerifyError::InvalidJWT)
    }

    pub fn verify_identity<I>(&self, hm: &I, path: &str) -> Result<(), VerifyError>
    where
        I: HeaderMap,
        <I as HeaderMap>::Error: std::error::Error + Send + Sync + 'static,
    {
        if self.keys.is_empty() {
            return Ok(());
        }

        let scheme_header = hm
            .extract(SIGNATURE_SCHEME_HEADER)
            .map_err(|e| VerifyError::ExtractHeader(SIGNATURE_SCHEME_HEADER, Box::new(e)))?
            .ok_or(VerifyError::MissingHeader(SIGNATURE_SCHEME_HEADER))?;

        match scheme_header {
            SIGNATURE_SCHEME_V1 => {
                let jwt = hm
                    .extract(SIGNATURE_JWT_V1_HEADER)
                    .map_err(|e| VerifyError::ExtractHeader(SIGNATURE_JWT_V1_HEADER, Box::new(e)))?
                    .ok_or(VerifyError::MissingHeader(SIGNATURE_JWT_V1_HEADER))?;

                self.check_v1_keys(jwt, Self::normalise_path(path))
            }
            SIGNATURE_SCHEME_UNSIGNED => Err(VerifyError::UnsignedRequest),
            scheme => Err(VerifyError::BadSchemeHeader(scheme.to_owned())),
        }
    }

    fn normalise_path<'a>(path: &'a str) -> &'a str {
        let slashes: Vec<usize> = path.match_indices('/').map(|(index, _)| index).collect();
        if slashes.len() >= 3
            && &path[slashes[slashes.len() - 3]..slashes[slashes.len() - 2]] == "/invoke"
        {
            &path[slashes[slashes.len() - 3]..]
        } else if !slashes.is_empty() && &path[slashes[slashes.len() - 1]..] == "/discover" {
            &path[slashes[slashes.len() - 1]..]
        } else {
            path
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use ring::rand::SystemRandom;
    use ring::signature::{Ed25519KeyPair, KeyPair};
    use serde::Serialize;
    use std::time::SystemTime;

    #[derive(Serialize)]
    pub(crate) struct Claims<'aud> {
        aud: &'aud str,
        exp: u64,
        iat: u64,
        nbf: u64,
    }

    #[test]
    fn verify() {
        let (jwt, identity_key) = mock_token_and_key();

        let verifier = IdentityVerifier::new(&[&identity_key]).unwrap();

        let headers: Vec<(String, String)> = [
            (
                SIGNATURE_SCHEME_HEADER.to_owned(),
                SIGNATURE_SCHEME_V1.to_owned(),
            ),
            (SIGNATURE_JWT_V1_HEADER.to_owned(), jwt),
        ]
        .into_iter()
        .collect();

        verifier.verify_identity(&headers, "/invoke/foo").unwrap();
    }

    #[test]
    fn bad_key() {
        let verifier =
            IdentityVerifier::new(&["publickeyv1_ChjENKeMvCtRnqG2mrBK1HmPKufgFUc98K8B3ononQvp"])
                .unwrap();

        let headers: Vec<(String, String)> = [
            (SIGNATURE_SCHEME_HEADER.to_owned(), SIGNATURE_SCHEME_V1.to_owned()),
            (SIGNATURE_JWT_V1_HEADER.to_owned(), "eyJ0eXAiOiJKV1QiLCJhbGciOiJFZERTQSIsImtpZCI6InB1YmxpY2tleXYxX0FmUXdtd2ZnRVpocldwdnY4TjUyU0hwUnRacUdHYUZyNEFaTjZxdFlXU2lZIn0.eyJhdWQiOiIvaW52b2tlL2ZvbyIsImV4cCI6MTcyMTY2MjcwOSwiaWF0IjoxNzIxNjYyNjQ5LCJuYmYiOjE3MjE2NjI1ODl9.UBReG_9cdFQ5VcaJxAV0rM8U_zaNw9kMXJZt691SiI0SWw7Ucmz5Zz3wtmVUc1jrkNsnTDhNEvOFGEZoKXTMCQ".to_owned())
        ].into_iter().collect();

        assert!(verifier.verify_identity(&headers, "/invoke/foo").is_err())
    }

    #[test]
    fn normalise_path() {
        let paths = vec![
            ("/invoke/a/b", "/invoke/a/b"),
            ("/foo/invoke/a/b", "/invoke/a/b"),
            ("/foo/bar/invoke/a/b", "/invoke/a/b"),
            ("/discover", "/discover"),
            ("/foo/discover", "/discover"),
            ("/foo/bar/discover", "/discover"),
            ("/foo", "/foo"),
            ("/invoke", "/invoke"),
            ("/foo/invoke", "/foo/invoke"),
            ("/invoke/a", "/invoke/a"),
            ("/foo/invoke/a", "/foo/invoke/a"),
            ("", ""),
            ("/", "/"),
            ("discover", "discover"),
        ];

        for (path, expected_path) in paths {
            let actual_path = IdentityVerifier::normalise_path(path);
            assert_eq!(expected_path, actual_path)
        }
    }

    fn mock_token_and_key() -> (String, String) {
        let serialized_keypair = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let keypair = Ed25519KeyPair::from_pkcs8(serialized_keypair.as_ref()).unwrap();

        let kid = format!(
            "{IDENTITY_V1_PREFIX}{}",
            bs58::encode(keypair.public_key()).into_string()
        );
        let signing_key = jsonwebtoken::EncodingKey::from_ed_der(serialized_keypair.as_ref());

        let header = jsonwebtoken::Header {
            typ: Some("JWT".into()),
            kid: Some(kid.clone()),
            alg: jsonwebtoken::Algorithm::EdDSA,
            ..Default::default()
        };
        let unix_seconds = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("duration since Unix epoch should be well-defined")
            .as_secs();
        let claims = Claims {
            aud: "/invoke/foo",
            nbf: unix_seconds.saturating_sub(60),
            iat: unix_seconds,
            exp: unix_seconds.saturating_add(60),
        };
        let jwt = jsonwebtoken::encode(&header, &claims, &signing_key).unwrap();

        (jwt, kid)
    }
}
