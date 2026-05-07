use std::io;
use std::io::ErrorKind;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::http::header::AUTHORIZATION;
use clap::Args;
use clap::ValueEnum;
use constant_time_eq::constant_time_eq_32;
use jsonwebtoken::Algorithm;
use jsonwebtoken::DecodingKey;
use jsonwebtoken::Validation;
use jsonwebtoken::decode;
use serde::Deserialize;
use sha2::Digest;
use sha2::Sha256;
use time::OffsetDateTime;

const DEFAULT_MAX_CLOCK_SKEW_SECONDS: u64 = 30;
const MIN_SIGNED_BEARER_SECRET_BYTES: usize = 32;

#[derive(Debug, Clone, Default, PartialEq, Eq, Args)]
pub struct AppServerWebsocketAuthArgs {
    #[arg(long = "ws-auth", value_name = "MODE", value_enum)]
    pub ws_auth: Option<WebsocketAuthCliMode>,
    #[arg(long = "ws-token-file", value_name = "PATH")]
    pub ws_token_file: Option<PathBuf>,
    #[arg(long = "ws-token-sha256", value_name = "HEX")]
    pub ws_token_sha256: Option<String>,
    #[arg(long = "ws-shared-secret-file", value_name = "PATH")]
    pub ws_shared_secret_file: Option<PathBuf>,
    #[arg(long = "ws-issuer", value_name = "ISSUER")]
    pub ws_issuer: Option<String>,
    #[arg(long = "ws-audience", value_name = "AUDIENCE")]
    pub ws_audience: Option<String>,
    #[arg(long = "ws-max-clock-skew-seconds", value_name = "SECONDS")]
    pub ws_max_clock_skew_seconds: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum WebsocketAuthCliMode {
    CapabilityToken,
    SignedBearerToken,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AppServerWebsocketAuthSettings {
    pub config: Option<AppServerWebsocketAuthConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppServerWebsocketAuthConfig {
    CapabilityToken {
        source: AppServerWebsocketCapabilityTokenSource,
    },
    SignedBearerToken {
        shared_secret_file: PathBuf,
        issuer: Option<String>,
        audience: Option<String>,
        max_clock_skew_seconds: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppServerWebsocketCapabilityTokenSource {
    TokenFile { token_file: PathBuf },
    TokenSha256 { token_sha256: [u8; 32] },
}

#[derive(Clone, Debug, Default)]
pub struct WebsocketAuthPolicy {
    mode: Option<WebsocketAuthMode>,
}

#[derive(Clone, Debug)]
enum WebsocketAuthMode {
    CapabilityToken {
        token_sha256: [u8; 32],
    },
    SignedBearerToken {
        shared_secret: Vec<u8>,
        issuer: Option<String>,
        audience: Option<String>,
        max_clock_skew_seconds: i64,
    },
}

#[derive(Debug)]
pub struct WebsocketAuthError {
    status_code: StatusCode,
    message: &'static str,
}

#[derive(Deserialize)]
struct JwtClaims {
    exp: i64,
    nbf: Option<i64>,
    iss: Option<String>,
    aud: Option<JwtAudienceClaim>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum JwtAudienceClaim {
    Single(String),
    Multiple(Vec<String>),
}

impl WebsocketAuthError {
    pub fn status_code(&self) -> StatusCode {
        self.status_code
    }

    pub fn message(&self) -> &'static str {
        self.message
    }
}

impl AppServerWebsocketAuthArgs {
    pub fn try_into_settings(self) -> anyhow::Result<AppServerWebsocketAuthSettings> {
        let normalize = |value: Option<String>| {
            value.and_then(|value| {
                let trimmed = value.trim();
                (!trimmed.is_empty()).then(|| trimmed.to_string())
            })
        };
        let config = match self.ws_auth {
            Some(WebsocketAuthCliMode::CapabilityToken) => {
                if self.ws_shared_secret_file.is_some()
                    || self.ws_issuer.is_some()
                    || self.ws_audience.is_some()
                    || self.ws_max_clock_skew_seconds.is_some()
                {
                    anyhow::bail!(
                        "signed bearer websocket auth flags require `--ws-auth signed-bearer-token`"
                    );
                }
                let source = match (self.ws_token_file, self.ws_token_sha256) {
                    (Some(_), Some(_)) => anyhow::bail!(
                        "`--ws-token-file` and `--ws-token-sha256` are mutually exclusive"
                    ),
                    (Some(token_file), None) => {
                        AppServerWebsocketCapabilityTokenSource::TokenFile { token_file }
                    }
                    (None, Some(token_sha256)) => {
                        AppServerWebsocketCapabilityTokenSource::TokenSha256 {
                            token_sha256: sha256_digest_arg("--ws-token-sha256", &token_sha256)?,
                        }
                    }
                    (None, None) => anyhow::bail!(
                        "`--ws-token-file` or `--ws-token-sha256` is required when `--ws-auth capability-token` is set"
                    ),
                };
                Some(AppServerWebsocketAuthConfig::CapabilityToken { source })
            }
            Some(WebsocketAuthCliMode::SignedBearerToken) => {
                if self.ws_token_file.is_some() || self.ws_token_sha256.is_some() {
                    anyhow::bail!(
                        "capability token websocket auth flags require `--ws-auth capability-token`"
                    );
                }
                let shared_secret_file = self.ws_shared_secret_file.context(
                    "`--ws-shared-secret-file` is required when `--ws-auth signed-bearer-token` is set",
                )?;
                Some(AppServerWebsocketAuthConfig::SignedBearerToken {
                    shared_secret_file,
                    issuer: normalize(self.ws_issuer),
                    audience: normalize(self.ws_audience),
                    max_clock_skew_seconds: self
                        .ws_max_clock_skew_seconds
                        .unwrap_or(DEFAULT_MAX_CLOCK_SKEW_SECONDS),
                })
            }
            None => {
                if self.ws_token_file.is_some()
                    || self.ws_token_sha256.is_some()
                    || self.ws_shared_secret_file.is_some()
                    || self.ws_issuer.is_some()
                    || self.ws_audience.is_some()
                    || self.ws_max_clock_skew_seconds.is_some()
                {
                    anyhow::bail!(
                        "websocket auth flags require `--ws-auth capability-token` or `--ws-auth signed-bearer-token`"
                    );
                }
                None
            }
        };
        Ok(AppServerWebsocketAuthSettings { config })
    }
}

pub fn policy_from_settings(
    settings: &AppServerWebsocketAuthSettings,
) -> io::Result<WebsocketAuthPolicy> {
    let mode = match settings.config.as_ref() {
        Some(AppServerWebsocketAuthConfig::CapabilityToken { source }) => match source {
            AppServerWebsocketCapabilityTokenSource::TokenFile { token_file } => {
                let token = read_trimmed_secret(token_file)?;
                Some(WebsocketAuthMode::CapabilityToken {
                    token_sha256: sha256_digest(token.as_bytes()),
                })
            }
            AppServerWebsocketCapabilityTokenSource::TokenSha256 { token_sha256 } => {
                Some(WebsocketAuthMode::CapabilityToken {
                    token_sha256: *token_sha256,
                })
            }
        },
        Some(AppServerWebsocketAuthConfig::SignedBearerToken {
            shared_secret_file,
            issuer,
            audience,
            max_clock_skew_seconds,
        }) => {
            let shared_secret = read_trimmed_secret(shared_secret_file)?.into_bytes();
            validate_signed_bearer_secret(shared_secret_file, &shared_secret)?;
            Some(WebsocketAuthMode::SignedBearerToken {
                shared_secret,
                issuer: issuer.clone(),
                audience: audience.clone(),
                max_clock_skew_seconds: i64::try_from(*max_clock_skew_seconds).map_err(|_| {
                    io::Error::new(
                        ErrorKind::InvalidInput,
                        "websocket auth clock skew is too large",
                    )
                })?,
            })
        }
        None => None,
    };
    Ok(WebsocketAuthPolicy { mode })
}

pub fn should_warn_about_unauthenticated_non_loopback_listener(
    bind_address: std::net::SocketAddr,
    policy: &WebsocketAuthPolicy,
) -> bool {
    !bind_address.ip().is_loopback() && policy.mode.is_none()
}

pub fn authorize_upgrade(
    headers: &HeaderMap,
    policy: &WebsocketAuthPolicy,
) -> Result<(), WebsocketAuthError> {
    let Some(mode) = policy.mode.as_ref() else {
        return Ok(());
    };
    let token = bearer_token_from_headers(headers)?;
    match mode {
        WebsocketAuthMode::CapabilityToken { token_sha256 } => {
            let actual_sha256 = sha256_digest(token.as_bytes());
            if constant_time_eq_32(token_sha256, &actual_sha256) {
                Ok(())
            } else {
                Err(unauthorized("invalid websocket bearer token"))
            }
        }
        WebsocketAuthMode::SignedBearerToken {
            shared_secret,
            issuer,
            audience,
            max_clock_skew_seconds,
        } => verify_signed_bearer_token(
            token,
            shared_secret,
            issuer.as_deref(),
            audience.as_deref(),
            *max_clock_skew_seconds,
        ),
    }
}

fn verify_signed_bearer_token(
    token: &str,
    shared_secret: &[u8],
    issuer: Option<&str>,
    audience: Option<&str>,
    max_clock_skew_seconds: i64,
) -> Result<(), WebsocketAuthError> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.required_spec_claims.clear();
    validation.validate_exp = false;
    validation.validate_nbf = false;
    validation.validate_aud = false;
    let claims = decode::<JwtClaims>(token, &DecodingKey::from_secret(shared_secret), &validation)
        .map_err(|_| unauthorized("invalid websocket jwt"))?
        .claims;
    let now = OffsetDateTime::now_utc().unix_timestamp();
    if now > claims.exp.saturating_add(max_clock_skew_seconds) {
        return Err(unauthorized("expired websocket jwt"));
    }
    if let Some(nbf) = claims.nbf
        && now < nbf.saturating_sub(max_clock_skew_seconds)
    {
        return Err(unauthorized("websocket jwt is not valid yet"));
    }
    if let Some(expected_issuer) = issuer
        && claims.iss.as_deref() != Some(expected_issuer)
    {
        return Err(unauthorized("websocket jwt issuer mismatch"));
    }
    if let Some(expected_audience) = audience
        && !audience_matches(claims.aud.as_ref(), expected_audience)
    {
        return Err(unauthorized("websocket jwt audience mismatch"));
    }
    Ok(())
}

fn audience_matches(audience: Option<&JwtAudienceClaim>, expected_audience: &str) -> bool {
    match audience {
        Some(JwtAudienceClaim::Single(actual)) => actual == expected_audience,
        Some(JwtAudienceClaim::Multiple(actual)) => {
            actual.iter().any(|aud| aud == expected_audience)
        }
        None => false,
    }
}

fn bearer_token_from_headers(headers: &HeaderMap) -> Result<&str, WebsocketAuthError> {
    let raw_header = headers
        .get(AUTHORIZATION)
        .ok_or_else(|| unauthorized("missing websocket bearer token"))?;
    let header = raw_header
        .to_str()
        .map_err(|_| unauthorized("invalid authorization header"))?;
    let Some((scheme, token)) = header.split_once(' ') else {
        return Err(unauthorized("invalid authorization header"));
    };
    if !scheme.eq_ignore_ascii_case("Bearer") {
        return Err(unauthorized("invalid authorization header"));
    }
    let token = token.trim();
    if token.is_empty() {
        return Err(unauthorized("invalid authorization header"));
    }
    Ok(token)
}

fn read_trimmed_secret(path: &Path) -> io::Result<String> {
    let raw = std::fs::read_to_string(path).map_err(|err| {
        io::Error::new(
            err.kind(),
            format!(
                "failed to read websocket auth secret {}: {err}",
                path.display()
            ),
        )
    })?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            format!("websocket auth secret {} must not be empty", path.display()),
        ));
    }
    Ok(trimmed.to_string())
}

fn validate_signed_bearer_secret(path: &Path, shared_secret: &[u8]) -> io::Result<()> {
    if shared_secret.len() < MIN_SIGNED_BEARER_SECRET_BYTES {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            format!(
                "signed websocket bearer secret {} must be at least {MIN_SIGNED_BEARER_SECRET_BYTES} bytes",
                path.display()
            ),
        ));
    }
    Ok(())
}

fn sha256_digest(value: &[u8]) -> [u8; 32] {
    Sha256::digest(value).into()
}

fn sha256_digest_arg(flag_name: &str, value: &str) -> anyhow::Result<[u8; 32]> {
    let trimmed = value.trim();
    if trimmed.len() != 64 {
        anyhow::bail!("{flag_name} must be a 64-character hex SHA-256 digest");
    }
    let mut digest = [0_u8; 32];
    hex::decode_to_slice(trimmed, &mut digest)
        .with_context(|| format!("{flag_name} must be hex-encoded"))?;
    Ok(digest)
}

fn unauthorized(message: &'static str) -> WebsocketAuthError {
    WebsocketAuthError {
        status_code: StatusCode::UNAUTHORIZED,
        message,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn capability_token_authorizes_matching_bearer() {
        let policy = WebsocketAuthPolicy {
            mode: Some(WebsocketAuthMode::CapabilityToken {
                token_sha256: sha256_digest(b"secret"),
            }),
        };
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer secret"));
        assert!(authorize_upgrade(&headers, &policy).is_ok());
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer wrong"));
        assert!(authorize_upgrade(&headers, &policy).is_err());
    }
}
