use axum::{
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};
use base64::{Engine, engine::general_purpose};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tracing::warn;

use crate::AppState;

type HmacSha256 = Hmac<Sha256>;

pub const SESSION_COOKIE: &str = "giskard_session";

/// Domain separation for signed tokens. A WebSocket ticket travels in a URL query string (and so
/// can end up in reverse-proxy access logs), so it must never be usable as a long-lived session
/// cookie — and vice versa. Each purpose is mixed into the MAC input, making the two token
/// families cryptographically distinct even though they share the signing key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenPurpose {
    /// The `giskard_session` browser cookie (lifetime: `auth.session_days`).
    Session,
    /// A short-lived `GET /api/ws?ticket=...` token (lifetime: 60 seconds).
    WsTicket,
}

impl TokenPurpose {
    fn domain(self) -> &'static str {
        match self {
            TokenPurpose::Session => "session",
            TokenPurpose::WsTicket => "ticket",
        }
    }
}

pub fn sign_token(purpose: TokenPurpose, expiry: u64, key: &[u8]) -> Result<String, String> {
    let mut mac =
        HmacSha256::new_from_slice(key).map_err(|e| format!("invalid session signing key: {e}"))?;
    mac.update(purpose.domain().as_bytes());
    mac.update(b":");
    mac.update(expiry.to_string().as_bytes());
    let sig = mac.finalize().into_bytes();
    Ok(format!(
        "{}.{}",
        expiry,
        general_purpose::URL_SAFE_NO_PAD.encode(sig)
    ))
}

pub fn verify_token(purpose: TokenPurpose, token: &str, key: &[u8]) -> bool {
    let Some((expiry_str, sig_str)) = token.split_once('.') else {
        return false;
    };
    let Ok(expiry) = expiry_str.parse::<u64>() else {
        return false;
    };
    if expiry < chrono::Utc::now().timestamp() as u64 {
        return false;
    }
    let mut mac = match HmacSha256::new_from_slice(key) {
        Ok(mac) => mac,
        Err(e) => {
            warn!("invalid session verification key: {e}");
            return false;
        }
    };
    mac.update(purpose.domain().as_bytes());
    mac.update(b":");
    mac.update(expiry_str.as_bytes());
    let Ok(sig_bytes) = general_purpose::URL_SAFE_NO_PAD.decode(sig_str) else {
        return false;
    };
    mac.verify_slice(&sig_bytes).is_ok()
}

/// Extract the (unverified) expiry timestamp from a token. Used for sliding-session renewal after
/// the token has already been verified.
pub fn token_expiry(token: &str) -> Option<u64> {
    token.split_once('.')?.0.parse::<u64>().ok()
}

pub fn create_session_cookie(
    expiry: u64,
    key: &[u8],
    secure: bool,
    max_age_secs: u64,
) -> Result<String, String> {
    let token = sign_token(TokenPurpose::Session, expiry, key)?;
    let secure_flag = if secure { "; Secure" } else { "" };
    Ok(format!(
        "{SESSION_COOKIE}={token}; HttpOnly; SameSite=Strict{secure_flag}; Path=/; Max-Age={max_age_secs}"
    ))
}

pub fn get_session_token_from_header(cookie_header: &str) -> Option<String> {
    for cookie in cookie_header.split(';') {
        let cookie = cookie.trim();
        if let Some(val) = cookie.strip_prefix(&format!("{SESSION_COOKIE}=")) {
            return Some(val.to_string());
        }
    }
    None
}

fn get_ticket_from_query(query: Option<&str>) -> Option<String> {
    query?.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        if key == "ticket" {
            Some(value.to_string())
        } else {
            None
        }
    })
}

pub async fn auth_middleware(
    State(state): State<AppState>,
    req: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let cookie_header = req
        .headers()
        .get(axum::http::header::COOKIE)
        .and_then(|v| v.to_str().ok());

    let session_token = cookie_header
        .and_then(get_session_token_from_header)
        .filter(|t| verify_token(TokenPurpose::Session, t, &state.session_key));

    let ticket_ok = req.uri().path() == "/api/ws"
        && get_ticket_from_query(req.uri().query())
            .map(|t| verify_token(TokenPurpose::WsTicket, &t, &state.session_key))
            .unwrap_or(false);

    if session_token.is_none() && !ticket_ok {
        warn!("auth: invalid or missing session cookie");
        return Err(StatusCode::UNAUTHORIZED);
    }

    // Sliding sessions: when a cookie-authenticated request arrives in the second half of the
    // session's lifetime, re-issue the cookie for a full `session_days` window. Skipped for the
    // WebSocket upgrade (a Set-Cookie on a 101 response is not reliably honored).
    let renewal = match &session_token {
        Some(token) if req.uri().path() != "/api/ws" => renewed_session_cookie(&state, token).await,
        _ => None,
    };

    let mut response = next.run(req).await;
    if let Some(cookie) = renewal {
        match axum::http::HeaderValue::from_str(&cookie) {
            Ok(value) => {
                response
                    .headers_mut()
                    .insert(axum::http::header::SET_COOKIE, value);
            }
            Err(e) => warn!("failed to build renewed session cookie header: {e}"),
        }
    }
    Ok(response)
}

/// Build a refreshed session cookie when `token` (already verified) is past the midpoint of the
/// configured session lifetime. Returns `None` when no renewal is due or config cannot be read.
async fn renewed_session_cookie(state: &AppState, token: &str) -> Option<String> {
    let expiry = token_expiry(token)?;
    let now = chrono::Utc::now().timestamp() as u64;
    let config = match state.store.load_config().await {
        Ok(config) => config,
        Err(e) => {
            warn!("session renewal: failed to load config: {e}");
            return None;
        }
    };
    let lifetime_secs = (config.auth.session_days as u64) * 86400;
    let remaining = expiry.saturating_sub(now);
    if remaining >= lifetime_secs / 2 {
        return None;
    }
    match create_session_cookie(
        now + lifetime_secs,
        &state.session_key,
        config.server.secure_cookies,
        lifetime_secs,
    ) {
        Ok(cookie) => Some(cookie),
        Err(e) => {
            warn!("session renewal: failed to sign cookie: {e}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &[u8] = b"0123456789abcdef0123456789abcdef";

    fn future_expiry() -> u64 {
        chrono::Utc::now().timestamp() as u64 + 3600
    }

    #[test]
    fn session_token_round_trips() {
        let token = sign_token(TokenPurpose::Session, future_expiry(), KEY).unwrap();
        assert!(verify_token(TokenPurpose::Session, &token, KEY));
    }

    #[test]
    fn purposes_are_not_interchangeable() {
        let expiry = future_expiry();
        let session = sign_token(TokenPurpose::Session, expiry, KEY).unwrap();
        let ticket = sign_token(TokenPurpose::WsTicket, expiry, KEY).unwrap();
        assert!(!verify_token(TokenPurpose::WsTicket, &session, KEY));
        assert!(!verify_token(TokenPurpose::Session, &ticket, KEY));
    }

    #[test]
    fn expired_token_is_rejected() {
        let expiry = chrono::Utc::now().timestamp() as u64 - 10;
        let token = sign_token(TokenPurpose::Session, expiry, KEY).unwrap();
        assert!(!verify_token(TokenPurpose::Session, &token, KEY));
    }

    #[test]
    fn wrong_key_is_rejected() {
        let token = sign_token(TokenPurpose::Session, future_expiry(), KEY).unwrap();
        assert!(!verify_token(
            TokenPurpose::Session,
            &token,
            b"another-key-another-key-another!"
        ));
    }

    #[test]
    fn tampered_expiry_is_rejected() {
        let token = sign_token(TokenPurpose::Session, future_expiry(), KEY).unwrap();
        let (_, sig) = token.split_once('.').unwrap();
        let forged = format!("{}.{}", future_expiry() + 999_999, sig);
        assert!(!verify_token(TokenPurpose::Session, &forged, KEY));
    }

    #[test]
    fn cookie_max_age_follows_lifetime() {
        let cookie = create_session_cookie(future_expiry(), KEY, false, 7 * 86400).unwrap();
        assert!(cookie.contains("Max-Age=604800"));
        assert!(!cookie.contains("Secure"));
        let secure = create_session_cookie(future_expiry(), KEY, true, 86400).unwrap();
        assert!(secure.contains("; Secure"));
        assert!(secure.contains("Max-Age=86400"));
    }
}
