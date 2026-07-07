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

pub fn sign_session(expiry: u64, key: &[u8]) -> Result<String, String> {
    let mut mac =
        HmacSha256::new_from_slice(key).map_err(|e| format!("invalid session signing key: {e}"))?;
    mac.update(expiry.to_string().as_bytes());
    let sig = mac.finalize().into_bytes();
    Ok(format!(
        "{}.{}",
        expiry,
        general_purpose::URL_SAFE_NO_PAD.encode(sig)
    ))
}

pub fn verify_session(token: &str, key: &[u8]) -> bool {
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
    mac.update(expiry_str.as_bytes());
    let Ok(sig_bytes) = general_purpose::URL_SAFE_NO_PAD.decode(sig_str) else {
        return false;
    };
    mac.verify_slice(&sig_bytes).is_ok()
}

pub fn create_session_cookie(expiry: u64, key: &[u8], secure: bool) -> Result<String, String> {
    let token = sign_session(expiry, key)?;
    let flags = if secure {
        "; HttpOnly; SameSite=Strict; Secure; Path=/; Max-Age=2592000"
    } else {
        "; HttpOnly; SameSite=Strict; Path=/; Max-Age=2592000"
    };
    Ok(format!("{SESSION_COOKIE}={token}{flags}"))
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

    let valid = cookie_header
        .and_then(get_session_token_from_header)
        .map(|t| verify_session(&t, &state.session_key))
        .unwrap_or(false)
        || (req.uri().path() == "/api/ws"
            && get_ticket_from_query(req.uri().query())
                .map(|t| verify_session(&t, &state.session_key))
                .unwrap_or(false));

    if !valid {
        warn!("auth: invalid or missing session cookie");
        return Err(StatusCode::UNAUTHORIZED);
    }

    Ok(next.run(req).await)
}
