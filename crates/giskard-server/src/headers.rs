//! Hardening response headers applied to every route.
//!
//! The UI is fully self-contained (its script and stylesheet are served from `/app.js` and
//! `/app.css`, with no third-party origins), which allows a strict Content-Security-Policy:
//!
//! - `script-src 'self'` — inline `<script>` and injected script never execute, so even a bug in
//!   the server-side Markdown sanitizer or the client's `escapeHtml` discipline cannot escalate
//!   to script execution;
//! - `style-src` keeps `'unsafe-inline'` because the UI sets a handful of inline `style`
//!   attributes (a CSS-injection foothold is accepted as low severity in a single-user app);
//! - `connect-src` lists `ws:`/`wss:` explicitly since some browsers do not extend `'self'` to
//!   WebSocket schemes;
//! - `frame-ancestors 'none'` (plus legacy `X-Frame-Options: DENY`) blocks clickjacking.

use axum::{http::HeaderValue, http::Request, middleware::Next, response::Response};

const CONTENT_SECURITY_POLICY: &str = "default-src 'self'; \
     script-src 'self'; \
     style-src 'self' 'unsafe-inline'; \
     img-src 'self' data:; \
     connect-src 'self' ws: wss:; \
     font-src 'self'; \
     object-src 'none'; \
     base-uri 'none'; \
     form-action 'self'; \
     frame-ancestors 'none'";

pub async fn security_headers_middleware(req: Request<axum::body::Body>, next: Next) -> Response {
    let mut response = next.run(req).await;
    let headers = response.headers_mut();
    headers.insert(
        "content-security-policy",
        HeaderValue::from_static(CONTENT_SECURITY_POLICY),
    );
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    headers.insert("x-frame-options", HeaderValue::from_static("DENY"));
    headers.insert("referrer-policy", HeaderValue::from_static("no-referrer"));
    headers.insert(
        "cross-origin-opener-policy",
        HeaderValue::from_static("same-origin"),
    );
    headers.insert(
        "cross-origin-resource-policy",
        HeaderValue::from_static("same-origin"),
    );
    headers.insert(
        "permissions-policy",
        HeaderValue::from_static("camera=(), microphone=(), geolocation=()"),
    );
    response
}
