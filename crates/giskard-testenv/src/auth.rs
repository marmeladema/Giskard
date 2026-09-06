use argon2::password_hash::{SaltString, rand_core::OsRng};
use argon2::{Argon2, PasswordHasher};

pub const PASSWORD: &str = "testpass";

pub fn password_hash(password: &str) -> String {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .unwrap()
        .to_string()
}

pub fn session_key() -> Vec<u8> {
    (0..32u8).collect()
}

pub async fn login(client: &reqwest::Client, base: &str) -> String {
    let response = login_with(client, base, PASSWORD).await;
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    response
        .headers()
        .get("set-cookie")
        .expect("login must set a session cookie")
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string()
}

pub async fn login_with(client: &reqwest::Client, base: &str, password: &str) -> reqwest::Response {
    client
        .post(format!("{base}/api/login"))
        .json(&serde_json::json!({"password": password}))
        .send()
        .await
        .unwrap()
}

#[cfg(test)]
mod tests {
    use argon2::Argon2;
    use argon2::password_hash::{PasswordHash, PasswordVerifier};

    #[test]
    fn generated_hash_verifies_only_the_password() {
        let hash = super::password_hash(super::PASSWORD);
        let parsed = PasswordHash::new(&hash).unwrap();
        assert!(
            Argon2::default()
                .verify_password(super::PASSWORD.as_bytes(), &parsed)
                .is_ok()
        );
        assert!(
            Argon2::default()
                .verify_password(b"wrong", &parsed)
                .is_err()
        );
    }
}
