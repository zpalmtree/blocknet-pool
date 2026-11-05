//! Sending alert emails over SMTP (Amazon SES): STARTTLS, AUTH PLAIN, one
//! message per connection. Alerts are a few emails a minute at most, so a
//! small client is enough and keeps the pool free of a mail library.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use base64::Engine as _;
use serde::Deserialize;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tokio_rustls::TlsConnector;

const SMTP_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct AlertsConfig {
    /// Explicit opt-in; nothing is sent while false.
    pub enabled: bool,
    pub smtp_host: String,
    pub smtp_port: u16,
    pub from_address: String,
    pub from_name: String,
    /// File holding `SMTP username: …` and `SMTP password: …` lines, not
    /// readable by other users.
    pub credentials_file: String,
    /// Links in emails point here.
    pub site_url: String,
    /// File holding a `Telegram token:` line for the alert bot; empty turns
    /// Telegram alerts off.
    pub telegram_token_file: String,
    /// The bot's username, for Connect Telegram links.
    pub telegram_bot: String,
}

impl Default for AlertsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            smtp_host: "email-smtp.us-east-1.amazonaws.com".to_string(),
            smtp_port: 587,
            from_address: "alerts@bntpool.com".to_string(),
            from_name: "bntpool.com".to_string(),
            credentials_file: String::new(),
            site_url: "https://bntpool.com".to_string(),
            telegram_token_file: String::new(),
            telegram_bot: "bntpool_alerts_bot".to_string(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct SmtpCredentials {
    username: String,
    password: String,
}

/// Reads `name value` lines from a secrets file other users cannot read.
fn secret_fields(path: &Path, what: &str) -> Result<String> {
    let metadata = std::fs::metadata(path).with_context(|| what.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o007 != 0 {
            bail!("{what} must not be readable by other users");
        }
    }
    if !metadata.is_file() {
        bail!("{what} must be a regular file");
    }
    std::fs::read_to_string(path).with_context(|| format!("read {what}"))
}

fn secret_field(text: &str, what: &str, name: &str) -> Result<String> {
    text.lines()
        .find_map(|line| line.trim().strip_prefix(name))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("{what} need a `{name}` line"))
}

/// Reads `SMTP username:` and `SMTP password:` lines from a file other users
/// cannot read.
pub(crate) fn load_credentials(path: &Path) -> Result<SmtpCredentials> {
    let what = "alert SMTP credentials";
    let text = secret_fields(path, what)?;
    Ok(SmtpCredentials {
        username: secret_field(&text, what, "SMTP username:")?,
        password: secret_field(&text, what, "SMTP password:")?,
    })
}

/// Reads the `Telegram token:` line from a file other users cannot read.
pub(crate) fn load_telegram_token(path: &Path) -> Result<String> {
    let what = "alert Telegram token";
    let text = secret_fields(path, what)?;
    let token = secret_field(&text, what, "Telegram token:")?;
    let (id, secret) = token
        .split_once(':')
        .ok_or_else(|| anyhow!("{what} is not a bot token"))?;
    if id.is_empty() || !id.bytes().all(|b| b.is_ascii_digit()) || secret.len() < 20 {
        bail!("{what} is not a bot token");
    }
    Ok(token)
}

/// One email ready to send.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct OutgoingMail {
    pub to: String,
    pub subject: String,
    pub text: String,
    pub html: String,
    /// One-click unsubscribe URL (RFC 8058), if any.
    pub unsubscribe_url: Option<String>,
    /// The same alert as a short plain-text chat message, if it goes to
    /// Discord and Telegram too.
    pub chat: Option<String>,
}

#[axum::async_trait]
pub(crate) trait Mailer: Send + Sync {
    async fn send(&self, mail: &OutgoingMail) -> Result<()>;
}

pub(crate) struct SmtpMailer {
    config: AlertsConfig,
    credentials: SmtpCredentials,
    tls: TlsConnector,
}

impl SmtpMailer {
    pub(crate) fn new(config: AlertsConfig) -> Result<Arc<Self>> {
        let credentials = load_credentials(Path::new(&config.credentials_file))?;
        let mut roots = RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let tls = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        Ok(Arc::new(Self {
            config,
            credentials,
            tls: TlsConnector::from(Arc::new(tls)),
        }))
    }
}

#[axum::async_trait]
impl Mailer for SmtpMailer {
    async fn send(&self, mail: &OutgoingMail) -> Result<()> {
        let message = build_message(&self.config, mail, SystemTime::now());
        tokio::time::timeout(SMTP_TIMEOUT, self.deliver(&mail.to, &message))
            .await
            .map_err(|_| anyhow!("SMTP timed out"))?
    }
}

impl SmtpMailer {
    async fn deliver(&self, to: &str, message: &str) -> Result<()> {
        let host = self.config.smtp_host.as_str();
        let tcp = TcpStream::connect((host, self.config.smtp_port))
            .await
            .with_context(|| format!("connect {host}"))?;
        let mut plain = BufReader::new(tcp);
        expect(&mut plain, 220).await?;
        command(&mut plain, "EHLO bntpool.com", 250).await?;
        command(&mut plain, "STARTTLS", 220).await?;
        let server = ServerName::try_from(host.to_string()).context("SMTP host name")?;
        let tls = self.tls.connect(server, plain.into_inner()).await?;
        let mut tls = BufReader::new(tls);
        command(&mut tls, "EHLO bntpool.com", 250).await?;
        let auth = base64::engine::general_purpose::STANDARD.encode(format!(
            "\0{}\0{}",
            self.credentials.username, self.credentials.password
        ));
        command(&mut tls, &format!("AUTH PLAIN {auth}"), 235).await?;
        command(
            &mut tls,
            &format!("MAIL FROM:<{}>", self.config.from_address),
            250,
        )
        .await?;
        command(&mut tls, &format!("RCPT TO:<{to}>"), 250).await?;
        command(&mut tls, "DATA", 354).await?;
        tls.get_mut()
            .write_all(dot_stuff(message).as_bytes())
            .await?;
        command(&mut tls, ".", 250).await?;
        let _ = command(&mut tls, "QUIT", 221).await;
        Ok(())
    }
}

async fn command<S>(stream: &mut BufReader<S>, line: &str, code: u16) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    stream.get_mut().write_all(line.as_bytes()).await?;
    stream.get_mut().write_all(b"\r\n").await?;
    stream.get_mut().flush().await?;
    expect(stream, code).await.with_context(|| {
        // Never log credentials.
        let shown = if line.starts_with("AUTH") {
            "AUTH"
        } else {
            line
        };
        format!("SMTP {shown}")
    })
}

/// Reads one (possibly multi-line) reply and checks its code.
async fn expect<S>(stream: &mut BufReader<S>, code: u16) -> Result<()>
where
    S: AsyncRead + Unpin,
{
    loop {
        let mut line = String::new();
        if stream.read_line(&mut line).await? == 0 {
            bail!("SMTP connection closed");
        }
        let got: u16 = line.get(..3).and_then(|c| c.parse().ok()).unwrap_or(0);
        if line.as_bytes().get(3) == Some(&b'-') {
            continue;
        }
        if got != code {
            bail!("SMTP expected {code}, got {}", line.trim_end());
        }
        return Ok(());
    }
}

/// Lines starting with "." get another "." (RFC 5321 §4.5.2); the message
/// ends with CRLF so the closing "." stands on its own line.
fn dot_stuff(message: &str) -> String {
    let mut out = String::with_capacity(message.len() + 16);
    for line in message.split("\r\n") {
        if line.starts_with('.') {
            out.push('.');
        }
        out.push_str(line);
        out.push_str("\r\n");
    }
    out
}

/// RFC 2047 encoded-word for a header value.
fn encode_header(value: &str) -> String {
    if value.is_ascii() && !value.contains(['\r', '\n']) {
        return value.to_string();
    }
    format!(
        "=?UTF-8?B?{}?=",
        base64::engine::general_purpose::STANDARD.encode(value.replace(['\r', '\n'], " "))
    )
}

fn wrap_base64(data: &str) -> String {
    let encoded = base64::engine::general_purpose::STANDARD.encode(data);
    encoded
        .as_bytes()
        .chunks(76)
        .map(|chunk| std::str::from_utf8(chunk).unwrap_or_default())
        .collect::<Vec<_>>()
        .join("\r\n")
}

fn rfc2822_date(at: SystemTime) -> String {
    let secs = at.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    const WEEKDAYS: [&str; 7] = ["Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    format!(
        "{}, {:02} {} {} {:02}:{:02}:{:02} +0000",
        WEEKDAYS[days.rem_euclid(7) as usize],
        day,
        MONTHS[(month - 1) as usize],
        year,
        rem / 3600,
        (rem / 60) % 60,
        rem % 60
    )
}

/// Days since 1970-01-01 to (year, month, day), proleptic Gregorian.
pub(crate) fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + i64::from(month <= 2), month, day)
}

/// The full message: headers and a text + HTML body.
pub(crate) fn build_message(config: &AlertsConfig, mail: &OutgoingMail, now: SystemTime) -> String {
    let boundary = format!("bntpool-{}", hex::encode(rand::random::<[u8; 12]>()));
    let mut headers = vec![
        format!(
            "From: {} <{}>",
            encode_header(&config.from_name),
            config.from_address
        ),
        format!("To: <{}>", mail.to),
        format!("Subject: {}", encode_header(&mail.subject)),
        format!("Date: {}", rfc2822_date(now)),
        format!(
            "Message-ID: <{}@bntpool.com>",
            hex::encode(rand::random::<[u8; 16]>())
        ),
        "MIME-Version: 1.0".to_string(),
        "Auto-Submitted: auto-generated".to_string(),
    ];
    if let Some(url) = &mail.unsubscribe_url {
        headers.push(format!("List-Unsubscribe: <{url}>"));
        headers.push("List-Unsubscribe-Post: List-Unsubscribe=One-Click".to_string());
    }
    headers.push(format!(
        "Content-Type: multipart/alternative; boundary=\"{boundary}\""
    ));
    format!(
        "{}\r\n\r\n--{b}\r\nContent-Type: text/plain; charset=UTF-8\r\nContent-Transfer-Encoding: base64\r\n\r\n{}\r\n--{b}\r\nContent-Type: text/html; charset=UTF-8\r\nContent-Transfer-Encoding: base64\r\n\r\n{}\r\n--{b}--",
        headers.join("\r\n"),
        wrap_base64(&mail.text),
        wrap_base64(&mail.html),
        b = boundary,
    )
}

/// Collects messages instead of sending them (tests, showcases).
#[cfg(test)]
#[derive(Default)]
pub(crate) struct MemoryMailer {
    pub sent: parking_lot::Mutex<Vec<OutgoingMail>>,
    pub fail: std::sync::atomic::AtomicBool,
}

#[cfg(test)]
#[axum::async_trait]
impl Mailer for MemoryMailer {
    async fn send(&self, mail: &OutgoingMail) -> Result<()> {
        if self.fail.load(std::sync::atomic::Ordering::Relaxed) {
            bail!("mailer down");
        }
        self.sent.lock().push(mail.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mail() -> OutgoingMail {
        OutgoingMail {
            to: "miner@example.com".into(),
            subject: "rig-1 stopped mining".into(),
            text: "line\r\n.starts with a dot".into(),
            html: "<p>hi</p>".into(),
            unsubscribe_url: Some("https://bntpool.com/api/alerts/unsubscribe?token=abc".into()),
            chat: None,
        }
    }

    #[test]
    fn messages_carry_unsubscribe_headers_and_both_bodies() {
        let message = build_message(&AlertsConfig::default(), &mail(), UNIX_EPOCH);
        assert!(message.contains("From: bntpool.com <alerts@bntpool.com>\r\n"));
        assert!(message.contains("To: <miner@example.com>\r\n"));
        assert!(message.contains("Subject: rig-1 stopped mining\r\n"));
        assert!(message.contains("Date: Thu, 01 Jan 1970 00:00:00 +0000\r\n"));
        assert!(message.contains(
            "List-Unsubscribe: <https://bntpool.com/api/alerts/unsubscribe?token=abc>\r\n"
        ));
        assert!(message.contains("List-Unsubscribe-Post: List-Unsubscribe=One-Click\r\n"));
        assert!(message.contains("Content-Type: text/plain; charset=UTF-8"));
        assert!(message.contains("Content-Type: text/html; charset=UTF-8"));
        assert!(message.lines().all(|line| line.len() <= 998));
    }

    #[test]
    fn non_ascii_subjects_are_encoded_and_header_injection_is_impossible() {
        assert_eq!(encode_header("plain"), "plain");
        let encoded = encode_header("Выплата\r\nBcc: x@y.z");
        assert!(encoded.starts_with("=?UTF-8?B?") && !encoded.contains('\n'));
        assert!(!encode_header("a\r\nBcc: x@y.z").contains('\n'));
    }

    #[test]
    fn dots_at_line_starts_are_doubled() {
        assert_eq!(dot_stuff("a\r\n.b\r\n..c"), "a\r\n..b\r\n...c\r\n");
    }

    #[test]
    fn dates_are_rfc_2822() {
        // 2026-11-05 04:12:00 UTC is a Thursday.
        let at = UNIX_EPOCH + Duration::from_secs(1_793_851_920);
        assert_eq!(rfc2822_date(at), "Thu, 05 Nov 2026 04:12:00 +0000");
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(11_016), (2000, 2, 29));
    }

    #[test]
    fn credentials_files_must_be_private_and_complete() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("smtp.txt");
        std::fs::write(&path, "SMTP username: AKIAEXAMPLE\nSMTP password: secret\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(load_credentials(&path).is_err());
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        }
        let creds = load_credentials(&path).unwrap();
        assert_eq!(creds.username, "AKIAEXAMPLE");
        assert_eq!(creds.password, "secret");
        std::fs::write(&path, "SMTP username: x\n").unwrap();
        assert!(load_credentials(&path).is_err());
    }

    /// A local SMTP server that speaks plain text (no STARTTLS) is enough to
    /// check the conversation; TLS is rustls' job.
    #[tokio::test]
    async fn smtp_replies_are_checked_line_by_line() {
        let (client, server) = tokio::io::duplex(4096);
        let server = tokio::spawn(async move {
            let mut server = server;
            server
                .write_all(b"220 hello\r\n250-first\r\n250 last\r\n")
                .await
                .unwrap();
        });
        let mut reader = BufReader::new(client);
        expect(&mut reader, 220).await.unwrap();
        expect(&mut reader, 250).await.unwrap();
        server.await.unwrap();
        assert!(expect(&mut reader, 250).await.is_err());
    }
}
