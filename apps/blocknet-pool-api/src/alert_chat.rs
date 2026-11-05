//! Alert messages to Discord (channel webhooks) and Telegram (the pool's
//! bot), and the bot loop that links Telegram chats to accounts.
//!
//! Messages are plain text. Discord posts never mention anyone, and errors
//! never carry the request URL (it holds the webhook or bot token).

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{anyhow, Context, Result};
use pool_runtime::store::PoolStore;
use serde::Deserialize;
use serde_json::{json, Value};

const HTTP_TIMEOUT: Duration = Duration::from_secs(15);
/// Telegram long-poll wait; the HTTP timeout must be longer.
const TELEGRAM_POLL_SECS: u64 = 25;
const DISCORD_MAX_CHARS: usize = 1_900;
const TELEGRAM_MAX_CHARS: usize = 3_900;

/// Why a chat send failed. `Gone` means the webhook was deleted or the bot
/// was blocked: the channel should be removed instead of retried.
#[derive(Debug)]
pub(crate) enum ChatError {
    Gone,
    Failed(anyhow::Error),
}

impl std::fmt::Display for ChatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Gone => write!(f, "channel is gone"),
            Self::Failed(err) => write!(f, "{err}"),
        }
    }
}

#[axum::async_trait]
pub(crate) trait ChatSender: Send + Sync {
    async fn discord(&self, webhook: &str, text: &str) -> Result<(), ChatError>;
    async fn telegram(&self, chat_id: i64, text: &str) -> Result<(), ChatError>;
}

fn clip(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let mut out: String = text.chars().take(max - 1).collect();
    out.push('…');
    out
}

fn without_url(err: reqwest::Error) -> anyhow::Error {
    anyhow!("{}", err.without_url())
}

pub(crate) struct HttpChat {
    client: reqwest::Client,
    /// `None` when Telegram is not set up.
    telegram_token: Option<String>,
}

impl HttpChat {
    pub(crate) fn new(telegram_token: Option<String>) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(TELEGRAM_POLL_SECS + 15))
            .connect_timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent("bntpool-alerts")
            .build()
            .context("alert chat HTTP client")?;
        Ok(Self {
            client,
            telegram_token,
        })
    }

    async fn telegram_call(
        &self,
        method: &str,
        body: &Value,
        timeout: Duration,
    ) -> Result<Value, ChatError> {
        let Some(token) = &self.telegram_token else {
            return Err(ChatError::Failed(anyhow!("telegram is not set up")));
        };
        let response = self
            .client
            .post(format!("https://api.telegram.org/bot{token}/{method}"))
            .timeout(timeout)
            .json(body)
            .send()
            .await
            .map_err(|err| ChatError::Failed(without_url(err)))?;
        let status = response.status();
        let value: Value = response
            .json()
            .await
            .map_err(|err| ChatError::Failed(without_url(err)))?;
        if value["ok"].as_bool() == Some(true) {
            return Ok(value["result"].clone());
        }
        let description = value["description"].as_str().unwrap_or_default();
        // Blocked by the user, chat deleted, or the bot removed.
        if status.as_u16() == 403 || description.contains("chat not found") {
            return Err(ChatError::Gone);
        }
        Err(ChatError::Failed(anyhow!(
            "telegram {method}: {} {description}",
            status.as_u16()
        )))
    }
}

#[axum::async_trait]
impl ChatSender for HttpChat {
    async fn discord(&self, webhook: &str, text: &str) -> Result<(), ChatError> {
        if !pool_runtime::miner_settings::discord_webhook_valid(webhook) {
            return Err(ChatError::Gone);
        }
        let response = self
            .client
            .post(webhook)
            .timeout(HTTP_TIMEOUT)
            .json(&json!({
                "content": clip(text, DISCORD_MAX_CHARS),
                "username": "bntpool.com",
                "allowed_mentions": {"parse": []},
            }))
            .send()
            .await
            .map_err(|err| ChatError::Failed(without_url(err)))?;
        match response.status().as_u16() {
            200..=299 => Ok(()),
            401 | 403 | 404 => Err(ChatError::Gone),
            code => Err(ChatError::Failed(anyhow!(
                "discord webhook answered {code}"
            ))),
        }
    }

    async fn telegram(&self, chat_id: i64, text: &str) -> Result<(), ChatError> {
        self.telegram_call(
            "sendMessage",
            &json!({
                "chat_id": chat_id,
                "text": clip(text, TELEGRAM_MAX_CHARS),
                "link_preview_options": {"is_disabled": true},
            }),
            HTTP_TIMEOUT,
        )
        .await
        .map(|_| ())
    }
}

/// Records chat sends, for tests.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct MemoryChat {
    pub sent: parking_lot::Mutex<Vec<(String, String)>>,
    /// Destinations answered as gone.
    pub gone: parking_lot::Mutex<Vec<String>>,
}

#[cfg(test)]
#[axum::async_trait]
impl ChatSender for MemoryChat {
    async fn discord(&self, webhook: &str, text: &str) -> Result<(), ChatError> {
        if self.gone.lock().iter().any(|gone| gone == webhook) {
            return Err(ChatError::Gone);
        }
        self.sent
            .lock()
            .push((webhook.to_string(), text.to_string()));
        Ok(())
    }

    async fn telegram(&self, chat_id: i64, text: &str) -> Result<(), ChatError> {
        let to = format!("telegram:{chat_id}");
        if self.gone.lock().contains(&to) {
            return Err(ChatError::Gone);
        }
        self.sent.lock().push((to, text.to_string()));
        Ok(())
    }
}

/// The `t.me` link a miner opens to connect a chat.
pub(crate) fn telegram_link(bot: &str, token: &str) -> String {
    format!("https://t.me/{bot}?start={token}")
}

// ---- bot ----

#[derive(Deserialize)]
struct Update {
    update_id: i64,
    message: Option<Message>,
    my_chat_member: Option<ChatMemberUpdate>,
}

#[derive(Deserialize)]
struct Message {
    chat: Chat,
    from: Option<User>,
    text: Option<String>,
}

#[derive(Deserialize)]
struct Chat {
    id: i64,
    #[serde(rename = "type")]
    kind: String,
}

#[derive(Deserialize)]
struct User {
    first_name: Option<String>,
    username: Option<String>,
    language_code: Option<String>,
}

#[derive(Deserialize)]
struct ChatMemberUpdate {
    chat: Chat,
    new_chat_member: ChatMember,
}

#[derive(Deserialize)]
struct ChatMember {
    status: String,
}

/// What the bot does with one message, decided without I/O.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum BotCommand {
    /// `/start <code>` from a Connect Telegram link.
    Link(String),
    Stop,
    Help,
}

pub(crate) fn parse_command(text: &str) -> BotCommand {
    let mut words = text.split_whitespace();
    let command = words
        .next()
        .unwrap_or_default()
        .split('@')
        .next()
        .unwrap_or_default();
    match command {
        "/start" => match words.next() {
            Some(code) if code.len() == 32 && code.bytes().all(|b| b.is_ascii_hexdigit()) => {
                BotCommand::Link(code.to_ascii_lowercase())
            }
            _ => BotCommand::Help,
        },
        "/stop" => BotCommand::Stop,
        _ => BotCommand::Help,
    }
}

fn display_name(user: Option<&User>) -> String {
    let Some(user) = user else {
        return "Telegram".to_string();
    };
    if let Some(username) = user.username.as_deref().filter(|name| !name.is_empty()) {
        return format!("@{username}");
    }
    user.first_name
        .as_deref()
        .map(|name| name.chars().take(40).collect())
        .unwrap_or_else(|| "Telegram".to_string())
}

async fn blocking<T, F>(store: &Arc<PoolStore>, operation: F) -> Result<T>
where
    F: FnOnce(&PoolStore) -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    let store = Arc::clone(store);
    tokio::task::spawn_blocking(move || operation(&store))
        .await
        .map_err(|err| anyhow!("join error: {err}"))?
}

/// Polls the bot's updates forever: links chats from Connect Telegram,
/// unlinks them on /stop or when the bot is blocked.
pub(crate) fn spawn_telegram_bot(store: Arc<PoolStore>, chat: Arc<HttpChat>) {
    tokio::spawn(async move {
        let commands = json!({"commands": [
            {"command": "stop", "description": "Stop bntpool.com alerts in this chat"},
        ]});
        if let Err(err) = chat
            .telegram_call("setMyCommands", &commands, HTTP_TIMEOUT)
            .await
        {
            tracing::warn!(error = %err, "telegram setMyCommands failed");
        }
        let mut offset: i64 = 0;
        loop {
            let body = json!({
                "offset": offset,
                "timeout": TELEGRAM_POLL_SECS,
                "allowed_updates": ["message", "my_chat_member"],
            });
            let updates = match chat
                .telegram_call(
                    "getUpdates",
                    &body,
                    Duration::from_secs(TELEGRAM_POLL_SECS + 10),
                )
                .await
            {
                Ok(value) => serde_json::from_value::<Vec<Update>>(value).unwrap_or_default(),
                Err(err) => {
                    tracing::warn!(error = %err, "telegram getUpdates failed");
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    continue;
                }
            };
            for update in updates {
                offset = offset.max(update.update_id + 1);
                if let Err(err) = handle_update(&store, &chat, update).await {
                    tracing::warn!(error = %err, "telegram update failed");
                }
            }
        }
    });
}

async fn handle_update(store: &Arc<PoolStore>, chat: &HttpChat, update: Update) -> Result<()> {
    if let Some(member) = update.my_chat_member {
        if matches!(member.new_chat_member.status.as_str(), "kicked" | "left") {
            let chat_id = member.chat.id;
            blocking(store, move |store| {
                store.clear_miner_alert_telegram_chat(chat_id)
            })
            .await?;
        }
        return Ok(());
    }
    let Some(message) = update.message else {
        return Ok(());
    };
    // Alerts go to private chats only.
    if message.chat.kind != "private" {
        return Ok(());
    }
    let chat_id = message.chat.id;
    let user_lang = crate::alerts::supported_lang(
        message
            .from
            .as_ref()
            .and_then(|user| user.language_code.as_deref())
            .and_then(|code| code.split('-').next())
            .unwrap_or("en"),
    )
    .to_string();
    let reply = match parse_command(message.text.as_deref().unwrap_or_default()) {
        BotCommand::Link(code) => {
            let hash = crate::alerts::token_hash(&code);
            let name = display_name(message.from.as_ref());
            let linked = blocking(store, move |store| {
                store.finish_miner_alert_telegram(&hash, chat_id, &name, SystemTime::now())
            })
            .await?;
            match linked {
                Some((account, lang)) => {
                    tracing::info!(account = %account, "telegram alerts connected");
                    crate::alerts::bot_text(
                        &lang,
                        "telegramConnected",
                        &[&crate::alerts::short_account(&account)],
                    )
                }
                None => crate::alerts::bot_text(&user_lang, "telegramExpired", &[]),
            }
        }
        BotCommand::Stop => {
            let cleared = blocking(store, move |store| {
                store.clear_miner_alert_telegram_chat(chat_id)
            })
            .await?;
            if cleared > 0 {
                tracing::info!(accounts = cleared, "telegram alerts stopped");
            }
            crate::alerts::bot_text(&user_lang, "telegramStopped", &[])
        }
        BotCommand::Help => crate::alerts::bot_text(&user_lang, "telegramHelp", &[]),
    };
    if let Err(err) = chat.telegram(chat_id, &reply).await {
        if !matches!(err, ChatError::Gone) {
            tracing::warn!(error = %err, "telegram reply failed");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bot_commands() {
        let code = "0123456789abcdef0123456789ABCDEF";
        assert_eq!(
            parse_command(&format!("/start {code}")),
            BotCommand::Link(code.to_ascii_lowercase())
        );
        assert_eq!(
            parse_command(&format!("/start@bntpool_alerts_bot {code}")),
            BotCommand::Link(code.to_ascii_lowercase())
        );
        assert_eq!(parse_command("/start"), BotCommand::Help);
        assert_eq!(parse_command("/start nothex"), BotCommand::Help);
        assert_eq!(parse_command("/stop"), BotCommand::Stop);
        assert_eq!(parse_command("hello"), BotCommand::Help);
    }

    #[test]
    fn long_messages_are_clipped() {
        assert_eq!(clip("abc", 5), "abc");
        assert_eq!(clip("abcdef", 4), "abc…");
    }

    #[test]
    fn chat_names_prefer_the_username() {
        let user = User {
            first_name: Some("Ada".into()),
            username: Some("ada_rigs".into()),
            language_code: None,
        };
        assert_eq!(display_name(Some(&user)), "@ada_rigs");
        let user = User {
            first_name: Some("Ada".into()),
            username: None,
            language_code: None,
        };
        assert_eq!(display_name(Some(&user)), "Ada");
    }
}
