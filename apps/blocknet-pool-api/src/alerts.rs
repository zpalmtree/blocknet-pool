//! Alert emails for miners (set up on My Stats, see `miner_settings.rs`):
//! the emails themselves, the confirm and unsubscribe endpoints, and the
//! worker that checks confirmed accounts once a minute.
//!
//! Rules: nothing but the confirmation email goes to an address until its
//! link is opened; at most `ALERT_DAILY_LIMIT` alerts per account a day;
//! every alert carries a one-click unsubscribe; a pool-wide outage (no
//! accepted share anywhere) never reads as rigs stopping.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use pool_runtime::miner_settings::{
    utc_day, utc_week, utc_week_start, AlertChannels, MinerAlert, ALERT_DAILY_LIMIT,
    ALERT_DROP_BASELINE, ALERT_DROP_MIN_HISTORY, ALERT_DROP_RATIO, ALERT_RECOVER_RATIO,
    ALERT_WATCH_WINDOW,
};
use pool_runtime::store::PoolStore;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::alert_chat::ChatError;
use crate::alert_mail::{civil_from_days, AlertsConfig, Mailer, OutgoingMail};

const WORKER_INTERVAL: Duration = Duration::from_secs(60);
/// Hashrate in emails is the average over this window.
const HASHRATE_WINDOW: Duration = Duration::from_secs(10 * 60);
/// Without one accepted share pool-wide this recently, the pool (not the
/// rigs) is down: skip stopped/started checks.
const POOL_ALIVE_WINDOW: Duration = Duration::from_secs(3 * 60);

const ATOMIC_PER_BNT: f64 = 100_000_000.0;

pub(crate) struct AlertService {
    pub config: AlertsConfig,
    pub mailer: Arc<dyn Mailer>,
    pub chat: Arc<dyn crate::alert_chat::ChatSender>,
    /// The bot's username while Telegram alerts are set up.
    pub telegram_bot: Option<String>,
    /// Runs the bot's update loop (production only).
    pub telegram_poller: Option<Arc<crate::alert_chat::HttpChat>>,
}

pub(crate) fn token_hash(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

pub(crate) fn new_token() -> String {
    hex::encode(rand::random::<[u8; 32]>())
}

/// The shorter code a Connect Telegram link carries (Telegram allows 64
/// characters after `start=`).
pub(crate) fn new_link_code() -> String {
    hex::encode(rand::random::<[u8; 16]>())
}

/// A message the Telegram bot replies with, in the given language.
pub(crate) fn bot_text(lang: &str, key: &str, values: &[&str]) -> String {
    t(supported_lang(lang), key, values)
}

fn catalog() -> &'static HashMap<String, HashMap<String, String>> {
    static CATALOG: OnceLock<HashMap<String, HashMap<String, String>>> = OnceLock::new();
    CATALOG.get_or_init(|| {
        serde_json::from_str(include_str!("alert_messages.json"))
            .expect("alert_messages.json is valid")
    })
}

/// Languages the emails are written in.
pub(crate) fn supported_lang(lang: &str) -> &'static str {
    catalog()
        .get_key_value(lang)
        .map(|(key, _)| key.as_str())
        .unwrap_or("en")
}

fn message(lang: &str, key: &str) -> String {
    let catalog = catalog();
    catalog
        .get(lang)
        .and_then(|messages| messages.get(key))
        .or_else(|| catalog.get("en").and_then(|messages| messages.get(key)))
        .cloned()
        .unwrap_or_else(|| key.to_string())
}

fn fill(template: &str, values: &[&str]) -> String {
    let mut out = template.to_string();
    for (index, value) in values.iter().enumerate() {
        out = out.replace(&format!("{{{index}}}"), value);
    }
    out
}

fn t(lang: &str, key: &str, values: &[&str]) -> String {
    fill(&message(lang, key), values)
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

/// "8yAUM9…1bVm"; memo accounts keep their prefix.
pub(crate) fn short_account(account: &str) -> String {
    let chars: Vec<char> = account.chars().collect();
    if chars.len() <= 14 {
        return account.to_string();
    }
    let head: String = chars[..6].iter().collect();
    let tail: String = chars[chars.len() - 4..].iter().collect();
    format!("{head}…{tail}")
}

fn rig_name(lang: &str, worker: &str) -> String {
    if worker.trim().is_empty() {
        t(lang, "unnamedRig", &[])
    } else {
        worker.to_string()
    }
}

pub(crate) fn format_bnt(atomic: u64) -> String {
    let text = format!("{:.8}", atomic as f64 / ATOMIC_PER_BNT);
    let text = text.trim_end_matches('0');
    let (whole, frac) = text.split_once('.').unwrap_or((text, ""));
    let frac = if frac.len() < 2 {
        format!("{frac:0<2}")
    } else {
        frac.to_string()
    };
    let mut grouped = String::new();
    for (index, digit) in whole.chars().rev().enumerate() {
        if index > 0 && index % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    format!("{}.{frac}", grouped.chars().rev().collect::<String>())
}

pub(crate) fn human_rate(hps: f64) -> String {
    if !hps.is_finite() || hps <= 0.0 {
        return "0 H/s".to_string();
    }
    let units = ["H/s", "KH/s", "MH/s", "GH/s", "TH/s", "PH/s"];
    let mut value = hps;
    let mut unit = 0;
    while value >= 1000.0 && unit < units.len() - 1 {
        value /= 1000.0;
        unit += 1;
    }
    format!("{value:.2} {}", units[unit])
}

/// "2026-11-05 04:12 UTC".
pub(crate) fn utc_time(at: SystemTime) -> String {
    let secs = at.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64;
    let (year, month, day) = civil_from_days(secs.div_euclid(86_400));
    let rem = secs.rem_euclid(86_400);
    format!(
        "{year}-{month:02}-{day:02} {:02}:{:02} UTC",
        rem / 3600,
        (rem / 60) % 60
    )
}

fn duration_text(lang: &str, duration: Duration) -> String {
    let minutes = (duration.as_secs() / 60).max(1);
    if minutes < 60 {
        t(lang, "minutes", &[&minutes.to_string()])
    } else {
        t(
            lang,
            "hoursMinutes",
            &[&(minutes / 60).to_string(), &(minutes % 60).to_string()],
        )
    }
}

/// One email in the pool's plain layout. `facts` values are HTML.
struct Email<'a> {
    lang: &'a str,
    title: String,
    paragraphs: Vec<String>,
    facts: Vec<(String, String, String)>,
    button: Option<(String, String)>,
    after: Vec<String>,
    footer: Vec<String>,
}

const DOT_ON: &str = "<span style=\"display:inline-block;width:8px;height:8px;border-radius:50%;background:#16a34a;margin-right:6px\"></span>";
const DOT_OFF: &str = "<span style=\"display:inline-block;width:8px;height:8px;border-radius:50%;background:#dc2626;margin-right:6px\"></span>";

impl Email<'_> {
    /// `facts` are (label HTML, value HTML, value text).
    fn render(&self) -> (String, String) {
        let font = "-apple-system,'Segoe UI',Helvetica,Arial,sans-serif";
        let mono = "ui-monospace,Menlo,Consolas,monospace";
        let mut html = format!(
            "<!doctype html><html lang=\"{lang}\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>{title}</title></head>\
             <body style=\"margin:0;padding:24px 12px;background:#e6eee9;font-family:{font};color:#0f271c\">\
             <table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\"><tr><td align=\"center\">\
             <table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" style=\"max-width:600px;background:#ffffff;border:1px solid #d3e2d9;border-radius:6px\">\
             <tr><td style=\"background:#0b2a1f;padding:18px 28px;color:#ffffff;font-weight:800;font-size:18px;border-radius:6px 6px 0 0\">bntpool.com</td></tr>\
             <tr><td style=\"padding:26px 28px 8px;font-size:15px;line-height:1.55\">\
             <h1 style=\"margin:0 0 12px;font-size:22px;line-height:1.25\">{title}</h1>",
            lang = escape(self.lang),
            title = escape(&self.title),
        );
        let mut text = format!("{}\n\n", self.title);
        for paragraph in &self.paragraphs {
            html.push_str(&format!("<p style=\"margin:0 0 14px\">{paragraph}</p>"));
            text.push_str(&strip_tags(paragraph));
            text.push_str("\n\n");
        }
        if !self.facts.is_empty() {
            html.push_str("<table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" style=\"margin:4px 0 18px;font-size:14px\">");
            for (label, value, value_text) in &self.facts {
                html.push_str(&format!(
                    "<tr><td style=\"padding:9px 0;border-top:1px solid #e3ece6;color:#546e62\">{label}</td>\
                     <td align=\"right\" style=\"padding:9px 0;border-top:1px solid #e3ece6;font-family:{mono}\">{value}</td></tr>"
                ));
                text.push_str(&format!("{}: {}\n", strip_tags(label), value_text));
            }
            html.push_str("</table>");
            text.push('\n');
        }
        if let Some((url, label)) = &self.button {
            html.push_str(&format!(
                "<p style=\"margin:6px 0 18px\"><a href=\"{url}\" style=\"display:inline-block;padding:11px 20px;background:#0b2a1f;color:#ffffff;text-decoration:none;border-radius:4px;font-weight:700;font-size:14px\">{label}</a></p>",
                url = escape(url),
                label = escape(label),
            ));
            text.push_str(&format!("{label}: {url}\n\n"));
        }
        for paragraph in &self.after {
            html.push_str(&format!("<p style=\"margin:0 0 14px\">{paragraph}</p>"));
            text.push_str(&strip_tags(paragraph));
            text.push_str("\n\n");
        }
        html.push_str("</td></tr><tr><td style=\"padding:16px 28px 22px;font-size:12px;line-height:1.5;color:#546e62;border-top:1px solid #e3ece6\">");
        html.push_str(&self.footer.join(" "));
        html.push_str("</td></tr></table></td></tr></table></body></html>");
        text.push_str("--\n");
        text.push_str(&strip_tags(&self.footer.join(" ")));
        text.push('\n');
        (html, text)
    }

    /// The alert as a short chat message: title, text, facts and the link,
    /// then which address it is for.
    fn chat_text(&self, account: &str) -> String {
        let mut text = format!("{}\n", self.title);
        for paragraph in &self.paragraphs {
            text.push_str(&strip_tags(paragraph));
            text.push('\n');
        }
        if !self.facts.is_empty() {
            text.push('\n');
            for (label, _, value_text) in &self.facts {
                text.push_str(&format!("{}: {}\n", strip_tags(label), value_text));
            }
        }
        if let Some((url, label)) = &self.button {
            text.push_str(&format!("\n{label}: {url}\n"));
        }
        text.push_str(&format!(
            "\n{}",
            t(self.lang, "chatFooter", &[&short_account(account)])
        ));
        text
    }
}

/// Text for the plain-text part: tags removed, links kept as "label (url)".
fn strip_tags(html: &str) -> String {
    let mut out = String::new();
    let mut rest = html;
    while let Some(start) = rest.find('<') {
        out.push_str(&rest[..start]);
        let Some(end) = rest[start..].find('>') else {
            break;
        };
        let tag = &rest[start..start + end + 1];
        if let Some(href) = tag
            .split("href=\"")
            .nth(1)
            .and_then(|value| value.split('"').next())
        {
            let close = rest[start + end + 1..].find("</a>").unwrap_or(0);
            let label = &rest[start + end + 1..start + end + 1 + close];
            out.push_str(&format!("{} ({})", strip_tags(label), unescape(href)));
            rest = &rest[start + end + 1 + close..];
            rest = rest.strip_prefix("</a>").unwrap_or(rest);
            continue;
        }
        rest = &rest[start + end + 1..];
    }
    out.push_str(rest);
    unescape(&out)
}

fn unescape(text: &str) -> String {
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&amp;", "&")
}

fn link(url: &str, label: &str) -> String {
    format!(
        "<a href=\"{}\" style=\"color:#0f7350\">{}</a>",
        escape(url),
        escape(label)
    )
}

fn alert_footer(config: &AlertsConfig, alert: &MinerAlert) -> Vec<String> {
    let lang = alert.lang.as_str();
    vec![
        escape(&t(lang, "footer", &[&short_account(&alert.account)])),
        link(
            &format!("{}#settings", stats_url(config, &alert.account, lang)),
            &t(lang, "changeAlerts", &[]),
        ),
        "·".to_string(),
        link(
            &unsubscribe_page_url(config, &alert.unsubscribe_token, lang),
            &t(lang, "unsubscribe", &[]),
        ),
    ]
}

fn unsubscribe_page_url(config: &AlertsConfig, token: &str, lang: &str) -> String {
    let site = config.site_url.trim_end_matches('/');
    let lang = if lang == "en" {
        String::new()
    } else {
        format!("&lang={lang}")
    };
    format!("{site}/alerts?unsubscribe={token}{lang}")
}

fn one_click_url(config: &AlertsConfig, token: &str) -> String {
    format!(
        "{}/api/alerts/unsubscribe?token={token}",
        config.site_url.trim_end_matches('/')
    )
}

/// My Stats opened on the account (exchange accounts need their memo, so
/// they open on whatever the browser last looked up).
fn stats_url(config: &AlertsConfig, account: &str, lang: &str) -> String {
    let site = config.site_url.trim_end_matches('/');
    let mut params = Vec::new();
    if !account.starts_with(pool_common::destination::MEMO_ACCOUNT_PREFIX) {
        params.push(format!("miner={}", urlencoding::encode(account)));
    }
    if lang != "en" {
        params.push(format!("lang={lang}"));
    }
    if params.is_empty() {
        format!("{site}/stats")
    } else {
        format!("{site}/stats?{}", params.join("&"))
    }
}

fn stats_button(config: &AlertsConfig, alert: &MinerAlert) -> (String, String) {
    (
        stats_url(config, &alert.account, &alert.lang),
        t(&alert.lang, "openStats", &[]),
    )
}

fn finish(
    config: &AlertsConfig,
    alert: &MinerAlert,
    subject: String,
    email: Email<'_>,
) -> OutgoingMail {
    let (html, text) = email.render();
    let chat = email.chat_text(&alert.account);
    OutgoingMail {
        to: alert.email.clone(),
        subject,
        text,
        html,
        unsubscribe_url: Some(one_click_url(config, &alert.unsubscribe_token)),
        chat: Some(chat),
    }
}

pub(crate) fn confirm_mail(
    config: &AlertsConfig,
    email: &str,
    lang: &str,
    account: &str,
    token: &str,
) -> OutgoingMail {
    let site = config.site_url.trim_end_matches('/');
    let lang_param = if lang == "en" {
        String::new()
    } else {
        format!("&lang={lang}")
    };
    let mail = Email {
        lang,
        title: t(lang, "confirmTitle", &[]),
        paragraphs: vec![escape(&t(lang, "confirmBody", &[&short_account(account)]))],
        facts: Vec::new(),
        button: Some((
            format!("{site}/alerts?confirm={token}{lang_param}"),
            t(lang, "confirmButton", &[]),
        )),
        after: vec![escape(&t(lang, "confirmIgnore", &[]))],
        footer: vec![escape(&t(lang, "confirmExpiry", &[]))],
    };
    let (html, text) = mail.render();
    OutgoingMail {
        to: email.to_string(),
        subject: t(lang, "confirmSubject", &[]),
        text,
        html,
        unsubscribe_url: None,
        chat: None,
    }
}

/// Rig activity: name, last accepted share, recent hashrate.
#[derive(Clone, Debug)]
pub(crate) struct Rig {
    pub name: String,
    pub last_share: SystemTime,
    pub hashrate: f64,
}

pub(crate) fn offline_mail(
    config: &AlertsConfig,
    alert: &MinerAlert,
    stopped: &[Rig],
    mining: &[Rig],
) -> OutgoingMail {
    let lang = alert.lang.as_str();
    let after = duration_text(
        lang,
        Duration::from_secs(u64::from(alert.offline_minutes) * 60),
    );
    let total: f64 = mining.iter().map(|rig| rig.hashrate).sum();
    let mut facts = Vec::new();
    let (subject, title, body) = if let [rig] = stopped {
        let name = rig_name(lang, &rig.name);
        facts.push((
            escape(&t(lang, "lastShare", &[])),
            escape(&utc_time(rig.last_share)),
            utc_time(rig.last_share),
        ));
        facts.push((
            format!("{DOT_OFF}{}", escape(&name)),
            escape(&t(lang, "stopped", &[])),
            t(lang, "stopped", &[]),
        ));
        (
            t(lang, "offlineSubjectOne", &[&name]),
            t(lang, "offlineSubjectOne", &[&name]),
            t(lang, "offlineBodyOne", &[&name, &after]),
        )
    } else {
        for rig in stopped.iter().take(20) {
            let since = t(lang, "since", &[&utc_time(rig.last_share)]);
            facts.push((
                format!("{DOT_OFF}{}", escape(&rig_name(lang, &rig.name))),
                escape(&since),
                since,
            ));
        }
        let count = stopped.len().to_string();
        (
            t(lang, "offlineSubjectMany", &[&count]),
            t(lang, "offlineSubjectMany", &[&count]),
            t(lang, "offlineBodyMany", &[&after]),
        )
    };
    if mining.len() <= 5 {
        for rig in mining {
            let rate = human_rate(rig.hashrate);
            facts.push((
                format!("{DOT_ON}{}", escape(&rig_name(lang, &rig.name))),
                escape(&rate),
                rate,
            ));
        }
    } else {
        let value = t(
            lang,
            "stillMiningValue",
            &[&mining.len().to_string(), &human_rate(total)],
        );
        facts.push((escape(&t(lang, "stillMining", &[])), escape(&value), value));
    }
    let rate = human_rate(total);
    facts.push((escape(&t(lang, "hashrateNow", &[])), escape(&rate), rate));
    let email = Email {
        lang,
        title,
        paragraphs: vec![escape(&body)],
        facts,
        button: Some(stats_button(config, alert)),
        after: Vec::new(),
        footer: alert_footer(config, alert),
    };
    finish(config, alert, subject, email)
}

pub(crate) fn back_mail(
    config: &AlertsConfig,
    alert: &MinerAlert,
    back: &[(Rig, Duration)],
    mining_total: f64,
) -> OutgoingMail {
    let lang = alert.lang.as_str();
    let mut facts = Vec::new();
    for (rig, _) in back.iter().take(20) {
        let rate = human_rate(rig.hashrate);
        facts.push((
            format!("{DOT_ON}{}", escape(&rig_name(lang, &rig.name))),
            escape(&rate),
            rate,
        ));
    }
    let rate = human_rate(mining_total);
    facts.push((escape(&t(lang, "hashrateNow", &[])), escape(&rate), rate));
    let (subject, body) = if let [(rig, away)] = back {
        let name = rig_name(lang, &rig.name);
        (
            t(lang, "backSubjectOne", &[&name]),
            t(lang, "backBodyOne", &[&name, &duration_text(lang, *away)]),
        )
    } else {
        (
            t(lang, "backSubjectMany", &[&back.len().to_string()]),
            t(lang, "backBodyMany", &[]),
        )
    };
    let email = Email {
        lang,
        title: subject.clone(),
        paragraphs: vec![escape(&body)],
        facts,
        button: Some(stats_button(config, alert)),
        after: Vec::new(),
        footer: alert_footer(config, alert),
    };
    finish(config, alert, subject, email)
}

pub(crate) fn payout_mail(
    config: &AlertsConfig,
    alert: &MinerAlert,
    amount: u64,
    fee: u64,
    tx_hash: &str,
    paid_total: u64,
) -> OutgoingMail {
    let lang = alert.lang.as_str();
    let amount_text = format_bnt(amount);
    let short_tx = if tx_hash.len() > 16 {
        format!("{}…{}", &tx_hash[..8], &tx_hash[tx_hash.len() - 4..])
    } else {
        tx_hash.to_string()
    };
    let tx_url = format!("https://explorer.blocknetcrypto.com/tx/{tx_hash}");
    let facts = vec![
        (
            escape(&t(lang, "amount", &[])),
            escape(&format!("{amount_text} BNT")),
            format!("{amount_text} BNT"),
        ),
        (
            escape(&t(lang, "networkFee", &[])),
            escape(&format!("{} BNT", format_bnt(fee))),
            format!("{} BNT", format_bnt(fee)),
        ),
        (
            escape(&t(lang, "transaction", &[])),
            link(&tx_url, &short_tx),
            tx_url.clone(),
        ),
        (
            escape(&t(lang, "paidSoFar", &[])),
            escape(&format!("{} BNT", format_bnt(paid_total))),
            format!("{} BNT", format_bnt(paid_total)),
        ),
    ];
    let email = Email {
        lang,
        title: t(lang, "payoutTitle", &[&amount_text]),
        paragraphs: vec![escape(&t(lang, "payoutBody", &[]))],
        facts,
        button: Some(stats_button(config, alert)),
        after: Vec::new(),
        footer: alert_footer(config, alert),
    };
    finish(
        config,
        alert,
        t(lang, "payoutSubject", &[&amount_text]),
        email,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn solo_mail(
    config: &AlertsConfig,
    alert: &MinerAlert,
    height: i64,
    reward: u64,
    worker: &str,
    confirmations: u64,
    pool_fee_pct: f64,
) -> OutgoingMail {
    let lang = alert.lang.as_str();
    let reward_text = format_bnt(reward);
    let height_text = height.to_string();
    let block_url = format!("https://explorer.blocknetcrypto.com/block/{height}");
    let fee = format!("{}%", format_fee(pool_fee_pct));
    let facts = vec![
        (
            escape(&t(lang, "block", &[])),
            link(&block_url, &height_text),
            block_url.clone(),
        ),
        (
            escape(&t(lang, "reward", &[])),
            escape(&format!("{reward_text} BNT")),
            format!("{reward_text} BNT"),
        ),
        (escape(&t(lang, "poolFee", &[])), escape(&fee), fee.clone()),
    ];
    let name = rig_name(lang, worker);
    let email = Email {
        lang,
        title: t(lang, "soloTitle", &[&height_text]),
        paragraphs: vec![escape(&t(
            lang,
            "soloBody",
            &[&name, &confirmations.to_string()],
        ))],
        facts,
        button: Some(stats_button(config, alert)),
        after: Vec::new(),
        footer: alert_footer(config, alert),
    };
    finish(
        config,
        alert,
        t(lang, "soloSubject", &[&reward_text]),
        email,
    )
}

/// A rig whose hashrate dropped or came back: now and its usual rate.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RateChange {
    pub name: String,
    pub now: f64,
    pub usual: f64,
}

fn rate_facts(lang: &str, rigs: &[RateChange], dot: &str) -> Vec<(String, String, String)> {
    rigs.iter()
        .take(20)
        .map(|rig| {
            let value = t(
                lang,
                "rateNowUsual",
                &[&human_rate(rig.now), &human_rate(rig.usual)],
            );
            (
                format!("{dot}{}", escape(&rig_name(lang, &rig.name))),
                escape(&value),
                value,
            )
        })
        .collect()
}

pub(crate) fn slow_mail(
    config: &AlertsConfig,
    alert: &MinerAlert,
    slow: &[RateChange],
) -> OutgoingMail {
    let lang = alert.lang.as_str();
    let after = duration_text(
        lang,
        Duration::from_secs(u64::from(alert.hashrate_drop_minutes) * 60),
    );
    let (subject, body) = if let [rig] = slow {
        let name = rig_name(lang, &rig.name);
        (
            t(lang, "slowSubjectOne", &[&name]),
            t(lang, "slowBodyOne", &[&name, &after]),
        )
    } else {
        (
            t(lang, "slowSubjectMany", &[&slow.len().to_string()]),
            t(lang, "slowBodyMany", &[&after]),
        )
    };
    let email = Email {
        lang,
        title: subject.clone(),
        paragraphs: vec![escape(&body)],
        facts: rate_facts(lang, slow, DOT_OFF),
        button: Some(stats_button(config, alert)),
        after: Vec::new(),
        footer: alert_footer(config, alert),
    };
    finish(config, alert, subject, email)
}

pub(crate) fn steady_mail(
    config: &AlertsConfig,
    alert: &MinerAlert,
    steady: &[RateChange],
) -> OutgoingMail {
    let lang = alert.lang.as_str();
    let subject = if let [rig] = steady {
        t(lang, "steadySubjectOne", &[&rig_name(lang, &rig.name)])
    } else {
        t(lang, "steadySubjectMany", &[&steady.len().to_string()])
    };
    let email = Email {
        lang,
        title: subject.clone(),
        paragraphs: vec![escape(&t(lang, "steadyBody", &[]))],
        facts: rate_facts(lang, steady, DOT_ON),
        button: Some(stats_button(config, alert)),
        after: Vec::new(),
        footer: alert_footer(config, alert),
    };
    finish(config, alert, subject, email)
}

/// Rigs on an older Seine than `latest`: (worker, version).
pub(crate) fn update_mail(
    config: &AlertsConfig,
    alert: &MinerAlert,
    latest: &str,
    rigs: &[(String, String)],
) -> OutgoingMail {
    let lang = alert.lang.as_str();
    let facts = rigs
        .iter()
        .take(20)
        .map(|(worker, version)| {
            let value = format!("Seine {version}");
            (
                format!("{DOT_OFF}{}", escape(&rig_name(lang, worker))),
                escape(&value),
                value,
            )
        })
        .collect();
    let subject = t(lang, "updateSubject", &[latest]);
    let email = Email {
        lang,
        title: subject.clone(),
        paragraphs: vec![escape(&t(lang, "updateBody", &[latest]))],
        facts,
        button: Some((
            "https://seineminer.com/#download".to_string(),
            t(lang, "updateButton", &[latest]),
        )),
        after: Vec::new(),
        footer: alert_footer(config, alert),
    };
    finish(config, alert, subject, email)
}

const WEEK: Duration = Duration::from_secs(7 * 86_400);

fn utc_date(at: SystemTime) -> String {
    utc_time(at).chars().take(10).collect()
}

pub(crate) fn weekly_mail(
    config: &AlertsConfig,
    alert: &MinerAlert,
    week_start: SystemTime,
    summary: &pool_runtime::miner_settings::WeekSummary,
    unpaid: u64,
) -> OutgoingMail {
    let lang = alert.lang.as_str();
    let first = utc_date(week_start);
    let last = utc_date(week_start + WEEK - Duration::from_secs(1));
    let paid = if summary.payouts == 1 {
        t(lang, "weeklyPaidOne", &[&format_bnt(summary.paid)])
    } else {
        t(
            lang,
            "weeklyPaidMany",
            &[&format_bnt(summary.paid), &summary.payouts.to_string()],
        )
    };
    let unpaid = format!("{} BNT", format_bnt(unpaid));
    let rate = human_rate(summary.work as f64 / WEEK.as_secs_f64());
    let uptime = t(
        lang,
        "weeklyUptimeValue",
        &[&((summary.active_hours.min(168) * 100 + 84) / 168).to_string()],
    );
    let mut facts = vec![
        (
            escape(&t(lang, "weeklyPaid", &[])),
            escape(&paid),
            paid.clone(),
        ),
        (
            escape(&t(lang, "weeklyUnpaid", &[])),
            escape(&unpaid),
            unpaid.clone(),
        ),
        (
            escape(&t(lang, "weeklyHashrate", &[])),
            escape(&rate),
            rate.clone(),
        ),
        (
            escape(&t(lang, "weeklyUptime", &[])),
            escape(&uptime),
            uptime.clone(),
        ),
    ];
    if let Some((worker, work)) = &summary.best_rig {
        let value = format!(
            "{} · {}",
            rig_name(lang, worker),
            human_rate(*work as f64 / WEEK.as_secs_f64())
        );
        facts.push((
            escape(&t(lang, "weeklyBestRig", &[])),
            escape(&value),
            value,
        ));
    }
    let email = Email {
        lang,
        title: t(lang, "weeklyTitle", &[]),
        paragraphs: vec![escape(&t(lang, "weeklyBody", &[&first, &last]))],
        facts,
        button: Some(stats_button(config, alert)),
        after: Vec::new(),
        footer: alert_footer(config, alert),
    };
    finish(config, alert, t(lang, "weeklySubject", &[&first]), email)
}

fn format_fee(pct: f64) -> String {
    let text = format!("{:.2}", (pct * 100.0).round() / 100.0);
    text.trim_end_matches('0').trim_end_matches('.').to_string()
}

// ---- endpoints ----

#[derive(Deserialize, Default)]
struct TokenQuery {
    token: Option<String>,
}

#[derive(Deserialize)]
struct TokenBody {
    token: String,
}

fn valid_token(token: &str) -> bool {
    token.len() == 64 && token.bytes().all(|b| b.is_ascii_hexdigit())
}

fn body_token(query: TokenQuery, body: &Bytes) -> Option<String> {
    query
        .token
        .or_else(|| {
            serde_json::from_slice::<TokenBody>(body)
                .ok()
                .map(|body| body.token)
        })
        .map(|token| token.trim().to_ascii_lowercase())
        .filter(|token| valid_token(token))
}

fn invalid_link() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({"error": "invalid_link", "message": "This link is no longer valid."})),
    )
        .into_response()
}

async fn blocking<T, F>(store: &Arc<PoolStore>, operation: F) -> Result<T>
where
    F: FnOnce(&PoolStore) -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    let store = Arc::clone(store);
    tokio::task::spawn_blocking(move || operation(&store))
        .await
        .map_err(|err| anyhow::anyhow!("join error: {err}"))?
}

async fn handle_confirm(
    State(store): State<Arc<PoolStore>>,
    Query(query): Query<TokenQuery>,
    body: Bytes,
) -> Response {
    let Some(token) = body_token(query, &body) else {
        return invalid_link();
    };
    let hash = token_hash(&token);
    match blocking(&store, move |store| {
        store.confirm_miner_alert(&hash, SystemTime::now())
    })
    .await
    {
        Ok(Some(account)) => {
            tracing::info!(account = %account, "alert email confirmed");
            Json(json!({"ok": true, "account": short_account(&account)})).into_response()
        }
        Ok(None) => invalid_link(),
        Err(err) => {
            tracing::warn!(error = %err, "alert confirm failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// Also the RFC 8058 one-click target (`?token=` with a form body).
async fn handle_unsubscribe(
    State(store): State<Arc<PoolStore>>,
    Query(query): Query<TokenQuery>,
    body: Bytes,
) -> Response {
    let Some(token) = body_token(query, &body) else {
        return invalid_link();
    };
    match blocking(&store, move |store| store.unsubscribe_miner_alert(&token)).await {
        Ok(Some(account)) => {
            tracing::info!(account = %account, "alert email unsubscribed");
            Json(json!({"ok": true, "account": short_account(&account)})).into_response()
        }
        // Already gone counts as done: the person is unsubscribed.
        Ok(None) => Json(json!({"ok": true})).into_response(),
        Err(err) => {
            tracing::warn!(error = %err, "alert unsubscribe failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

/// Mail apps that open the List-Unsubscribe URL instead of posting to it
/// land on the unsubscribe page, which asks for a click.
async fn handle_unsubscribe_get(Query(query): Query<TokenQuery>) -> Response {
    let token = query
        .token
        .map(|token| token.trim().to_ascii_lowercase())
        .filter(|token| valid_token(token))
        .unwrap_or_default();
    axum::response::Redirect::to(&format!("/alerts?unsubscribe={token}")).into_response()
}

pub(crate) fn router<S>(store: Arc<PoolStore>) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/api/alerts/confirm", post(handle_confirm))
        .route(
            "/api/alerts/unsubscribe",
            post(handle_unsubscribe).get(handle_unsubscribe_get),
        )
        .layer(axum::extract::DefaultBodyLimit::max(1024))
        .with_state(store)
}

// ---- worker ----

pub(crate) struct WorkerContext {
    pub store: Arc<PoolStore>,
    pub service: Arc<AlertService>,
    pub pool_fee_pct: f64,
    pub confirmations: u64,
    /// Since when shares have been arriving pool-wide without a gap (set by
    /// the worker). Rigs are only judged once the pool has been up for their
    /// whole threshold, so a recovery is not read as rigs stopping.
    pub pool_up_since: parking_lot::Mutex<Option<SystemTime>>,
    /// The latest Seine release, for update alerts.
    pub miner_release: pool_runtime::miner_release::MinerRelease,
}

pub(crate) fn spawn_worker(context: WorkerContext) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(WORKER_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if let Err(err) = run_cycle(&context, SystemTime::now()).await {
                tracing::warn!(error = %err, "alert cycle failed");
            }
        }
    });
}

/// How long the pool has had shares arriving without a gap.
fn pool_up_for(up_since: Option<SystemTime>, now: SystemTime) -> Duration {
    up_since.map_or(Duration::ZERO, |since| {
        now.duration_since(since).unwrap_or_default()
    })
}

/// One pass over every account with somewhere to send alerts.
pub(crate) async fn run_cycle(context: &WorkerContext, now: SystemTime) -> Result<()> {
    let alerts = blocking(&context.store, |store| store.active_miner_alerts()).await?;
    if alerts.is_empty() {
        return Ok(());
    }
    let pool_alive = blocking(&context.store, move |store| {
        store.any_accepted_share_since(now - POOL_ALIVE_WINDOW)
    })
    .await?;
    let up_since = {
        let mut up = context.pool_up_since.lock();
        if !pool_alive {
            *up = None;
        } else if up.is_none() {
            *up = Some(now);
        }
        *up
    };
    let up_for = pool_up_for(up_since, now);
    for alert in alerts {
        if let Err(err) = run_account(context, &alert, now, up_for).await {
            tracing::warn!(account = %alert.account, error = %err, "alert check failed");
        }
    }
    Ok(())
}

/// Sends one alert to every channel the account has now. `counted` alerts
/// use up the day's allowance (given back if nothing accepted it). True when
/// at least one channel took it; false when over the limit or no channel is
/// left. Deleted webhooks and blocked bots are removed.
async fn deliver(
    context: &WorkerContext,
    alert: &MinerAlert,
    mut mail: OutgoingMail,
    now: SystemTime,
    counted: bool,
) -> Result<bool> {
    let account = alert.account.clone();
    let day = utc_day(now);
    let channels: Option<AlertChannels> = blocking(&context.store, move |store| {
        if counted {
            store.take_alert_send(&account, day, ALERT_DAILY_LIMIT)
        } else {
            Ok(store
                .miner_alert_channels(&account)?
                .filter(AlertChannels::any))
        }
    })
    .await?;
    let Some(channels) = channels else {
        return Ok(false);
    };
    let mut sent = false;
    let mut failure: Option<anyhow::Error> = None;
    if let Some(email) = &channels.email {
        mail.to = email.clone();
        match context.service.mailer.send(&mail).await {
            Ok(()) => sent = true,
            Err(err) => failure = Some(err),
        }
    }
    let chat = mail.chat.clone().unwrap_or_else(|| mail.text.clone());
    if let Some(webhook) = &channels.discord {
        match context.service.chat.discord(webhook, &chat).await {
            Ok(()) => sent = true,
            Err(ChatError::Gone) => {
                tracing::info!(account = %alert.account, "discord webhook gone, removing it");
                let account = alert.account.clone();
                blocking(&context.store, move |store| {
                    store.set_miner_alert_discord(&account, None, true, now)
                })
                .await?;
            }
            Err(ChatError::Failed(err)) => failure = Some(err),
        }
    }
    if let Some(chat_id) = channels.telegram {
        match context.service.chat.telegram(chat_id, &chat).await {
            Ok(()) => sent = true,
            Err(ChatError::Gone) => {
                tracing::info!(account = %alert.account, "telegram chat gone, removing it");
                blocking(&context.store, move |store| {
                    store.clear_miner_alert_telegram_chat(chat_id)
                })
                .await?;
            }
            Err(ChatError::Failed(err)) => failure = Some(err),
        }
    }
    if sent {
        return Ok(true);
    }
    // Nothing accepted it: the day's allowance is not used up.
    if counted {
        let account = alert.account.clone();
        let _ = blocking(&context.store, move |store| {
            store.refund_alert_send(&account, day)
        })
        .await;
    }
    match failure {
        Some(err) => Err(err),
        None => Ok(false),
    }
}

async fn send_counted(
    context: &WorkerContext,
    alert: &MinerAlert,
    mail: OutgoingMail,
    now: SystemTime,
) -> Result<bool> {
    deliver(context, alert, mail, now, true).await
}

async fn run_account(
    context: &WorkerContext,
    alert: &MinerAlert,
    now: SystemTime,
    pool_up_for: Duration,
) -> Result<()> {
    let config = &context.service.config;
    let account = alert.account.clone();

    let offline_threshold = Duration::from_secs(u64::from(alert.offline_minutes) * 60);
    if alert.offline_minutes > 0 && pool_up_for >= offline_threshold {
        let activity_account = account.clone();
        let (activity, known) = blocking(&context.store, move |store| {
            Ok((
                store.alert_worker_activity(
                    &activity_account,
                    now - ALERT_WATCH_WINDOW,
                    now - HASHRATE_WINDOW,
                )?,
                store.alert_offline_workers(&activity_account)?,
            ))
        })
        .await?;
        let threshold = now - offline_threshold;
        let rigs: Vec<Rig> = activity
            .into_iter()
            .map(|(name, last_share, work)| Rig {
                name,
                last_share,
                hashrate: work as f64 / HASHRATE_WINDOW.as_secs_f64(),
            })
            .collect();
        let stopped: Vec<Rig> = rigs
            .iter()
            .filter(|rig| rig.last_share < threshold && !known.contains_key(&rig.name))
            .cloned()
            .collect();
        let back: Vec<(Rig, Duration)> = rigs
            .iter()
            .filter_map(|rig| {
                let since = known.get(&rig.name)?;
                (rig.last_share > *since).then(|| {
                    (
                        rig.clone(),
                        rig.last_share.duration_since(*since).unwrap_or_default(),
                    )
                })
            })
            .collect();
        let mining: Vec<Rig> = rigs
            .iter()
            .filter(|rig| rig.last_share >= threshold)
            .cloned()
            .collect();
        let mining_total: f64 = mining.iter().map(|rig| rig.hashrate).sum();

        if !stopped.is_empty() {
            let mail = offline_mail(config, alert, &stopped, &mining);
            // Over today's limit the rigs are still marked, so the alert is
            // not sent late; a send failure leaves them for the next pass.
            send_counted(context, alert, mail, now).await?;
            let marks: Vec<(String, SystemTime)> = stopped
                .iter()
                .map(|rig| (rig.name.clone(), rig.last_share))
                .collect();
            let mark_account = account.clone();
            blocking(&context.store, move |store| {
                store.mark_alert_workers_offline(&mark_account, &marks)
            })
            .await?;
        }
        if !back.is_empty() {
            let mail = back_mail(config, alert, &back, mining_total);
            send_counted(context, alert, mail, now).await?;
            let names: Vec<String> = back.iter().map(|(rig, _)| rig.name.clone()).collect();
            let clear_account = account.clone();
            blocking(&context.store, move |store| {
                store.clear_alert_workers_offline(&clear_account, &names)
            })
            .await?;
        }
    }

    let drop_window = Duration::from_secs(u64::from(alert.hashrate_drop_minutes) * 60);
    if alert.hashrate_drop_minutes > 0 && pool_up_for >= drop_window {
        check_hashrate(context, alert, now, drop_window).await?;
    }

    if alert.notify_updates {
        check_updates(context, alert, now).await?;
    }

    if alert.weekly_summary {
        check_week(context, alert, now).await?;
    }

    let payout_account = account.clone();
    let payout_cursor = alert.payout_cursor;
    let payouts = blocking(&context.store, move |store| {
        store.alert_payouts_after(&payout_account, payout_cursor)
    })
    .await?;
    let mut new_payout_cursor = alert.payout_cursor;
    if !payouts.is_empty() {
        let paid_account = account.clone();
        let paid_total = blocking(&context.store, move |store| {
            Ok(store.get_balance(&paid_account)?.paid)
        })
        .await?;
        for (id, amount, fee, tx_hash, _) in &payouts {
            if alert.notify_payouts {
                let mail = payout_mail(config, alert, *amount, *fee, tx_hash, paid_total);
                if let Err(err) = send_counted(context, alert, mail, now).await {
                    // Keep the cursor before this payout to retry it.
                    save_cursors(context, &account, new_payout_cursor, alert.block_cursor).await?;
                    return Err(err);
                }
            }
            new_payout_cursor = *id;
        }
    }

    let block_account = account.clone();
    let block_cursor = alert.block_cursor;
    let blocks = blocking(&context.store, move |store| {
        store.alert_solo_blocks_after(&block_account, block_cursor)
    })
    .await?;
    let mut new_block_cursor = alert.block_cursor;
    for (height, _, reward, worker) in &blocks {
        if alert.notify_solo_blocks {
            let mail = solo_mail(
                config,
                alert,
                *height,
                *reward,
                worker,
                context.confirmations,
                context.pool_fee_pct,
            );
            if let Err(err) = send_counted(context, alert, mail, now).await {
                save_cursors(context, &account, new_payout_cursor, new_block_cursor).await?;
                return Err(err);
            }
        }
        new_block_cursor = *height;
    }
    if new_payout_cursor != alert.payout_cursor || new_block_cursor != alert.block_cursor {
        save_cursors(context, &account, new_payout_cursor, new_block_cursor).await?;
    }
    Ok(())
}

/// What the hashrate check found for one rig.
#[derive(Debug, PartialEq)]
pub(crate) enum RateVerdict {
    /// Below [`ALERT_DROP_RATIO`] of its usual rate for the whole window.
    Dropped(RateChange),
    /// Back to at least [`ALERT_RECOVER_RATIO`] of its usual rate.
    Steady(RateChange),
    /// Stopped sending shares (the stopped-mining alert covers that).
    Stopped,
    /// Not enough history, or in between.
    Unclear,
}

/// Judges one rig: its rate over the last `window` against its average over
/// the [`ALERT_DROP_BASELINE`] before that.
pub(crate) fn rate_verdict(
    rates: &pool_runtime::miner_settings::RigRates,
    now: SystemTime,
    window: Duration,
) -> RateVerdict {
    let recent_since = now - window;
    if rates.last_share < now - HASHRATE_WINDOW {
        return RateVerdict::Stopped;
    }
    let baseline_since = recent_since - ALERT_DROP_BASELINE;
    let history_start = rates.first_share.max(baseline_since);
    let history = recent_since
        .duration_since(history_start)
        .unwrap_or_default();
    if history < ALERT_DROP_MIN_HISTORY || rates.baseline_work == 0 {
        return RateVerdict::Unclear;
    }
    let usual = rates.baseline_work as f64 / history.as_secs_f64();
    let now_rate = rates.recent_work as f64 / window.as_secs_f64();
    let change = RateChange {
        name: rates.worker.clone(),
        now: now_rate,
        usual,
    };
    if now_rate < usual * ALERT_DROP_RATIO {
        RateVerdict::Dropped(change)
    } else if now_rate >= usual * ALERT_RECOVER_RATIO {
        RateVerdict::Steady(change)
    } else {
        RateVerdict::Unclear
    }
}

async fn check_hashrate(
    context: &WorkerContext,
    alert: &MinerAlert,
    now: SystemTime,
    window: Duration,
) -> Result<()> {
    let config = &context.service.config;
    let account = alert.account.clone();
    let (rates, known) = blocking(&context.store, move |store| {
        Ok((
            store.alert_worker_rates(&account, now - window - ALERT_DROP_BASELINE, now - window)?,
            store.alert_slow_workers(&account)?,
        ))
    })
    .await?;
    let mut dropped = Vec::new();
    let mut steady = Vec::new();
    let mut forget = Vec::new();
    for rig in &rates {
        match rate_verdict(rig, now, window) {
            RateVerdict::Dropped(change) if !known.contains_key(&rig.worker) => {
                dropped.push(change)
            }
            RateVerdict::Steady(change) if known.contains_key(&rig.worker) => steady.push(change),
            RateVerdict::Stopped if known.contains_key(&rig.worker) => {
                forget.push(rig.worker.clone())
            }
            _ => {}
        }
    }
    // Rigs gone from the window entirely are forgotten too.
    forget.extend(
        known
            .keys()
            .filter(|worker| !rates.iter().any(|rig| &rig.worker == *worker))
            .cloned(),
    );
    if !dropped.is_empty() {
        send_counted(context, alert, slow_mail(config, alert, &dropped), now).await?;
        let names: Vec<String> = dropped.iter().map(|rig| rig.name.clone()).collect();
        let account = alert.account.clone();
        blocking(&context.store, move |store| {
            store.mark_alert_workers_slow(&account, &names, now)
        })
        .await?;
    }
    if !steady.is_empty() {
        send_counted(context, alert, steady_mail(config, alert, &steady), now).await?;
        forget.extend(steady.iter().map(|rig| rig.name.clone()));
    }
    if !forget.is_empty() {
        let account = alert.account.clone();
        blocking(&context.store, move |store| {
            store.clear_alert_workers_slow(&account, &forget)
        })
        .await?;
    }
    Ok(())
}

/// Seine versions as (major, minor, patch); `None` for anything else.
fn seine_version(raw: &str) -> Option<(u64, u64, u64)> {
    let trimmed = raw.trim().trim_start_matches('v');
    let core = trimmed.split(['-', '+']).next().unwrap_or(trimmed);
    let mut parts = core.split('.').map(|part| part.parse::<u64>().ok());
    let version = (parts.next()??, parts.next()??, parts.next()??);
    parts.next().is_none().then_some(version)
}

/// Rigs that mined in the last hour on a version older than `latest`.
pub(crate) fn outdated_rigs(latest: &str, versions: &[(String, String)]) -> Vec<(String, String)> {
    let Some(latest) = seine_version(latest) else {
        return Vec::new();
    };
    let mut rigs: Vec<(String, String)> = versions
        .iter()
        .filter(|(_, version)| seine_version(version).is_some_and(|version| version < latest))
        .cloned()
        .collect();
    rigs.sort();
    rigs
}

async fn check_updates(context: &WorkerContext, alert: &MinerAlert, now: SystemTime) -> Result<()> {
    let latest = context.miner_release.version();
    if seine_version(&latest).is_none() || alert.update_notified == latest {
        return Ok(());
    }
    // Versions only change on reconnect; every ten minutes is plenty.
    let minute = now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() / 60;
    if !minute.is_multiple_of(10) {
        return Ok(());
    }
    let account = alert.account.clone();
    let versions = blocking(&context.store, move |store| {
        store.alert_worker_versions(&account, now - Duration::from_secs(3600))
    })
    .await?;
    let rigs = outdated_rigs(&latest, &versions);
    if rigs.is_empty() {
        return Ok(());
    }
    let mail = update_mail(&context.service.config, alert, &latest, &rigs);
    // Over the day's limit the release still counts as told.
    send_counted(context, alert, mail, now).await?;
    let account = alert.account.clone();
    blocking(&context.store, move |store| {
        store.set_alert_update_notified(&account, &latest)
    })
    .await
}

async fn check_week(context: &WorkerContext, alert: &MinerAlert, now: SystemTime) -> Result<()> {
    let week = utc_week(now);
    if alert.summary_week >= week {
        return Ok(());
    }
    let account = alert.account.clone();
    let first = alert.summary_week == 0;
    let taken = blocking(&context.store, move |store| {
        store.take_alert_summary_week(&account, week)
    })
    .await?;
    // The first week starts from when the summary was turned on.
    if !taken || first {
        return Ok(());
    }
    let start = utc_week_start(week - 1);
    let account = alert.account.clone();
    let (summary, unpaid) = blocking(&context.store, move |store| {
        Ok((
            store.alert_week_summary(&account, start, start + WEEK)?,
            store.get_balance(&account)?.pending,
        ))
    })
    .await?;
    if summary.work == 0 && summary.paid == 0 {
        return Ok(());
    }
    let mail = weekly_mail(&context.service.config, alert, start, &summary, unpaid);
    deliver(context, alert, mail, now, false).await?;
    Ok(())
}

async fn save_cursors(
    context: &WorkerContext,
    account: &str,
    payout: i64,
    block: i64,
) -> Result<()> {
    let account = account.to_string();
    blocking(&context.store, move |store| {
        store.advance_alert_cursors(&account, payout, block)
    })
    .await
}

#[cfg(test)]
pub(crate) fn sample_alert(account: &str) -> MinerAlert {
    MinerAlert {
        account: account.to_string(),
        email: "miner@example.com".into(),
        lang: "en".into(),
        confirmed_at: Some(UNIX_EPOCH),
        confirm_sent_at: UNIX_EPOCH,
        unsubscribe_token: "a".repeat(64),
        offline_minutes: 30,
        notify_payouts: true,
        notify_solo_blocks: false,
        hashrate_drop_minutes: 30,
        notify_updates: true,
        weekly_summary: true,
        discord_webhook: None,
        telegram_chat_id: None,
        telegram_name: None,
        telegram_pending: false,
        update_notified: String::new(),
        summary_week: 0,
        payout_cursor: 0,
        block_cursor: 0,
        day: 0,
        sent_today: 0,
    }
}

/// Every email kind rendered with sample data, for the showcase.
#[cfg(test)]
pub(crate) fn sample_mails(config: &AlertsConfig) -> Vec<(&'static str, OutgoingMail)> {
    let alert = sample_alert("8yAUM9aprE1ffiEm5KugyGH1TUqXQTuF9j5Mt2ZabBJmM9HAb5bfPkpa5JjgGQHU8bAmsuuJrcJVSXNWt5KLVn7Tv1bVm");
    let at = UNIX_EPOCH + Duration::from_secs(1_793_851_920);
    let rig = |name: &str, rate: f64| Rig {
        name: name.into(),
        last_share: at,
        hashrate: rate,
    };
    vec![
        (
            "confirm",
            confirm_mail(
                config,
                "miner@example.com",
                "en",
                &alert.account,
                &"b".repeat(64),
            ),
        ),
        (
            "offline",
            offline_mail(
                config,
                &alert,
                &[rig("rig-1", 0.0)],
                &[rig("laptop", 111.3)],
            ),
        ),
        (
            "offline-many",
            offline_mail(
                config,
                &alert,
                &[rig("rig-1", 0.0), rig("rig-2", 0.0), rig("basement", 0.0)],
                &[
                    rig("a", 300.0),
                    rig("b", 300.0),
                    rig("c", 300.0),
                    rig("d", 300.0),
                    rig("e", 200.0),
                    rig("f", 100.0),
                ],
            ),
        ),
        (
            "back",
            back_mail(
                config,
                &alert,
                &[(rig("rig-1", 201.1), Duration::from_secs(72 * 60))],
                312.4,
            ),
        ),
        (
            "payout",
            payout_mail(
                config,
                &alert,
                1_245_000_000,
                28_000,
                "9b31a063aa11bb22cc33dd44ee55ff66e4c2",
                4_865_000_000,
            ),
        ),
        (
            "solo",
            solo_mail(config, &alert, 72_140, 43_946_080_260, "rig-1", 60, 0.0),
        ),
        (
            "slow",
            slow_mail(
                config,
                &alert,
                &[RateChange {
                    name: "rig-1".into(),
                    now: 88.0,
                    usual: 201.1,
                }],
            ),
        ),
        (
            "steady",
            steady_mail(
                config,
                &alert,
                &[RateChange {
                    name: "rig-1".into(),
                    now: 198.4,
                    usual: 201.1,
                }],
            ),
        ),
        (
            "update",
            update_mail(
                config,
                &alert,
                "0.3.2",
                &[
                    ("laptop".into(), "0.3.1".into()),
                    ("rig-1".into(), "0.2.26".into()),
                ],
            ),
        ),
        (
            "weekly",
            weekly_mail(
                config,
                &alert,
                pool_runtime::miner_settings::utc_week_start(2965),
                &pool_runtime::miner_settings::WeekSummary {
                    work: 312 * 7 * 86_400,
                    active_hours: 161,
                    best_rig: Some(("rig-1".into(), 201 * 7 * 86_400)),
                    paid: 4_865_000_000,
                    payouts: 3,
                },
                42_000_000,
            ),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_language_has_every_message_and_placeholders_match() {
        let catalog = catalog();
        let en = &catalog["en"];
        for (lang, messages) in catalog {
            for (key, value) in en {
                let translated = messages
                    .get(key)
                    .unwrap_or_else(|| panic!("{lang} lacks {key}"));
                for index in 0..4 {
                    let placeholder = format!("{{{index}}}");
                    assert_eq!(
                        value.contains(&placeholder),
                        translated.contains(&placeholder),
                        "{lang}.{key} placeholder {placeholder}"
                    );
                }
            }
            assert_eq!(messages.len(), en.len(), "{lang} has extra keys");
        }
        assert_eq!(supported_lang("xx"), "en");
    }

    #[test]
    fn rig_names_are_escaped_in_html_and_plain_in_text() {
        let config = AlertsConfig::default();
        let alert = sample_alert("acct");
        let rig = Rig {
            name: "<b>evil</b>".into(),
            last_share: UNIX_EPOCH,
            hashrate: 0.0,
        };
        let mail = offline_mail(&config, &alert, &[rig], &[]);
        assert!(!mail.html.contains("<b>evil</b>"));
        assert!(mail.html.contains("&lt;b&gt;evil&lt;/b&gt;"));
        assert!(mail.text.contains("<b>evil</b> stopped mining"));
        assert_eq!(mail.subject, "<b>evil</b> stopped mining");
        assert!(mail
            .unsubscribe_url
            .as_deref()
            .unwrap()
            .ends_with(&"a".repeat(64)));
        assert!(mail
            .text
            .contains("Unsubscribe (https://bntpool.com/alerts?unsubscribe="));
    }

    fn rates(
        first_ago: u64,
        last_ago: u64,
        baseline: u64,
        recent: u64,
    ) -> (pool_runtime::miner_settings::RigRates, SystemTime) {
        let now = UNIX_EPOCH + Duration::from_secs(1_800_000_000);
        (
            pool_runtime::miner_settings::RigRates {
                worker: "rig-1".into(),
                first_share: now - Duration::from_secs(first_ago),
                last_share: now - Duration::from_secs(last_ago),
                baseline_work: baseline,
                recent_work: recent,
            },
            now,
        )
    }

    #[test]
    fn hashrate_drops_need_history_and_a_halved_rate() {
        let window = Duration::from_secs(30 * 60);
        // 6 h of history at 100 H/s before the window.
        let usual = 100 * 6 * 3600;
        let (rig, now) = rates(7 * 3600, 10, usual, 40 * 1800);
        assert!(
            matches!(rate_verdict(&rig, now, window), RateVerdict::Dropped(ref change)
            if (change.usual - 100.0).abs() < 1e-9 && (change.now - 40.0).abs() < 1e-9)
        );
        let (rig, now) = rates(7 * 3600, 10, usual, 90 * 1800);
        assert!(matches!(
            rate_verdict(&rig, now, window),
            RateVerdict::Steady(_)
        ));
        let (rig, now) = rates(7 * 3600, 10, usual, 65 * 1800);
        assert_eq!(rate_verdict(&rig, now, window), RateVerdict::Unclear);
        // Under an hour of history before the window: not judged.
        let (rig, now) = rates(30 * 60 + 50 * 60, 10, 100 * 3000, 0);
        assert_eq!(rate_verdict(&rig, now, window), RateVerdict::Unclear);
        // No share for a while: the stopped alert covers it.
        let (rig, now) = rates(7 * 3600, 20 * 60, usual, 0);
        assert_eq!(rate_verdict(&rig, now, window), RateVerdict::Stopped);
    }

    #[test]
    fn rigs_on_older_seine_releases_are_listed() {
        let versions = vec![
            ("b".to_string(), "0.3.1".to_string()),
            ("a".to_string(), "v0.2.26".to_string()),
            ("c".to_string(), "0.3.2".to_string()),
            ("d".to_string(), "0.4.0-rc.1".to_string()),
            ("e".to_string(), "custom".to_string()),
        ];
        assert_eq!(
            outdated_rigs("0.3.2", &versions),
            vec![
                ("a".to_string(), "v0.2.26".to_string()),
                ("b".to_string(), "0.3.1".to_string())
            ]
        );
        assert!(outdated_rigs("", &versions).is_empty());
    }

    #[test]
    fn chat_messages_carry_the_facts_and_the_stats_link() {
        let config = AlertsConfig::default();
        let alert = sample_alert("8yAUM9aprE1ffiEm5KugyGH1TUqXQTuF9j5Mt2ZabBJmM9HAb5bfPkpa5JjgGQHU8bAmsuuJrcJVSXNWt5KLVn7Tv1bVm");
        let mail = payout_mail(&config, &alert, 1_245_000_000, 28_000, "abc", 4_865_000_000);
        let chat = mail.chat.expect("chat text");
        assert!(chat.starts_with("12.45 BNT is on its way\n"), "{chat}");
        assert!(chat.contains("Amount: 12.45 BNT"), "{chat}");
        assert!(
            chat.contains("Open My Stats: https://bntpool.com/stats?miner=8yAUM9"),
            "{chat}"
        );
        assert!(
            chat.ends_with("bntpool.com alerts for 8yAUM9…1bVm"),
            "{chat}"
        );
        assert!(!chat.contains('<'));
        assert!(confirm_mail(&config, "a@b.co", "en", &alert.account, "t")
            .chat
            .is_none());
    }

    #[test]
    fn weekly_summaries_round_uptime_and_name_the_best_rig() {
        let config = AlertsConfig::default();
        let alert = sample_alert("acct");
        let summary = pool_runtime::miner_settings::WeekSummary {
            work: 300 * 7 * 86_400,
            active_hours: 161,
            best_rig: Some(("rig-1".into(), 200 * 7 * 86_400)),
            paid: 4_865_000_000,
            payouts: 3,
        };
        let start = pool_runtime::miner_settings::utc_week_start(2965);
        let mail = weekly_mail(&config, &alert, start, &summary, 42_000_000);
        assert_eq!(mail.subject, "Your week on bntpool from 2026-10-26");
        assert!(
            mail.text.contains("2026-10-26 to 2026-11-01, UTC."),
            "{}",
            mail.text
        );
        assert!(mail.text.contains("Paid out: 48.65 BNT in 3 payouts"));
        assert!(mail.text.contains("Unpaid now: 0.42 BNT"));
        assert!(mail.text.contains("Average hashrate: 300.00 H/s"));
        assert!(mail.text.contains("Mining time: 96% of the week"));
        assert!(mail.text.contains("Best rig: rig-1 · 200.00 H/s"));
    }

    #[test]
    fn numbers_and_times_read_like_the_site() {
        assert_eq!(format_bnt(1_245_000_000), "12.45");
        assert_eq!(format_bnt(28_000), "0.00028");
        assert_eq!(format_bnt(1_000_000_000_000), "10,000.00");
        assert_eq!(human_rate(1_234.5), "1.23 KH/s");
        assert_eq!(human_rate(0.0), "0 H/s");
        assert_eq!(
            utc_time(UNIX_EPOCH + Duration::from_secs(1_793_851_920)),
            "2026-11-05 04:12 UTC"
        );
        assert_eq!(
            duration_text("en", Duration::from_secs(72 * 60)),
            "1 h 12 min"
        );
        assert_eq!(duration_text("en", Duration::from_secs(30 * 60)), "30 min");
        assert_eq!(short_account("8yAUM9aprE1ffiEm5Kug1bVm"), "8yAUM9…1bVm");
    }

    #[test]
    fn tokens_from_query_or_json() {
        let token = "c".repeat(64);
        assert_eq!(
            body_token(
                TokenQuery {
                    token: Some(token.clone())
                },
                &Bytes::from_static(b"List-Unsubscribe=One-Click")
            ),
            Some(token.clone())
        );
        let body = Bytes::from(format!("{{\"token\":\"{token}\"}}"));
        assert_eq!(body_token(TokenQuery::default(), &body), Some(token));
        assert_eq!(
            body_token(
                TokenQuery {
                    token: Some("short".into())
                },
                &Bytes::new()
            ),
            None
        );
    }

    /// Writes every email to $ALERT_SAMPLES_DIR for the showcase.
    #[test]
    fn write_sample_mails_when_asked() {
        let Ok(dir) = std::env::var("ALERT_SAMPLES_DIR") else {
            return;
        };
        for (name, mail) in sample_mails(&AlertsConfig::default()) {
            std::fs::write(format!("{dir}/{name}.html"), &mail.html).unwrap();
            std::fs::write(
                format!("{dir}/{name}.txt"),
                format!("Subject: {}\n\n{}", mail.subject, mail.text),
            )
            .unwrap();
            if let Some(chat) = &mail.chat {
                std::fs::write(format!("{dir}/{name}.chat.txt"), chat).unwrap();
            }
        }
    }
}
