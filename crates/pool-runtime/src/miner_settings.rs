//! Per-account miner settings changed from My Stats.
//!
//! An account (a miner address, or a `memo:` account) has no settings row
//! until someone sets a password for it. The first password is open to
//! anyone: addresses are public, so nothing proves ownership up front.
//! Instead, a network that has mined more for the account than the network
//! that set the current password can take the account back (see
//! [`take_back_decision`]). Settings are limited to what a squatter could
//! only use to annoy the owner: the payout address can never change here.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Atomic units per BNT.
pub const ATOMIC_PER_BNT: u64 = 100_000_000;

/// Highest minimum payout a miner may choose when the pool sets no limit on
/// what one payout may send.
pub const FALLBACK_MAX_MIN_PAYOUT_ATOMIC: u64 = 100_000 * ATOMIC_PER_BNT;

/// Work older than this does not count toward a take-back.
pub const TAKE_BACK_WINDOW: Duration = Duration::from_secs(7 * 86_400);

/// After a take-back, the account cannot be taken back again for this long.
pub const TAKE_BACK_COOLDOWN: Duration = Duration::from_secs(86_400);

/// Source work rows older than this are pruned.
pub const SOURCE_WORK_RETENTION: Duration = Duration::from_secs(8 * 86_400);

/// Source work is summed per hour.
pub const SOURCE_WORK_BUCKET_SECS: i64 = 3_600;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MinerSettings {
    /// Atomic units; `None` keeps the pool's minimum.
    pub min_payout: Option<u64>,
    /// Logins without a `solo:` prefix mine solo.
    pub default_solo: bool,
    /// Public miner stats show "Rig 1", "Rig 2" instead of worker names.
    pub hide_workers: bool,
    /// Visitors see no hashrate, balance, payouts or rigs at all.
    pub private_stats: bool,
    /// Share difficulty this account's rigs start at; `None` lets the pool
    /// choose. Vardiff still adjusts it after login.
    pub start_difficulty: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinerSettingsRecord {
    pub account: String,
    /// PHC string. `None` right after a take-back, until a new one is set.
    pub password_hash: Option<String>,
    /// IP group that set the password or last took the account back. A
    /// take-back has to beat this group's recent work.
    pub owner_ip_group: String,
    pub claimed_at: SystemTime,
    pub changed_at: SystemTime,
    pub taken_back_at: Option<SystemTime>,
    pub settings: MinerSettings,
}

/// Hour bucket (unix seconds, floored) for `at`.
pub fn source_work_hour(at: SystemTime) -> i64 {
    let secs = at
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .min(i64::MAX as u64) as i64;
    secs - secs.rem_euclid(SOURCE_WORK_BUCKET_SECS)
}

/// Highest minimum payout a miner may choose: the most one payout may send
/// (the smaller of the per-recipient and per-run limits; 0 means no limit),
/// so a chosen minimum can always be paid, and never below the pool minimum.
pub fn max_min_payout(pool_minimum: u64, per_recipient_limit: u64, per_run_limit: u64) -> u64 {
    [per_recipient_limit, per_run_limit]
        .into_iter()
        .filter(|limit| *limit > 0)
        .min()
        .unwrap_or(FALLBACK_MAX_MIN_PAYOUT_ATOMIC)
        .max(pool_minimum)
}

/// Checks a requested minimum payout against the account's pool minimum and
/// the highest allowed minimum (see [`max_min_payout`]).
pub fn validate_min_payout(requested: u64, pool_minimum: u64, maximum: u64) -> Result<u64, String> {
    if requested < pool_minimum {
        return Err(format!(
            "minimum payout must be at least {}",
            format_bnt(pool_minimum)
        ));
    }
    let maximum = maximum.max(pool_minimum);
    if requested > maximum {
        return Err(format!(
            "minimum payout must be at most {}",
            format_bnt(maximum)
        ));
    }
    Ok(requested)
}

/// The minimum payout that applies: the miner's choice, never below the
/// account's pool minimum. Exchange (memo) accounts keep their higher pool
/// minimum as a floor so an exchange is not sent many small deposits.
pub fn effective_min_payout(pool_minimum: u64, chosen: Option<u64>) -> u64 {
    chosen.map_or(pool_minimum, |chosen| chosen.max(pool_minimum))
}

fn format_bnt(atomic: u64) -> String {
    let whole = atomic / ATOMIC_PER_BNT;
    let frac = atomic % ATOMIC_PER_BNT;
    if frac == 0 {
        format!("{whole} BNT")
    } else {
        let frac = format!("{frac:08}");
        format!("{whole}.{} BNT", frac.trim_end_matches('0'))
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TakeBack {
    /// The claimant may set a new password. `share` is the claimant's part of
    /// the account's work in the window, from 0 to 1.
    Allowed { share: f64 },
    /// The claimant's group did no more work than the owner's.
    NotEnoughWork { share: f64 },
    /// A take-back happened too recently; retry after this long.
    CoolingDown(Duration),
}

/// Decides a take-back from per-group work over [`TAKE_BACK_WINDOW`].
///
/// Every take-back waits out [`TAKE_BACK_COOLDOWN`] after the last one, so
/// knowing a rig's IP cannot be replayed. Then the claimant needs some work,
/// and strictly more than the group recorded as owner (ties keep the owner);
/// the owner's own group may reset a forgotten password while it mines.
pub fn take_back_decision(
    record: &MinerSettingsRecord,
    claimant_group: &str,
    work_by_group: &HashMap<String, u64>,
    now: SystemTime,
) -> TakeBack {
    if let Some(at) = record.taken_back_at {
        let elapsed = now.duration_since(at).unwrap_or_default();
        if elapsed < TAKE_BACK_COOLDOWN {
            return TakeBack::CoolingDown(TAKE_BACK_COOLDOWN - elapsed);
        }
    }
    let total: u64 = work_by_group.values().copied().fold(0, u64::saturating_add);
    let claimant = work_by_group.get(claimant_group).copied().unwrap_or(0);
    let share = if total == 0 {
        0.0
    } else {
        claimant as f64 / total as f64
    };
    if claimant_group == record.owner_ip_group {
        return if claimant > 0 {
            TakeBack::Allowed { share }
        } else {
            TakeBack::NotEnoughWork { share }
        };
    }
    let owner = work_by_group
        .get(&record.owner_ip_group)
        .copied()
        .unwrap_or(0);
    if claimant > 0 && claimant > owner {
        TakeBack::Allowed { share }
    } else {
        TakeBack::NotEnoughWork { share }
    }
}

/// Choices for alert emails offered on My Stats.
pub const ALERT_OFFLINE_MINUTES: [u32; 3] = [10, 30, 60];

/// Most alert emails sent for one account per UTC day.
pub const ALERT_DAILY_LIMIT: u32 = 10;

/// Rigs seen in this window are watched for stopping.
pub const ALERT_WATCH_WINDOW: Duration = Duration::from_secs(2 * 3600);

/// How long a confirmation link works.
pub const ALERT_CONFIRM_TTL: Duration = Duration::from_secs(86_400);

/// Alert email set for an account on My Stats.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MinerAlert {
    pub account: String,
    pub email: String,
    /// Site language when the email was set; emails use it.
    pub lang: String,
    /// `None` until the confirmation link is opened. Nothing but the
    /// confirmation email is sent before then.
    pub confirmed_at: Option<SystemTime>,
    pub confirm_sent_at: SystemTime,
    pub unsubscribe_token: String,
    /// 0 turns rig-stopped alerts off.
    pub offline_minutes: u32,
    pub notify_payouts: bool,
    pub notify_solo_blocks: bool,
    /// 0 turns hashrate-drop alerts off; otherwise how long a rig must stay
    /// below half its usual hashrate.
    pub hashrate_drop_minutes: u32,
    /// Tell the miner when a rig runs an older Seine than the latest release.
    pub notify_updates: bool,
    pub weekly_summary: bool,
    /// Discord channel webhook alerts are posted to.
    pub discord_webhook: Option<String>,
    /// Telegram chat alerts are sent to, and how to show it.
    pub telegram_chat_id: Option<i64>,
    pub telegram_name: Option<String>,
    /// A Telegram link is waiting for Start in the bot.
    pub telegram_pending: bool,
    /// Latest Seine release the miner was already told about.
    pub update_notified: String,
    /// Monday-based UTC week (see [`utc_week`]) of the last weekly summary.
    pub summary_week: i64,
    /// Highest payout id and block height already considered.
    pub payout_cursor: i64,
    pub block_cursor: i64,
    /// UTC day (days since the epoch) `sent_today` counts.
    pub day: i64,
    pub sent_today: u32,
}

/// Alert choices saved from My Stats.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AlertChoices {
    pub offline_minutes: u32,
    pub notify_payouts: bool,
    pub notify_solo_blocks: bool,
    pub hashrate_drop_minutes: u32,
    pub notify_updates: bool,
    pub weekly_summary: bool,
}

/// Where an account's alerts go: a confirmed email, a Discord webhook and a
/// Telegram chat, each optional.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AlertChannels {
    pub email: Option<String>,
    pub discord: Option<String>,
    pub telegram: Option<i64>,
}

impl AlertChannels {
    pub fn any(&self) -> bool {
        self.email.is_some() || self.discord.is_some() || self.telegram.is_some()
    }
}

/// One rig's accepted work over an alert window (see `alert_worker_rates`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RigRates {
    pub worker: String,
    pub first_share: SystemTime,
    pub last_share: SystemTime,
    /// Accepted difficulty from the baseline start until the recent window.
    pub baseline_work: u64,
    /// Accepted difficulty in the recent window.
    pub recent_work: u64,
}

/// An account's week for the weekly summary.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WeekSummary {
    /// Accepted difficulty.
    pub work: u64,
    /// UTC hours with at least one accepted share.
    pub active_hours: u64,
    /// The rig with the most accepted difficulty.
    pub best_rig: Option<(String, u64)>,
    /// Atomic units paid in the week, and how many payouts.
    pub paid: u64,
    pub payouts: u64,
}

/// Days since the epoch, in UTC.
pub fn utc_day(at: SystemTime) -> i64 {
    (at.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() / 86_400) as i64
}

/// Monday-based UTC week number: weeks since Monday 1969-12-29.
pub fn utc_week(at: SystemTime) -> i64 {
    (utc_day(at) + 3).div_euclid(7)
}

/// Midnight UTC on the Monday that starts `week`.
pub fn utc_week_start(week: i64) -> SystemTime {
    let days = (week * 7 - 3).max(0) as u64;
    UNIX_EPOCH + Duration::from_secs(days * 86_400)
}

/// A saved starting difficulty that applies in the current difficulty era
/// (`initial`..=`max`); `None` for one outside it, e.g. saved before a fork
/// changed the difficulty scale.
pub fn start_difficulty_in_era(chosen: u64, initial: u64, max: u64) -> Option<u64> {
    (initial..=max.max(initial))
        .contains(&chosen)
        .then_some(chosen)
}

/// How long a rig must stay below half its usual hashrate, in minutes.
/// Shares arrive about twice a minute, so shorter windows would read
/// ordinary gaps between shares as drops.
pub const ALERT_DROP_MINUTES: [u32; 3] = [30, 60, 120];

/// A rig's usual hashrate is its average over this window, before the
/// minutes being judged.
pub const ALERT_DROP_BASELINE: Duration = Duration::from_secs(6 * 3600);

/// Rigs with less history than this in the baseline are not judged.
pub const ALERT_DROP_MIN_HISTORY: Duration = Duration::from_secs(3600);

/// Below this part of the usual hashrate a rig has dropped...
pub const ALERT_DROP_RATIO: f64 = 0.5;

/// ...and at or above this part it is back.
pub const ALERT_RECOVER_RATIO: f64 = 0.8;

/// How long a Connect Telegram link works.
pub const TELEGRAM_LINK_TTL: Duration = Duration::from_secs(30 * 60);

/// A Discord channel webhook URL, the only kind of Discord link accepted.
pub fn discord_webhook_valid(url: &str) -> bool {
    let Some(rest) = [
        "https://discord.com/api/webhooks/",
        "https://discordapp.com/api/webhooks/",
        "https://canary.discord.com/api/webhooks/",
        "https://ptb.discord.com/api/webhooks/",
    ]
    .iter()
    .find_map(|prefix| url.strip_prefix(prefix)) else {
        return false;
    };
    let Some((id, token)) = rest.split_once('/') else {
        return false;
    };
    url.len() <= 200
        && !id.is_empty()
        && id.len() <= 25
        && id.bytes().all(|b| b.is_ascii_digit())
        && (20..=100).contains(&token.len())
        && token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// "discord.com/api/webhooks/1204…3qXw" for showing a saved webhook.
pub fn discord_webhook_display(url: &str) -> String {
    let rest = url.strip_prefix("https://").unwrap_or(url);
    let Some((path, token)) = rest.rsplit_once('/') else {
        return rest.to_string();
    };
    let Some((base, id)) = path.rsplit_once('/') else {
        return rest.to_string();
    };
    let id: String = id.chars().take(4).collect();
    let tail: String = token
        .chars()
        .rev()
        .take(4)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("{base}/{id}…{tail}")
}

/// A plausible email address: one `@`, a dotted domain, no spaces or
/// control characters, at most 254 bytes. Delivery is proven by the
/// confirmation link, not by this check.
pub fn plausible_email(email: &str) -> bool {
    let email = email.trim();
    if email.is_empty()
        || email.len() > 254
        || email.chars().any(|c| c.is_whitespace() || c.is_control())
    {
        return false;
    }
    let Some((local, domain)) = email.rsplit_once('@') else {
        return false;
    };
    !local.is_empty()
        && local.len() <= 64
        && !local.contains('@')
        && !email.contains(['<', '>', ',', ';', '"', '\\'])
        && domain.contains('.')
        && !domain.starts_with('.')
        && !domain.ends_with('.')
        && !domain.contains("..")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weeks_start_on_monday_utc() {
        // 2026-11-02 is a Monday.
        let monday = UNIX_EPOCH + Duration::from_secs(20_759 * 86_400);
        let sunday_night = monday - Duration::from_secs(1);
        assert_eq!(utc_week(monday), utc_week(sunday_night) + 1);
        assert_eq!(
            utc_week(monday + Duration::from_secs(6 * 86_400 + 86_399)),
            utc_week(monday)
        );
        assert_eq!(utc_week_start(utc_week(monday)), monday);
    }

    #[test]
    fn only_discord_channel_webhooks_are_accepted() {
        let good = "https://discord.com/api/webhooks/1204567890123456789/AbCdEfGhIjKlMnOpQrStUvWxYz_0123456789-abcdEFGH3qXw";
        assert!(discord_webhook_valid(good));
        assert!(discord_webhook_valid(
            &good.replace("discord.com", "discordapp.com")
        ));
        for bad in [
            "http://discord.com/api/webhooks/1/abcdefghijklmnopqrstuvwxyz",
            "https://discord.com.evil.example/api/webhooks/1/abcdefghijklmnopqrstuvwxyz",
            "https://evil.example/?https://discord.com/api/webhooks/1/abcdefghijklmnopqrstuvwxyz",
            "https://discord.com/api/webhooks/12a/abcdefghijklmnopqrstuvwxyz",
            "https://discord.com/api/webhooks/1/abcdefghijklmnopqrstuvwxyz/extra",
            "https://discord.com/api/webhooks/1/abc?wait=true",
            "https://discord.com/api/webhooks/1/abcdefghijklmnopqrstuvwxyz#x",
        ] {
            assert!(!discord_webhook_valid(bad), "{bad}");
        }
        assert_eq!(
            discord_webhook_display(good),
            "discord.com/api/webhooks/1204…3qXw"
        );
    }

    fn record(owner: &str) -> MinerSettingsRecord {
        MinerSettingsRecord {
            account: "addr".into(),
            password_hash: Some("$argon2id$...".into()),
            owner_ip_group: owner.into(),
            claimed_at: UNIX_EPOCH,
            changed_at: UNIX_EPOCH,
            taken_back_at: None,
            settings: MinerSettings::default(),
        }
    }

    fn work(rows: &[(&str, u64)]) -> HashMap<String, u64> {
        rows.iter()
            .map(|(group, work)| (group.to_string(), *work))
            .collect()
    }

    #[test]
    fn a_squatter_without_work_loses_to_any_rig() {
        let now = UNIX_EPOCH + Duration::from_secs(10 * 86_400);
        let decision = take_back_decision(
            &record("198.51.100.7"),
            "203.0.113.24",
            &work(&[("203.0.113.24", 10)]),
            now,
        );
        assert_eq!(decision, TakeBack::Allowed { share: 1.0 });
    }

    #[test]
    fn the_claimant_needs_more_work_than_the_owner() {
        let now = UNIX_EPOCH + Duration::from_secs(10 * 86_400);
        let rows = work(&[("owner", 50), ("other", 50), ("small", 10)]);
        assert!(matches!(
            take_back_decision(&record("owner"), "other", &rows, now),
            TakeBack::NotEnoughWork { .. }
        ));
        assert!(matches!(
            take_back_decision(&record("owner"), "small", &rows, now),
            TakeBack::NotEnoughWork { .. }
        ));
        assert!(matches!(
            take_back_decision(&record("owner"), "nobody", &rows, now),
            TakeBack::NotEnoughWork { share } if share == 0.0
        ));
        let rows = work(&[("owner", 50), ("other", 51)]);
        assert!(matches!(
            take_back_decision(&record("owner"), "other", &rows, now),
            TakeBack::Allowed { .. }
        ));
    }

    #[test]
    fn the_owner_group_resets_a_forgotten_password_only_while_it_mines() {
        let now = UNIX_EPOCH + Duration::from_secs(10 * 86_400);
        assert!(matches!(
            take_back_decision(
                &record("owner"),
                "owner",
                &work(&[("owner", 50), ("big", 500)]),
                now
            ),
            TakeBack::Allowed { .. }
        ));
        // A reset waits out the cooldown like any take-back.
        let mut rec = record("owner");
        rec.taken_back_at = Some(now - Duration::from_secs(60));
        assert!(matches!(
            take_back_decision(&rec, "owner", &work(&[("owner", 50)]), now),
            TakeBack::CoolingDown(_)
        ));
        // A squatter that claimed from a browser has no work of its own.
        assert!(matches!(
            take_back_decision(&record("squatter"), "squatter", &work(&[("rig", 50)]), now),
            TakeBack::NotEnoughWork { .. }
        ));
    }

    #[test]
    fn take_backs_cool_down() {
        let taken = UNIX_EPOCH + Duration::from_secs(10 * 86_400);
        let mut rec = record("owner");
        rec.taken_back_at = Some(taken);
        let rows = work(&[("rig", 100)]);
        assert!(matches!(
            take_back_decision(&rec, "rig", &rows, taken + Duration::from_secs(3_600)),
            TakeBack::CoolingDown(left) if left == Duration::from_secs(23 * 3_600)
        ));
        assert!(matches!(
            take_back_decision(&rec, "rig", &rows, taken + TAKE_BACK_COOLDOWN),
            TakeBack::Allowed { .. }
        ));
    }

    #[test]
    fn min_payout_is_bounded_by_the_pool_minimum_and_what_one_payout_sends() {
        let pool = ATOMIC_PER_BNT;
        let bnt = ATOMIC_PER_BNT;
        // bntpool: 10,000 BNT per recipient, 25,000 BNT per run.
        let max = max_min_payout(pool, 10_000 * bnt, 25_000 * bnt);
        assert_eq!(max, 10_000 * bnt);
        assert_eq!(max_min_payout(pool, 0, 25_000 * bnt), 25_000 * bnt);
        assert_eq!(max_min_payout(pool, 0, 0), FALLBACK_MAX_MIN_PAYOUT_ATOMIC);
        assert_eq!(max_min_payout(20 * bnt, 10 * bnt, 0), 20 * bnt);

        assert_eq!(validate_min_payout(5_000 * bnt, pool, max), Ok(5_000 * bnt));
        assert_eq!(validate_min_payout(max, pool, max), Ok(max));
        assert!(validate_min_payout(max + 1, pool, max).is_err());
        assert!(validate_min_payout(bnt / 2, pool, max).is_err());
        // A pool minimum above the limit (exchange accounts) still validates.
        let memo = 150 * bnt;
        assert_eq!(validate_min_payout(memo, memo, 100 * bnt), Ok(memo));
        assert_eq!(effective_min_payout(pool, None), pool);
        assert_eq!(effective_min_payout(pool, Some(5 * pool)), 5 * pool);
        assert_eq!(effective_min_payout(pool, Some(pool / 2)), pool);
        // Exchange accounts never go below their own (higher) minimum.
        assert_eq!(effective_min_payout(10 * pool, None), 10 * pool);
        assert_eq!(effective_min_payout(10 * pool, Some(20 * pool)), 20 * pool);
        assert_eq!(effective_min_payout(10 * pool, Some(2 * pool)), 10 * pool);
    }

    #[test]
    fn alert_emails_are_checked_loosely() {
        for good in ["miner@example.com", "a.b+pool@mail.example.co.uk"] {
            assert!(plausible_email(good), "{good}");
        }
        for bad in [
            "",
            "no-at",
            "a@b",
            "a@.com",
            "a@b.",
            "a b@c.com",
            "a@b..com",
            "x@y.com, z@w.com",
            "<a@b.com>",
            "a\r\n@b.com",
        ] {
            assert!(!plausible_email(bad), "{bad}");
        }
        assert_eq!(utc_day(UNIX_EPOCH + Duration::from_secs(86_400 * 3 + 5)), 3);
    }

    #[test]
    fn hours_floor_to_the_bucket() {
        let at = UNIX_EPOCH + Duration::from_secs(7_200 + 1_234);
        assert_eq!(source_work_hour(at), 7_200);
        assert!(validate_min_payout(1, 2, 10)
            .unwrap_err()
            .contains("0.00000002 BNT"));
    }
}
