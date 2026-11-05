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
    /// Highest payout id and block height already considered.
    pub payout_cursor: i64,
    pub block_cursor: i64,
    /// UTC day (days since the epoch) `sent_today` counts.
    pub day: i64,
    pub sent_today: u32,
}

/// Days since the epoch, in UTC.
pub fn utc_day(at: SystemTime) -> i64 {
    (at.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() / 86_400) as i64
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
