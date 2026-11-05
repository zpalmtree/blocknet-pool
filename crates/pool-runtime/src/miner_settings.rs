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

/// Highest minimum payout a miner may choose.
pub const MAX_MIN_PAYOUT_ATOMIC: u64 = 100 * ATOMIC_PER_BNT;

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

/// Checks a requested minimum payout against the account's pool minimum.
pub fn validate_min_payout(requested: u64, pool_minimum: u64) -> Result<u64, String> {
    if requested < pool_minimum {
        return Err(format!(
            "minimum payout must be at least {}",
            format_bnt(pool_minimum)
        ));
    }
    if requested > MAX_MIN_PAYOUT_ATOMIC.max(pool_minimum) {
        return Err(format!(
            "minimum payout must be at most {}",
            format_bnt(MAX_MIN_PAYOUT_ATOMIC.max(pool_minimum))
        ));
    }
    Ok(requested)
}

/// The minimum payout that applies: the miner's choice, never below the pool's.
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
    fn min_payout_is_bounded_by_the_pool_minimum_and_the_cap() {
        let pool = ATOMIC_PER_BNT;
        assert_eq!(
            validate_min_payout(5 * ATOMIC_PER_BNT, pool),
            Ok(5 * ATOMIC_PER_BNT)
        );
        assert!(validate_min_payout(ATOMIC_PER_BNT / 2, pool).is_err());
        assert!(validate_min_payout(MAX_MIN_PAYOUT_ATOMIC + 1, pool).is_err());
        assert_eq!(
            validate_min_payout(MAX_MIN_PAYOUT_ATOMIC, pool),
            Ok(MAX_MIN_PAYOUT_ATOMIC)
        );
        // A pool minimum above the cap (exchange accounts) still validates.
        let memo = 150 * ATOMIC_PER_BNT;
        assert_eq!(validate_min_payout(memo, memo), Ok(memo));
        assert_eq!(effective_min_payout(pool, None), pool);
        assert_eq!(effective_min_payout(pool, Some(5 * pool)), 5 * pool);
        assert_eq!(effective_min_payout(10 * pool, Some(5 * pool)), 10 * pool);
    }

    #[test]
    fn hours_floor_to_the_bucket() {
        let at = UNIX_EPOCH + Duration::from_secs(7_200 + 1_234);
        assert_eq!(source_work_hour(at), 7_200);
        assert!(validate_min_payout(1, 2)
            .unwrap_err()
            .contains("0.00000002 BNT"));
    }
}
