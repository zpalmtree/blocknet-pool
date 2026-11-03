//! Solo mining: a miner opts in by logging in as `solo:<address>` (or
//! `solo:@name`). Solo work is kept out of PPLNS, and a block found by a solo
//! connection is credited to its finder alone, less the pool fee.

/// Login prefix that selects solo mining.
pub const SOLO_LOGIN_PREFIX: &str = "solo:";

/// Splits an optional `solo:` prefix (any letter case) off a login address,
/// returning whether it was present and the trimmed remainder.
pub fn split_solo_login(input: &str) -> (bool, &str) {
    let trimmed = input.trim();
    match trimmed.get(..SOLO_LOGIN_PREFIX.len()) {
        Some(prefix) if prefix.eq_ignore_ascii_case(SOLO_LOGIN_PREFIX) => {
            (true, trimmed[SOLO_LOGIN_PREFIX.len()..].trim())
        }
        _ => (false, trimmed),
    }
}

/// The owner a job assignment is issued to. Solo connections get a distinct
/// owner, so work assigned to one mode cannot be submitted through the other
/// even when both use the same address.
pub fn assignment_owner(address: &str, solo: bool) -> String {
    if solo {
        format!("{SOLO_LOGIN_PREFIX}{address}")
    } else {
        address.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_the_solo_prefix_in_any_case() {
        assert_eq!(split_solo_login("solo:ADDR"), (true, "ADDR"));
        assert_eq!(split_solo_login("  SOLO: @alice "), (true, "@alice"));
        assert_eq!(split_solo_login("Solo:"), (true, ""));
        assert_eq!(split_solo_login(" ADDR "), (false, "ADDR"));
        assert_eq!(split_solo_login("sol"), (false, "sol"));
        assert_eq!(split_solo_login("soloADDR"), (false, "soloADDR"));
    }

    #[test]
    fn solo_assignments_have_a_distinct_owner() {
        assert_eq!(assignment_owner("ADDR", false), "ADDR");
        assert_eq!(assignment_owner("ADDR", true), "solo:ADDR");
    }
}
