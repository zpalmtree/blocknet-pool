//! Per-source identity helpers.
//!
//! Miner addresses are public and login does not prove ownership, so abuse
//! evidence is tracked against the network source that produced it (the
//! connection/session and its IP group) before it is ever attributed to an
//! address.

use std::net::IpAddr;

/// Groups an IP for per-source accounting: IPv4 by full address, IPv6 by /64
/// (one end-site/host allocation). IPv4-mapped IPv6 addresses are treated as
/// IPv4.
pub(crate) fn ip_group_key(ip: IpAddr) -> String {
    match ip {
        IpAddr::V4(v4) => v4.to_string(),
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return v4.to_string();
            }
            let seg = v6.segments();
            format!("{:x}:{:x}:{:x}:{:x}::/64", seg[0], seg[1], seg[2], seg[3])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ipv6_groups_by_slash_64_and_ipv4_by_address() {
        let a: IpAddr = "2001:db8:1:2:aaaa::1".parse().unwrap();
        let b: IpAddr = "2001:db8:1:2:ffff::9".parse().unwrap();
        let c: IpAddr = "2001:db8:1:3::1".parse().unwrap();
        assert_eq!(ip_group_key(a), ip_group_key(b));
        assert_ne!(ip_group_key(a), ip_group_key(c));
        assert_eq!(ip_group_key("192.0.2.7".parse().unwrap()), "192.0.2.7");
        assert_eq!(
            ip_group_key("::ffff:192.0.2.7".parse().unwrap()),
            "192.0.2.7"
        );
    }
}
