use std::net::IpAddr;

/// Groups an IP for per-source accounting: IPv4 by full address, IPv6 by /64
/// (one end-site/host allocation). IPv4-mapped IPv6 addresses are treated as
/// IPv4.
pub fn ip_group_key(ip: IpAddr) -> String {
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
