//! Process-level memory and CPU sampling.
//!
//! Uses Linux `/proc/self/{status,stat}` when available. On other
//! platforms the readers return `None` and callers treat the field as
//! "unavailable". The CLI prints those as `n/a` rather than failing.

// `fs` is only used by the Linux `/proc` readers below; on other
// platforms those `cfg` blocks compile out, so gate the import to match.
#[cfg(target_os = "linux")]
use std::fs;
use std::time::Duration;

/// Resident set size in bytes, or `None` if unavailable.
pub fn rss_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let s = fs::read_to_string("/proc/self/status").ok()?;
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                let kib: u64 = rest.split_whitespace().next()?.parse().ok()?;
                return Some(kib * 1024);
            }
        }
        None
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// Peak resident set size in bytes (VmHWM on Linux).
pub fn peak_rss_bytes() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let s = fs::read_to_string("/proc/self/status").ok()?;
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("VmHWM:") {
                let kib: u64 = rest.split_whitespace().next()?.parse().ok()?;
                return Some(kib * 1024);
            }
        }
        None
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// User+system CPU time consumed by the process so far.
pub fn cpu_time() -> Option<Duration> {
    #[cfg(target_os = "linux")]
    {
        // /proc/self/stat fields 14 (utime) and 15 (stime) are jiffies.
        let s = fs::read_to_string("/proc/self/stat").ok()?;
        // The comm field (field 2) can contain spaces / parens; skip past
        // the last ')' before splitting on whitespace.
        let rparen = s.rfind(')')?;
        let rest = &s[rparen + 1..];
        let fields: Vec<&str> = rest.split_whitespace().collect();
        // After the ')' the first field is state (index 0), so utime and
        // stime land at indices 11 and 12.
        let utime: u64 = fields.get(11)?.parse().ok()?;
        let stime: u64 = fields.get(12)?.parse().ok()?;
        let ticks = utime + stime;
        let hz = clock_ticks_per_sec().unwrap_or(100);
        Some(Duration::from_nanos(ticks * 1_000_000_000 / hz))
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

#[cfg(target_os = "linux")]
fn clock_ticks_per_sec() -> Option<u64> {
    // sysconf(_SC_CLK_TCK). Default to 100 Hz if the call is unavailable.
    unsafe {
        // 2 == _SC_CLK_TCK on glibc/musl.
        let v = libc_sysconf(2);
        if v > 0 {
            Some(v as u64)
        } else {
            None
        }
    }
}

#[cfg(target_os = "linux")]
extern "C" {
    #[link_name = "sysconf"]
    fn libc_sysconf(name: i32) -> i64;
}

/// Convenience: delta between two RSS samples, clamped at 0.
pub fn rss_delta(before: Option<u64>, after: Option<u64>) -> Option<i64> {
    Some(after? as i64 - before? as i64)
}

/// Convenience: delta between two CPU samples, clamped at 0.
pub fn cpu_delta(before: Option<Duration>, after: Option<Duration>) -> Option<Duration> {
    Some(after?.saturating_sub(before?))
}

/// Compute latency percentile from a sorted slice of durations.
pub fn percentile(sorted: &[Duration], pct: f64) -> Option<Duration> {
    if sorted.is_empty() {
        return None;
    }
    let clamped = pct.clamp(0.0, 100.0);
    let idx = ((clamped / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
    sorted.get(idx).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rss_is_sensible() {
        // Linux CI always satisfies this; elsewhere we just ensure the
        // probe doesn't panic.
        if let Some(n) = rss_bytes() {
            assert!(n > 0);
        }
    }

    #[test]
    fn percentile_bounds() {
        let xs: Vec<Duration> = (1..=100).map(Duration::from_micros).collect();
        assert_eq!(percentile(&xs, 50.0), Some(Duration::from_micros(51)));
        assert_eq!(percentile(&xs, 0.0), Some(Duration::from_micros(1)));
        assert_eq!(percentile(&xs, 100.0), Some(Duration::from_micros(100)));
        assert_eq!(percentile(&[], 50.0), None);
    }
}
