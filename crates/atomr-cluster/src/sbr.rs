//! Split-brain resolvers.
//!
//! Five strategies are implemented matching :
//! * KeepMajority
//! * StaticQuorum
//! * KeepOldest
//! * KeepReferee
//! * LeaseMajority

use crate::member::{Member, MemberStatus};

/// What the resolver recommends the cluster do with the given side.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum DowningDecision {
    DownUnreachable,
    DownAll,
    DownSelf,
    Stay,
}

pub trait DowningStrategy: Send + Sync {
    fn decide(&self, reachable: &[&Member], unreachable: &[&Member]) -> DowningDecision;
}

/// KeepMajority: the side with strictly more up members survives.
#[derive(Debug, Clone, Copy, Default)]
pub struct KeepMajorityStrategy;

impl DowningStrategy for KeepMajorityStrategy {
    fn decide(&self, r: &[&Member], u: &[&Member]) -> DowningDecision {
        let up = |ms: &[&Member]| ms.iter().filter(|m| m.status == MemberStatus::Up).count();
        let rn = up(r);
        let un = up(u);
        if rn > un {
            DowningDecision::DownUnreachable
        } else if rn < un {
            DowningDecision::DownSelf
        } else {
            DowningDecision::DownAll
        }
    }
}

/// StaticQuorum: requires at least `quorum_size` reachable members to survive.
#[derive(Debug, Clone, Copy)]
pub struct StaticQuorumStrategy {
    pub quorum_size: usize,
}

impl DowningStrategy for StaticQuorumStrategy {
    fn decide(&self, r: &[&Member], _: &[&Member]) -> DowningDecision {
        if r.len() >= self.quorum_size {
            DowningDecision::DownUnreachable
        } else {
            DowningDecision::DownSelf
        }
    }
}

/// KeepOldest: the side containing the oldest (lowest `up_number`) up member survives.
#[derive(Debug, Clone, Copy, Default)]
pub struct KeepOldestStrategy {
    pub down_if_alone: bool,
}

impl DowningStrategy for KeepOldestStrategy {
    fn decide(&self, r: &[&Member], u: &[&Member]) -> DowningDecision {
        fn oldest<'a>(ms: &[&'a Member]) -> Option<&'a Member> {
            ms.iter().min_by_key(|m| m.up_number).copied()
        }
        let rolds = oldest(r);
        let uolds = oldest(u);
        match (rolds, uolds) {
            (Some(ro), Some(uo)) => {
                if ro.up_number <= uo.up_number {
                    if r.len() == 1 && self.down_if_alone {
                        DowningDecision::DownAll
                    } else {
                        DowningDecision::DownUnreachable
                    }
                } else {
                    DowningDecision::DownSelf
                }
            }
            (Some(_), None) => DowningDecision::DownUnreachable,
            (None, Some(_)) => DowningDecision::DownSelf,
            (None, None) => DowningDecision::Stay,
        }
    }
}

/// KeepReferee: the side containing the designated `referee` member survives.
#[derive(Debug, Clone)]
pub struct KeepReferee {
    pub referee: String,
    pub down_all_if_less_than: usize,
}

impl DowningStrategy for KeepReferee {
    fn decide(&self, r: &[&Member], _u: &[&Member]) -> DowningDecision {
        let has_referee = r.iter().any(|m| m.address.to_string() == self.referee);
        if !has_referee {
            return DowningDecision::DownSelf;
        }
        if r.len() < self.down_all_if_less_than {
            DowningDecision::DownAll
        } else {
            DowningDecision::DownUnreachable
        }
    }
}

/// DownAll: unconditionally downs every member on both sides of the
/// partition. Used when the operator prefers cluster-wide restart over
/// any chance of split-brain (matches "down-all-when-unstable" in
/// related industry SBR catalogs).
///
/// Returns [`DowningDecision::DownAll`] whenever there is any
/// unreachable member; [`DowningDecision::Stay`] when the partition is
/// healthy. The reachable/unreachable inputs are inspected only to
/// distinguish those two cases.
#[derive(Debug, Clone, Copy, Default)]
pub struct DownAllStrategy;

impl DowningStrategy for DownAllStrategy {
    fn decide(&self, _r: &[&Member], u: &[&Member]) -> DowningDecision {
        if u.is_empty() {
            DowningDecision::Stay
        } else {
            DowningDecision::DownAll
        }
    }
}

/// LeaseMajority: majority decision gated by an external lease. In-memory
/// simulation of whether a lease was acquired.
#[derive(Debug, Clone, Copy, Default)]
pub struct LeaseMajorityStrategy {
    pub lease_acquired: bool,
}

impl DowningStrategy for LeaseMajorityStrategy {
    fn decide(&self, r: &[&Member], u: &[&Member]) -> DowningDecision {
        let m = KeepMajorityStrategy.decide(r, u);
        match m {
            DowningDecision::DownAll if self.lease_acquired => DowningDecision::DownUnreachable,
            other => other,
        }
    }
}

/// RoleWeightedQuorum: weights survival by *role importance* rather than
/// raw node count.
///
/// Each role carries a weight. A member's weight is the **max** weight
/// among its roles (0 if it holds no weighted role). The reachable side
/// survives (`DownUnreachable`) iff the sum of its Up members' weights is
/// at least `min_quorum_weight`; otherwise this node steps down
/// (`DownSelf`).
///
/// This lets a numerically larger partition that happens to lack the
/// heavyweight (core) roles be the side that is downed — see the
/// `role_weighted_*` tests.
#[derive(Debug, Clone, Default)]
pub struct RoleWeightedQuorum {
    pub weights: std::collections::HashMap<String, u32>,
    pub min_quorum_weight: u32,
}

impl RoleWeightedQuorum {
    pub fn new(weights: std::collections::HashMap<String, u32>, min_quorum_weight: u32) -> Self {
        Self { weights, min_quorum_weight }
    }

    /// Weight of a single member = the max weight across its roles
    /// (0 if none of its roles are weighted).
    fn member_weight(&self, m: &Member) -> u32 {
        m.roles.iter().filter_map(|r| self.weights.get(r).copied()).max().unwrap_or(0)
    }
}

impl DowningStrategy for RoleWeightedQuorum {
    fn decide(&self, r: &[&Member], _u: &[&Member]) -> DowningDecision {
        let reachable_weight: u32 =
            r.iter().filter(|m| m.status == MemberStatus::Up).map(|m| self.member_weight(m)).sum();
        if reachable_weight >= self.min_quorum_weight {
            DowningDecision::DownUnreachable
        } else {
            DowningDecision::DownSelf
        }
    }
}

/// Observer notified when the SBR runtime concludes this node has lost
/// quorum (and later, when it is regained). Implementations must be
/// cheap and non-blocking; the runtime fires these from `tick`.
///
/// `on_quorum_lost` fires exactly once on the loss transition;
/// `on_quorum_regained` fires exactly once when the partition heals
/// after a prior loss. See [`crate::SbrRuntime::with_observer`].
pub trait QuorumObserver: Send + Sync {
    fn on_quorum_lost(&self);
    fn on_quorum_regained(&self);
}

/// Facade that holds any of the strategies behind a trait object.
pub struct SplitBrainResolver {
    pub strategy: Box<dyn DowningStrategy>,
}

impl SplitBrainResolver {
    pub fn new(strategy: Box<dyn DowningStrategy>) -> Self {
        Self { strategy }
    }
    pub fn decide(&self, r: &[&Member], u: &[&Member]) -> DowningDecision {
        self.strategy.decide(r, u)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use atomr_core::actor::Address;

    fn up(n: i32) -> Member {
        let mut m = Member::new(Address::local(format!("N{n}")), vec![]);
        m.status = MemberStatus::Up;
        m.up_number = n;
        m
    }

    fn up_roles(n: i32, roles: &[&str]) -> Member {
        let mut m =
            Member::new(Address::local(format!("N{n}")), roles.iter().map(|s| s.to_string()).collect());
        m.status = MemberStatus::Up;
        m.up_number = n;
        m
    }

    #[test]
    fn keep_majority_prefers_larger_side() {
        let r = [up(1), up(2), up(3)];
        let u = [up(4)];
        let r_ref: Vec<&Member> = r.iter().collect();
        let u_ref: Vec<&Member> = u.iter().collect();
        assert_eq!(KeepMajorityStrategy.decide(&r_ref, &u_ref), DowningDecision::DownUnreachable);
    }

    #[test]
    fn static_quorum_enforces_size() {
        let r = [up(1)];
        let u = [up(2)];
        let r_ref: Vec<&Member> = r.iter().collect();
        let u_ref: Vec<&Member> = u.iter().collect();
        assert_eq!(StaticQuorumStrategy { quorum_size: 2 }.decide(&r_ref, &u_ref), DowningDecision::DownSelf);
    }

    #[test]
    fn keep_oldest_picks_lowest_up_number() {
        let r = [up(1)];
        let u = [up(2), up(3)];
        let r_ref: Vec<&Member> = r.iter().collect();
        let u_ref: Vec<&Member> = u.iter().collect();
        assert_eq!(KeepOldestStrategy::default().decide(&r_ref, &u_ref), DowningDecision::DownUnreachable);
    }

    #[test]
    fn down_all_strategy_downs_every_member_when_partitioned() {
        let r = [up(1), up(2)];
        let u = [up(3)];
        let r_ref: Vec<&Member> = r.iter().collect();
        let u_ref: Vec<&Member> = u.iter().collect();
        assert_eq!(DownAllStrategy.decide(&r_ref, &u_ref), DowningDecision::DownAll);
    }

    #[test]
    fn down_all_strategy_stays_when_no_unreachable() {
        let r = [up(1), up(2), up(3)];
        let u: [Member; 0] = [];
        let r_ref: Vec<&Member> = r.iter().collect();
        let u_ref: Vec<&Member> = u.iter().collect();
        assert_eq!(DownAllStrategy.decide(&r_ref, &u_ref), DowningDecision::Stay);
    }

    #[test]
    fn role_weighted_downs_larger_but_lightweight_side() {
        // Reachable side: 3 plain workers (weight 0 each) → total 0.
        // Unreachable side holds the single "core" node.
        // Even though reachable has MORE nodes, it lacks core weight and
        // must step down.
        let mut weights = std::collections::HashMap::new();
        weights.insert("core".to_string(), 10);
        weights.insert("worker".to_string(), 1);
        let q = RoleWeightedQuorum::new(weights, 10);

        let r = [up_roles(1, &["worker"]), up_roles(2, &["worker"]), up_roles(3, &["worker"])];
        let u = [up_roles(4, &["core"])];
        let r_ref: Vec<&Member> = r.iter().collect();
        let u_ref: Vec<&Member> = u.iter().collect();
        assert_eq!(q.decide(&r_ref, &u_ref), DowningDecision::DownSelf);
    }

    #[test]
    fn role_weighted_keeps_side_with_core_weight() {
        let mut weights = std::collections::HashMap::new();
        weights.insert("core".to_string(), 10);
        weights.insert("worker".to_string(), 1);
        let q = RoleWeightedQuorum::new(weights, 10);

        // Reachable holds the core node → meets quorum weight → survives.
        let r = [up_roles(1, &["core"]), up_roles(2, &["worker"])];
        let u = [up_roles(3, &["worker"]), up_roles(4, &["worker"]), up_roles(5, &["worker"])];
        let r_ref: Vec<&Member> = r.iter().collect();
        let u_ref: Vec<&Member> = u.iter().collect();
        assert_eq!(q.decide(&r_ref, &u_ref), DowningDecision::DownUnreachable);
    }

    #[test]
    fn role_weighted_member_weight_is_max_of_roles() {
        // A member with both roles counts the MAX (10), not the sum.
        let mut weights = std::collections::HashMap::new();
        weights.insert("core".to_string(), 10);
        weights.insert("worker".to_string(), 1);
        let q = RoleWeightedQuorum::new(weights, 10);
        let m = up_roles(1, &["core", "worker"]);
        assert_eq!(q.member_weight(&m), 10);
        // Unmatched role → 0.
        let plain = up_roles(2, &["other"]);
        assert_eq!(q.member_weight(&plain), 0);
    }

    #[test]
    fn down_all_strategy_downs_even_with_majority_reachable() {
        // Unlike KeepMajority, DownAll doesn't care about side sizes —
        // any unreachable member triggers a full down.
        let r = [up(1), up(2), up(3), up(4)];
        let u = [up(5)];
        let r_ref: Vec<&Member> = r.iter().collect();
        let u_ref: Vec<&Member> = u.iter().collect();
        assert_eq!(DownAllStrategy.decide(&r_ref, &u_ref), DowningDecision::DownAll);
    }
}
