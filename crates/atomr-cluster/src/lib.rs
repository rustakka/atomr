//! atomr-cluster.
//!
//! Contains membership, gossip, reachability, heartbeat, and the
//! split-brain resolver strategies.

mod cluster_daemon;
mod events;
mod gossip;
mod gossip_pdu;
mod heartbeat;
mod heartbeat_sender;
mod leader;
mod member;
mod membership;
mod multi_dc;
mod reachability;
mod remote_adapter;
mod sbr;
mod sbr_runtime;
mod transport;
mod vector_clock;

pub use cluster_daemon::{
    spawn_daemon, spawn_daemon_with_sbr, ClusterDaemonHandle, DaemonCmd, DaemonConfig, DaemonSnapshot,
    GossipTransport, NoSbr,
};
pub use events::{ClusterEvent, ClusterEventBus, SubscriptionHandle};
pub use gossip::{Gossip, GossipOverview};
pub use gossip_pdu::{decide as gossip_decide, pick_gossip_target, GossipDecision, GossipPdu};
pub use heartbeat::HeartbeatState;
pub use heartbeat_sender::{HeartbeatSender, PeerHeartbeat};
pub use leader::{elect_leader, is_converged, next_status, LeaderHandover, LeaderHandoverEvent};
pub use member::{Member, MemberStatus};
pub use membership::MembershipState;
pub use multi_dc::{
    heartbeat_interval_for, member_dc, partition_by_dc, same_dc, CrossDcSettings, DC_ROLE_PREFIX, DEFAULT_DC,
};
pub use reachability::{Reachability, ReachabilityStatus};
pub use remote_adapter::ClusterRemoteAdapter;
pub use sbr::{
    DownAllStrategy, DowningDecision, DowningStrategy, KeepMajorityStrategy, KeepOldestStrategy, KeepReferee,
    LeaseMajorityStrategy, QuorumObserver, RoleWeightedQuorum, SplitBrainResolver, StaticQuorumStrategy,
};
pub use sbr_runtime::{SbrAction, SbrRuntime};
pub use transport::{
    ClusterFrame, InProcessClusterTransport, InProcessRegistry, RecordingSink, RemoteMessageSink,
    RemoteTellRecord, TcpClusterTransport,
};
pub use vector_clock::{VectorClock, VectorRelation};
