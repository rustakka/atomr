//! atomr-cluster-tools.
//! `src/contrib/cluster/`.

mod cluster_client;
mod cluster_singleton;
mod kill_switch;
mod pub_sub;

pub use cluster_client::{ClusterClient, ClusterClientError, ClusterClientSettings, ClusterReceptionist};
pub use cluster_singleton::{
    ClusterSingletonManager, ClusterSingletonProxy, FenceToken, HandoffState, SingletonHandoff,
    SingletonState,
};
pub use kill_switch::{
    AckHandle, ClusterKillSwitch, HaltGuarded, HaltReason, HaltToken, KillSwitchQuorumObserver,
    QuiescenceReport, ResetAuthorization, ResetError,
};
pub use pub_sub::{ClusterPubSub, DistributedPubSub, MediatorPdu, MediatorTransport};
