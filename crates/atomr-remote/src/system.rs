//! `RemoteSystem` — convenience wrapper that builds and wires up the
//! whole remoting stack on top of a `atomr_core::ActorSystem`.
//!
//! Most users hold one `RemoteSystem` per process. It owns:
//!
//! * the underlying `Transport` (default: `TcpTransport`),
//! * the `AkkaProtocolTransport` handshake/heartbeat layer,
//! * the `EndpointManager` association state machine,
//! * the `RemoteSystemDaemon` for inbound dispatch,
//! * the `RemoteWatcher` for cross-system death watch,
//! * a `SerializerRegistry` and `AddressUid`.
//!
//! Spawn it with [`RemoteSystem::start`], register your message types with
//! [`RemoteSystem::register_bincode::<MyMsg>()`], and then deliver a remote
//! actor handle to local code with [`RemoteSystem::actor_selection`].

use std::net::SocketAddr;
use std::sync::Arc;

use atomr_core::actor::{ActorPath, ActorRef, ActorSystem, Address, SerializedMessage, UntypedActorRef};

use crate::address_uid::AddressUid;
use crate::endpoint::InboundEnvelope;
use crate::endpoint_manager::EndpointManager;
use crate::pdu::DisassociateReason;
use crate::provider::RemoteActorRefProvider;
use crate::remote_watcher::RemoteWatcher;
use crate::serialization::SerializerRegistry;
use crate::settings::RemoteSettings;
use crate::system_daemon::{LocalDispatch, RemoteSystemDaemon};
use crate::transport::{AkkaProtocolTransport, TcpTransport, Transport};

/// Returned by [`RemoteSystem::start`].
pub struct RemoteSystem {
    pub system: ActorSystem,
    pub provider: Arc<RemoteActorRefProvider>,
    pub daemon: Arc<RemoteSystemDaemon>,
    pub watcher: Arc<RemoteWatcher>,
    pub address_uid: AddressUid,
    pub local_address: Address,
}

impl RemoteSystem {
    /// Convenience: build a `TcpTransport` bound to `bind`, install it on
    /// `system`, and return the wired [`RemoteSystem`].
    pub async fn start(
        system: ActorSystem,
        bind: SocketAddr,
        settings: RemoteSettings,
    ) -> Result<Self, crate::transport::TransportError> {
        let transport: Arc<dyn Transport> = Arc::new(TcpTransport::with_advertised(
            system.name().to_string(),
            bind,
            settings.hostname.clone(),
            settings.max_frame_size,
        ));
        Self::start_with_transport(system, transport, settings).await
    }

    pub async fn start_with_transport(
        system: ActorSystem,
        transport: Arc<dyn Transport>,
        settings: RemoteSettings,
    ) -> Result<Self, crate::transport::TransportError> {
        let address_uid = AddressUid::new();
        let protocol = AkkaProtocolTransport::new(transport, settings.clone(), address_uid.clone());
        let endpoint_manager = EndpointManager::new(protocol.clone(), settings.clone());
        let local_address = endpoint_manager.start().await?;

        let registry = SerializerRegistry::standard();
        let local_uid = address_uid.get();
        let daemon =
            RemoteSystemDaemon::new(system.clone(), registry.clone(), endpoint_manager.clone(), local_uid);
        let watcher = RemoteWatcher::new(endpoint_manager.clone(), registry.clone(), local_uid);

        // Drain the manager's inbound stream into the daemon dispatcher.
        let mut inbound = endpoint_manager.take_inbound();
        let daemon_for_pump = daemon.clone();
        tokio::spawn(async move {
            while let Some(env) = inbound.recv().await {
                handle_inbound(&daemon_for_pump, env);
            }
        });

        let provider = RemoteActorRefProvider::new(
            local_address.clone(),
            local_uid,
            endpoint_manager.clone(),
            registry,
            daemon.clone(),
        );
        provider.install(&system);

        Ok(Self { system, provider, daemon, watcher, address_uid, local_address })
    }

    pub fn endpoint_manager(&self) -> &EndpointManager {
        self.provider.endpoint_manager()
    }

    pub fn registry(&self) -> &SerializerRegistry {
        self.provider.registry()
    }

    /// Register the bincode codec for `T`. Required for any user message
    /// type that crosses the wire.
    pub fn register_bincode<T>(&self)
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + 'static,
    {
        self.registry().register_bincode::<T>();
    }

    /// Register the JSON codec for `T`.
    pub fn register_json<T>(&self)
    where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + 'static,
    {
        self.registry().register_json::<T>();
    }

    /// Register a local actor as the destination for inbound remote
    /// messages addressed to its path. Caller must already have the
    /// codec for `M` registered.
    pub fn expose_actor<M>(&self, target: ActorRef<M>)
    where
        M: Send + 'static,
    {
        let target = target.clone();
        let path = target.path().clone();
        let dispatch: LocalDispatch = Arc::new(move |_p, _manifest, value| {
            // Downcast to M and forward.
            match value.downcast::<M>() {
                Ok(m) => target.tell(*m),
                Err(_) => {
                    tracing::warn!(target = %target.path(), "remote msg type mismatch");
                }
            }
        });
        self.daemon.register(path, dispatch);
    }

    /// Look up a remote actor by full path string, returning a typed
    /// `ActorRef<M>` with the bincode codec for `M`. Caller is responsible
    /// for matching `M` to whatever the receiving side declares.
    pub fn actor_selection<M>(&self, path: &str) -> Option<ActorRef<M>>
    where
        M: serde::Serialize + Send + 'static,
    {
        let endpoint_manager = self.endpoint_manager().clone();
        let registry = self.registry().clone();
        let local_uid = self.address_uid.get();
        // Reject malformed paths up front so callers don't get a
        // dangling `ActorRef<M>` whose underlying handle never resolves.
        let _parsed = parse_actor_path(path)?;
        let serialize: Arc<dyn Fn(M, Option<ActorPath>) -> SerializedMessage + Send + Sync> =
            Arc::new(move |msg: M, sender: Option<ActorPath>| {
                let manifest = std::any::type_name::<M>().to_string();
                let payload =
                    bincode::serde::encode_to_vec(&msg, bincode::config::standard()).unwrap_or_default();
                SerializedMessage {
                    serializer_id: crate::serialization::BINCODE_SERIALIZER_ID,
                    manifest,
                    payload,
                    sender,
                }
            });
        let _ = (registry, local_uid, endpoint_manager);
        self.system.actor_selection_with(path, serialize)
    }

    /// Untyped variant — useful for system-message-only refs (e.g.
    /// remote watchers).
    pub fn actor_selection_untyped(&self, path: &str) -> Option<UntypedActorRef> {
        self.system.actor_selection(path)
    }

    pub async fn shutdown(&self) {
        let _ = self.endpoint_manager().shutdown().await;
        self.daemon.clear();
        let _ = DisassociateReason::Normal; // referenced for clarity
    }
}

fn handle_inbound(daemon: &Arc<RemoteSystemDaemon>, inbound: InboundEnvelope) {
    let env = inbound.envelope;
    // Parse recipient_path → ActorPath. The dispatcher only needs the
    // path-without-address segment to look up the local route.
    let Some(path) = parse_actor_path(&env.recipient_path) else {
        tracing::warn!(rec = %env.recipient_path, "could not parse recipient");
        return;
    };
    if env.system {
        // System-control payload — decode RemoteSystemMsg and dispatch.
        match daemon.registry().decode_dyn(&env.manifest, env.serializer_id, &env.payload) {
            Ok((value, _)) => {
                if let Ok(msg) = value.downcast::<atomr_core::actor::RemoteSystemMsg>() {
                    daemon.dispatch_system(&path, *msg);
                }
            }
            Err(e) => {
                tracing::warn!("system payload decode failed: {e}");
            }
        }
    } else if let Err(e) = daemon.dispatch_user(&path, &env.manifest, env.serializer_id, &env.payload) {
        tracing::warn!(rec = %env.recipient_path, "user payload dispatch failed: {e}");
    }
}

fn parse_actor_path(s: &str) -> Option<ActorPath> {
    let scheme_end = s.find("://")?;
    let after = &s[scheme_end + 3..];
    let split = after.find('/').unwrap_or(after.len());
    let (addr_str, path_str) = (&s[..scheme_end + 3 + split], &after[split..]);
    let address = Address::parse(addr_str)?;
    let mut path = ActorPath::root(address);
    for seg in path_str.split('/').filter(|x| !x.is_empty()) {
        if let Some((name, uid)) = seg.split_once('#') {
            path = path.child(name).with_uid(uid.parse().ok()?);
        } else {
            path = path.child(seg);
        }
    }
    Some(path)
}
