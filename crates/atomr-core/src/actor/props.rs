//! `Props` — a recipe for constructing an actor.

use std::fmt;
use std::sync::Arc;

use super::deploy::Deploy;
use super::metadata::MessageInterceptor;
use super::traits::Actor;
use crate::supervision::SupervisorStrategy;

pub type Factory<A> = Arc<dyn Fn() -> A + Send + Sync>;

/// Typed props. The factory produces fresh `A` instances on initial start
/// and on restart.
pub struct Props<A: Actor> {
    factory: Factory<A>,
    pub dispatcher: Option<String>,
    pub mailbox: Option<String>,
    pub deploy: Deploy,
    pub supervisor_strategy: Option<SupervisorStrategy>,
    /// Optional message interceptor (FR-10) invoked around each `handle` and on
    /// outgoing sends — e.g. a tracing-context propagator.
    pub interceptor: Option<Arc<dyn MessageInterceptor>>,
}

impl<A: Actor> Clone for Props<A> {
    fn clone(&self) -> Self {
        Self {
            factory: self.factory.clone(),
            dispatcher: self.dispatcher.clone(),
            mailbox: self.mailbox.clone(),
            deploy: self.deploy.clone(),
            supervisor_strategy: self.supervisor_strategy.clone(),
            interceptor: self.interceptor.clone(),
        }
    }
}

impl<A: Actor> fmt::Debug for Props<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Props")
            .field("dispatcher", &self.dispatcher)
            .field("mailbox", &self.mailbox)
            .field("deploy", &self.deploy)
            .finish_non_exhaustive()
    }
}

impl<A: Actor> Props<A> {
    /// Create props from a zero-argument factory. Mirrors `Props.Create<T>(() => new T())`.
    pub fn create<F>(factory: F) -> Self
    where
        F: Fn() -> A + Send + Sync + 'static,
    {
        Self {
            factory: Arc::new(factory),
            dispatcher: None,
            mailbox: None,
            deploy: Deploy::local(),
            supervisor_strategy: None,
            interceptor: None,
        }
    }

    /// Install a [`MessageInterceptor`] (FR-10). It fires before/after each
    /// `handle` and can inject child trace context on outgoing sends.
    pub fn with_interceptor(mut self, interceptor: Arc<dyn MessageInterceptor>) -> Self {
        self.interceptor = Some(interceptor);
        self
    }

    pub fn with_dispatcher(mut self, d: impl Into<String>) -> Self {
        self.dispatcher = Some(d.into());
        self
    }

    pub fn with_mailbox(mut self, m: impl Into<String>) -> Self {
        self.mailbox = Some(m.into());
        self
    }

    pub fn with_supervisor_strategy(mut self, s: SupervisorStrategy) -> Self {
        self.supervisor_strategy = Some(s);
        self
    }

    pub fn with_deploy(mut self, d: Deploy) -> Self {
        self.deploy = d;
        self
    }

    pub fn new_actor(&self) -> A {
        (self.factory)()
    }
}

/// Type-erased props — used when an actor needs to hold props of an
/// unknown `A` (e.g. remote deployment, routers).
#[derive(Clone)]
pub struct BoxedProps {
    pub spawn: Arc<dyn Fn() -> Arc<dyn std::any::Any + Send + Sync> + Send + Sync>,
    pub dispatcher: Option<String>,
    pub mailbox: Option<String>,
    pub deploy: Deploy,
}

impl fmt::Debug for BoxedProps {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoxedProps").finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actor::context::Context;

    #[derive(Default)]
    struct A(u32);

    #[async_trait::async_trait]
    impl Actor for A {
        type Msg = ();
        async fn handle(&mut self, _: &mut Context<Self>, _: ()) {}
    }

    #[test]
    fn create_and_instantiate() {
        let p = Props::create(|| A(5));
        let a = p.new_actor();
        assert_eq!(a.0, 5);
    }
}
