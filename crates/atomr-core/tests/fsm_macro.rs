//! Phase 3.7 — `fsm!` declarative macro smoke test.

use atomr_core::actor::FiniteStateMachine;
use atomr_core::fsm;

#[derive(Clone, Eq, PartialEq, Debug)]
enum Light {
    Idle,
    Running,
}

enum Cmd {
    Go,
    Stop,
}

struct Traffic;

fsm! {
    Traffic,
    state = Light,
    data = u32,
    msg = Cmd;
    initial state = Light::Idle, data = 0;
    (Light::Idle, Cmd::Go) => |_s, d| (Light::Running, *d + 1, None);
    (Light::Running, Cmd::Stop) => |_s, d| (Light::Idle, *d, None);
}

#[test]
fn fsm_macro_drives_transitions() {
    let mut t = Traffic;
    assert_eq!(t.initial_state(), Light::Idle);
    let s1 = t.transition(&Light::Idle, &0, Cmd::Go).unwrap();
    assert_eq!(s1.next, Light::Running);
    assert_eq!(s1.data, 1);
    let s2 = t.transition(&Light::Running, &1, Cmd::Stop).unwrap();
    assert_eq!(s2.next, Light::Idle);
}

#[test]
fn fsm_macro_returns_none_on_unhandled() {
    let mut t = Traffic;
    assert!(t.transition(&Light::Running, &0, Cmd::Go).is_none());
}
