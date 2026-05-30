//! USB link probe — diagnostic + interactive demo for
//! `atomr-remote-serial` over a USB cable.
//!
//! Three subcommands:
//!   * `list-devices` — enumerate serial ports on this OS.
//!   * `listen` — open a device, host a Peer actor, print the
//!     address to give to the connect side.
//!   * `connect` — open a device, associate to the printed
//!     address, exchange chat lines + ping/pong stats.
//!
//! See `examples/usb-link-probe/README.md` for the worked
//! Linux ↔ Windows flow.

mod io_loop;
mod peer;
mod stats;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context as _, Result};
use clap::{Parser, Subcommand};
use tokio::sync::mpsc::unbounded_channel;

use atomr_core::actor::{ActorSystem, Props};
use atomr_remote::transport::Transport;
use atomr_remote::{RemoteSettings, RemoteSystem};
use atomr_remote_serial::SerialTransport;

use crate::io_loop::LoopArgs;
use crate::peer::{LinkMsg, Peer};
use crate::stats::Stats;

#[derive(Debug, Parser)]
#[command(
    name = "usb-link-probe",
    about = "Cross-OS diagnostic for atomr-remote-serial over a USB cable",
    version
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// List serial ports visible to the OS.
    ListDevices,
    /// Open a device and wait to be associated with by a `connect` peer.
    Listen {
        #[arg(long)]
        device: String,
        #[arg(long, default_value_t = 115_200)]
        baud: u32,
        #[arg(long, default_value = "A")]
        system: String,
        #[arg(long, default_value = "1s", value_parser = parse_duration)]
        ping_interval: Duration,
        #[arg(long, default_value = "5s", value_parser = parse_duration)]
        stats_interval: Duration,
    },
    /// Open a device and associate to the address printed by a `listen` peer.
    Connect {
        #[arg(long)]
        device: String,
        #[arg(long)]
        peer: String,
        #[arg(long, default_value_t = 115_200)]
        baud: u32,
        #[arg(long, default_value = "B")]
        system: String,
        #[arg(long, default_value = "1s", value_parser = parse_duration)]
        ping_interval: Duration,
        #[arg(long, default_value = "5s", value_parser = parse_duration)]
        stats_interval: Duration,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::ListDevices => list_devices(),
        Cmd::Listen { device, baud, system, ping_interval, stats_interval } => {
            run_endpoint(EndpointArgs {
                device,
                baud,
                system_name: system,
                peer_addr_initial: None,
                ping_interval,
                stats_interval,
            })
            .await
        }
        Cmd::Connect { device, peer, baud, system, ping_interval, stats_interval } => {
            run_endpoint(EndpointArgs {
                device,
                baud,
                system_name: system,
                peer_addr_initial: Some(peer),
                ping_interval,
                stats_interval,
            })
            .await
        }
    }
}

fn list_devices() -> Result<()> {
    match tokio_serial::available_ports() {
        Ok(ports) if ports.is_empty() => {
            println!("(no serial ports found)");
        }
        Ok(ports) => {
            for p in ports {
                println!("{}  {:?}", p.port_name, p.port_type);
            }
        }
        Err(e) => return Err(anyhow!("enumerate failed: {e}")),
    }
    Ok(())
}

struct EndpointArgs {
    device: String,
    baud: u32,
    system_name: String,
    peer_addr_initial: Option<String>,
    ping_interval: Duration,
    stats_interval: Duration,
}

async fn run_endpoint(args: EndpointArgs) -> Result<()> {
    let transport: Arc<dyn Transport> = Arc::new(SerialTransport::with_options(
        args.system_name.clone(),
        args.device.clone(),
        args.baud,
        4 * 1024 * 1024,
        atomr_remote_serial::ReconnectPolicy::default(),
    ));

    let sys = ActorSystem::create(args.system_name.clone(), atomr_config::Config::reference())
        .await
        .with_context(|| "ActorSystem::create failed")?;
    let remote = RemoteSystem::start_with_transport(sys, transport, RemoteSettings::default())
        .await
        .with_context(|| format!("opening serial device {}", args.device))?;
    remote.register_bincode::<LinkMsg>();

    let (inbound_tx, inbound_rx) = unbounded_channel::<LinkMsg>();
    let peer_ref = remote
        .system
        .actor_of(Props::create(move || Peer::new(inbound_tx.clone())), "peer")
        .map_err(|e| anyhow!("spawn peer actor: {e:?}"))?;
    remote.expose_actor(peer_ref);

    let my_addr = remote.local_address.to_string();
    println!("local address: {my_addr}");
    if let Some(p) = args.peer_addr_initial.as_deref() {
        println!("peer address:  {p}");
    } else {
        println!("peer address:  (waiting for incoming Ping to learn it)");
    }
    println!("(type lines, Ctrl-C to exit)");

    let remote = Arc::new(remote);
    let stats = Arc::new(Stats::new());
    let result = io_loop::run(
        inbound_rx,
        LoopArgs {
            remote: remote.clone(),
            stats,
            my_addr,
            peer_addr_initial: args.peer_addr_initial,
            ping_interval: args.ping_interval,
            stats_interval: args.stats_interval,
        },
    )
    .await;

    remote.shutdown().await;
    result
}

/// `1s`, `500ms`, `2m` — small subset of the humantime parsing the
/// rest of the workspace uses.
fn parse_duration(s: &str) -> Result<Duration, String> {
    let s = s.trim();
    let (num, unit) = s
        .find(|c: char| c.is_alphabetic())
        .map(|i| (&s[..i], &s[i..]))
        .ok_or_else(|| format!("missing unit in `{s}` (try `1s`, `500ms`, `2m`)"))?;
    let n: u64 = num.parse().map_err(|e| format!("invalid number `{num}`: {e}"))?;
    Ok(match unit {
        "ms" => Duration::from_millis(n),
        "s" => Duration::from_secs(n),
        "m" => Duration::from_secs(n * 60),
        other => return Err(format!("unknown unit `{other}` (use ms / s / m)")),
    })
}
