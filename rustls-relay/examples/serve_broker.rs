//! Starts the test broker in Docker for a client on a different host.
//!
//! Usage: `cargo run -p rustls-relay --example serve_broker -- <host> [dir]`
//!
//! `<host>` is the name or IP address that the client uses for this machine.
//! The program writes `ca.pem` into `dir`, prints the client settings, and
//! stops the broker when you press Enter.

#[path = "../tests/support/broker.rs"]
mod broker;

use std::path::PathBuf;

fn main() {
    let mut args = std::env::args().skip(1);
    let host = args.next().expect("usage: serve_broker <host> [dir]");
    let dir = PathBuf::from(args.next().unwrap_or_else(|| "relay-broker".into()));
    std::fs::create_dir_all(&dir).unwrap();
    let dir = dir.canonicalize().unwrap();

    broker::write_certs(&dir, &host);
    let docker = broker::start(&dir, &host, "0.0.0.0");
    println!(
        "Broker is up. Copy {} to the client host.",
        dir.join("ca.pem").display()
    );
    println!("RELAY_BOOTSTRAP_SSL={host}:{}", docker.ssl_port);
    println!("RELAY_BOOTSTRAP_SASL_SSL={host}:{}", docker.sasl_ssl_port);
    println!("RELAY_CA_PEM=<path to ca.pem on the client host>");
    println!("Press Enter to stop the broker.");
    let _ = std::io::stdin().read_line(&mut String::new());
}
