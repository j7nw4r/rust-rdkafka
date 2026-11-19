# rustls-relay

This crate is a prototype. It gives rust-rdkafka TLS outside librdkafka, and librdkafka is built without OpenSSL.

librdkafka uses `security.protocol=plaintext` or `sasl_plaintext`. The `ClientContext::connect` hook sends each broker socket to a loopback listener in the process. The relay opens TCP to the broker, does the TLS handshake, and copies the bytes.

The crate name is from the first version. The relay now has two TLS backends:

| Feature | TLS backend | Crypto | Targets |
|---|---|---|---|
| `ring` (default) | Rustls | ring | all |
| `symcrypt` | Rustls | SymCrypt (`symcrypt.dll` or `libsymcrypt.so`) | Windows, Linux |
| `schannel` | SChannel through native-tls | CNG (BCrypt and NCrypt) | Windows only |

The `schannel` feature causes a compile error on other targets, because native-tls uses OpenSSL there. You can enable `symcrypt` and `schannel` in one build, and select the backend at run time.

The hook, the loopback hand-off, the broker backoff, the error classes, the error callback, and the pending TTL are the same for all backends.

## Test settings

The round-trip test is `tests/tls_round_trip.rs`. It is ignored by default. Run it with `-- --ignored --nocapture`.

| Variable | Use |
|---|---|
| `RELAY_TLS_BACKEND` | `rustls` or `schannel`. The default is `rustls` if the build has it, and `schannel` if not. |
| `RELAY_BOOTSTRAP_SSL` | The `host:port` of an SSL listener on an external broker. If you set it, the test does not start Docker. |
| `RELAY_BOOTSTRAP_SASL_SSL` | The `host:port` of a SASL_SSL listener with SASL PLAIN. If you do not set it, the test skips the SASL part. |
| `RELAY_CA_PEM` | The absolute path of the CA certificate (PEM) for the external broker. |
| `RELAY_SASL_USERNAME`, `RELAY_SASL_PASSWORD` | The SASL PLAIN user. The default is `alice` and `alice-secret`. |
| `RELAY_KAFKA_IMAGE` | The broker image when the test starts Docker. The default is `apache/kafka:4.1.1`. |
| `RELAY_DOCKER_NETWORK` | A Docker network for the broker, for example `container:<id>`. |

The client uses IPv4 only (`broker.address.family=v4`). The first output line tells which backend ran, for example `tls backend: rustls (crypto provider: symcrypt)` or `tls backend: schannel`.

## Run on macOS or Linux with Docker

```sh
cargo test -p rustls-relay --test tls_round_trip -- --ignored --nocapture
```

To use SymCrypt on Linux, run `scripts/test-symcrypt-linux.sh` with a SymCrypt Linux release tarball.

## Run on Windows

A Windows host often cannot run Linux containers. Thus, run the broker on a different machine that has Docker, and point the test at it.

### 1. Start the broker on a Docker host

On a macOS or Linux machine with Docker, in a checkout of this branch:

```sh
cargo run -p rustls-relay --example serve_broker -- <broker-host> relay-broker
```

`<broker-host>` is the DNS name or the IPv4 address that the Windows host uses for this machine. The broker certificate is valid for this name. The program prints the two listener addresses. Keep it open. Press Enter to stop the broker.

Make sure that the Windows host can reach the two ports. Copy `relay-broker/ca.pem` to the Windows host.

### 2. Prepare the Windows host

1. Install the Rust MSVC toolchain, the Visual Studio C++ build tools, CMake, and Git.
2. Clone the repository and check out this branch.
3. Run `git submodule update --init` to get librdkafka.
4. For the SymCrypt run, download `symcrypt-windows-amd64-release-<version>.zip` from https://github.com/microsoft/SymCrypt/releases. Use version 103.4.2 or later. Extract it, for example to `C:\symcrypt`. The `dll` folder has `symcrypt.lib` and `symcrypt.dll`.

Do not import the test CA into a Windows certificate store. The test gives the CA to the relay directly, and the bad-certificate control needs a relay that does not trust it.

### 3. Run the test with SymCrypt and SChannel from one build

In PowerShell, in the repository root:

```powershell
$env:SYMCRYPT_LIB_PATH = "C:\symcrypt\dll"
$env:PATH = "C:\symcrypt\dll;$env:PATH"
$env:RELAY_BOOTSTRAP_SSL = "<broker-host>:<ssl-port>"
$env:RELAY_BOOTSTRAP_SASL_SSL = "<broker-host>:<sasl-ssl-port>"
$env:RELAY_CA_PEM = "C:\path\to\ca.pem"
$env:RUST_LOG = "rustls_relay=info"

$env:RELAY_TLS_BACKEND = "rustls"
cargo test -p rustls-relay --no-default-features --features symcrypt,schannel,rdkafka/cmake-build --test tls_round_trip -- --ignored --nocapture

$env:RELAY_TLS_BACKEND = "schannel"
cargo test -p rustls-relay --no-default-features --features symcrypt,schannel,rdkafka/cmake-build --test tls_round_trip -- --ignored --nocapture
```

`SYMCRYPT_LIB_PATH` is the folder with `symcrypt.lib`, for the link step. At run time, Windows finds `symcrypt.dll` through `PATH`. You can also copy `symcrypt.dll` to `target\debug\deps`. The `cmake-build` feature is necessary because the default librdkafka build does not work with MSVC.

### SChannel only

The SChannel backend needs no DLL and no `SYMCRYPT_LIB_PATH`:

```powershell
cargo test -p rustls-relay --no-default-features --features schannel,rdkafka/cmake-build --test tls_round_trip -- --ignored --nocapture
```

### How SChannel trusts the test CA

`TlsConfig::schannel` gives each extra CA to native-tls with `add_root_certificate`. On Windows, native-tls puts these certificates in an in-memory certificate store. SChannel uses that store to build the chain. If a certificate of the chain is in the store, the chain policy check allows the unknown CA. The policy check also makes sure that the certificate is valid for the broker host. The Windows root store stays trusted too. native-tls also has `disable_built_in_roots`, which accepts only chains through the extra CAs. The relay does not use it.

### TLS versions with SChannel

The relay asks for TLS 1.2 or later. SChannel supports TLS 1.3 on Windows 11 and Windows Server 2022 or later. Older Windows hosts use TLS 1.2.

### Expected output

A good run prints the backend on the first line, and ends with these lines:

```
[control] plaintext without the relay: metadata request failed, as expected
[control] relay without the test CA: metadata request failed, as expected; N TLS failures in 8s
[plaintext] consumed 100 of 100 messages from relay-ssl-<id>
[sasl_plaintext] consumed 100 of 100 messages from relay-sasl-ssl-<id>
test tls_round_trip ... ok
```

N must be 8 or less. All failures in the control must be `CertificateInvalid`.
