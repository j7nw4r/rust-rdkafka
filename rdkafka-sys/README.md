# rdkafka-sys

Low level bindings to [librdkafka](https://github.com/confluentinc/librdkafka),
a C library for the [Apache Kafka] protocol with producer, consumer, and
admin clients.

For a safe wrapper, see the [rdkafka] crate.

## Version

The rdkafka-sys version number is in the format `X.Y.Z+RX.RY.RZ`, where
`X.Y.Z` is the version of this crate and follows SemVer conventions, while
`RX.RY.RZ` is the version of the bundled librdkafka.

Note that versions before v2.0.0+1.4.2 did not follow this convention, and
instead directly correspond to the bundled librdkafka version.

## Build

### Known issues

* When any of librdkafka's optional dependencies are enabled, like libz or
  OpenSSL, if you have multiple versions of that library installed upon your
  system, librdkafka's build system may disagree with Cargo about which
  version of the library to use! **This can result in subtly broken
  builds,** if librdkafka compiles against the headers for one version but
  Cargo links against a different version.  For complete confidence when
  building release binaries, use an environment like a Docker container or a
  chroot jail where you can guarantee that only one version of each
  dependency is present. The current design of Cargo unfortunately makes
  this nearly impossible to fix.

* Windows is only supported when using the CMake build system via the
  `cmake-build` Cargo feature.

### Features

By default a submodule with the librdkafka sources will be used to compile
and statically link the library.

The **`dynamic-linking`** feature can be used to link rdkafka to a locally
installed version of librdkafka: if the feature is enabled, the build script
will use `pkg-config` to check the version of the library installed in the
system, and it will configure the compiler to dynamically link against it.
The system version of librdkafka must be at least the version of
librdkafka bundled with this crate.

The **`static-linking`** feature can be used to link rdkafka to a locally
built version of librdkafka: if the feature is enabled, the build script
will try to find `DEP_LIBRDKAFKA_STATIC_ROOT` environment variable
and it will statically link against it.

The **`cmake-build`** feature builds librdkafka with its [CMake] build
system, rather than its default [mklove]-based build system. This feature
requires that CMake is installed on the build machine.

The following features directly correspond to librdkafka features (i.e.,
flags you would pass to `configure` if you were compiling manually).

  * The **`ssl`** feature enables SSL support. By default, the system's
    OpenSSL library is dynamically linked, but static linking of the version
    bundled with the [openssl-sys] crate can be requested with the
    `ssl-vendored` feature.
  * The **`gssapi`** feature enables SASL GSSAPI support with Cyrus
    libsasl2. By default the system's libsasl2 is dynamically linked, but
    static linking of the version bundled with the [sasl2-sys] crate can be
    requested with the `gssapi-vendored` feature.
  * The **`libz`** feature enables support for zlib compression. This
    feature is enabled by default. By default, the system's libz is
    dynamically linked, but static linking of the version bundled with the
    [libz-sys] crate can be requested with the `libz-static` feature.
  * The **`curl`** feature enables the HTTP client via curl. By default, the
    system's curl is dynamically linked, but static linking of the version
    bundled with the [curl-sys] crate can be requested with the
    `curl-static` feature.
  * The **`zstd`** feature enables support for ZSTD compression. By default,
    this builds and statically links the version bundled with the [zstd-sys]
    crate, but dynamic linking of the system's version can be requested with
    the `zstd-pkg-config` feature.
  * The **`external-lz4`** feature statically links against the copy of
    liblz4 bundled with the [lz4-sys] crate. By default, librdkafka
    statically links against its own bundled version of liblz4. Due to
    limitations with lz4-sys, it is not yet possible to dynamically link
    against the system's version of liblz4.

The **`rustls`** feature selects an experimental broker TLS backend. See
[rustls](#rustls-experimental).

All features are disabled by default unless noted otherwise above. The build
process is defined in [`build.rs`].

### rustls (experimental)

The bundled librdkafka comes from the experimental
[`feat/tls-backends-fips`] branch. That branch adds a broker TLS backend
that uses rustls through rustls-ffi 0.15.7, and the `ssl.fips.mode=required`
policy. Read its [`docs/TLS-BACKENDS.md`] for the scope of the policy.

The **`rustls`** feature enables `cmake-build` and builds librdkafka
statically with `RDKAFKA_TLS_BACKEND=rustls`. It adds a dependency on the
[rustls-ffi] crate (`=0.15.7`, no default features, `no_log_capture`).
librdkafka contains no rustls code: its `rustls_*` calls resolve to that
crate in your binary. No C rustls library and no pkg-config are used.

  * The feature needs CMake, a C compiler and Rust 1.85 or later (the
    rustls-ffi MSRV).
  * A rustls build has no OpenSSL, SASL GSSAPI, SCRAM, OAuth or curl. The
    build fails if you also enable `ssl`, `ssl-vendored`, `gssapi`,
    `gssapi-vendored`, `sasl`, `curl`, `curl-static`, `dynamic-linking` or
    `static-linking`.
  * rustls-ffi pins rustls `=0.23.46`. Use rustls `0.23` in your
    application, so that Cargo builds one rustls. One rustls has one
    process-default provider, and librdkafka reads that provider.
  * rustls-ffi has no crypto provider here. Your application must install
    the process-default rustls provider before it creates a client.
    Otherwise client creation fails with `no process-default rustls crypto
    provider is installed`.

On Windows, [rustls-cng-crypto] uses Windows CNG (BCrypt). Other platforms
need another provider. This example uses the rustls `ring` provider as a
stand-in:

```toml
[dependencies]
rdkafka = { version = "0.40", features = ["rustls"] }

[target.'cfg(windows)'.dependencies]
rustls-cng-crypto = "0.1.2"

[target.'cfg(not(windows))'.dependencies]
rustls = { version = "0.23", default-features = false, features = ["std", "tls12", "ring"] }
```

```rust,ignore
use rdkafka::config::ClientConfig;
use rdkafka::producer::BaseProducer;

fn main() {
    #[cfg(windows)]
    let provider = rustls_cng_crypto::default_provider();
    #[cfg(not(windows))]
    let provider = rustls::crypto::ring::default_provider();
    provider
        .install_default()
        .expect("a default provider is already installed");

    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", "broker.example.com:9093")
        .set("security.protocol", "ssl")
        .set("ssl.ca.location", "ca.pem")
        .create()
        .expect("producer creation failed");
}
```

#### FIPS

FIPS mode depends on the provider that the application installs. With
`ssl.fips.mode=required`, client creation fails unless the provider
reports FIPS and the rustls client configuration is FIPS compatible.
The error is `the process-default rustls crypto provider does not report
FIPS mode`. A passing runtime check is not a FIPS 140-3 deployment
qualification.

  * rustls-cng-crypto reports FIPS only when Windows runs in FIPS mode.
    `rustls_cng_crypto::fips_provider()` keeps only FIPS-approved
    algorithms, and it is empty when Windows does not run in FIPS mode.
  * The rustls `fips` feature gives `rustls::crypto::default_fips_provider()`,
    an aws-lc-rs FIPS provider. Its build needs Go and CMake. The tests use
    it as a FIPS stand-in on macOS.

#### Example application

[`examples/rustls`] is a separate crate outside the workspace, so that its
providers are not dependencies of this crate. Its features select the
providers: `ring` (the default), `aws-lc-rs-fips` and `cng` (Windows). Its
test checks that a client is refused without a provider, that an ordinary
client works with one, and that required mode works only when the provider
reports FIPS:

```sh
cd examples/rustls
cargo test
RDKAFKA_TEST_PROVIDER=aws-lc-rs-fips cargo test --features aws-lc-rs-fips
cargo run --features aws-lc-rs-fips -- --provider aws-lc-rs-fips --fips \
  --ca ca.crt --cert client.crt --key client.key \
  --brokers localhost:9093 --topic test
```

On Windows, use `--no-default-features --features cng` and
`--provider cng`. This crate has not been tested on Windows yet.

[`feat/tls-backends-fips`]: https://github.com/j7nw4r/librdkafka/tree/feat/tls-backends-fips
[`docs/TLS-BACKENDS.md`]: https://github.com/j7nw4r/librdkafka/blob/feat/tls-backends-fips/docs/TLS-BACKENDS.md
[`examples/rustls`]: https://github.com/fede1024/rust-rdkafka/tree/master/examples/rustls
[rustls-ffi]: https://crates.io/crates/rustls-ffi
[rustls-cng-crypto]: https://crates.io/crates/rustls-cng-crypto
[`build.rs`]: https://github.com/fede1024/rust-rdkafka/tree/master/rdkafka-sys/build.rs
[Apache Kafka]: https://kafka.apache.org
[CMake]: https://cmake.org
[libz-sys]: https://crates.io/crates/libz-sys
[curl-sys]: https://crates.io/crates/curl-sys
[lz4-sys]: https://crates.io/crates/lz4-sys
[mklove]: https://github.com/edenhill/mklove
[openssl-sys]: https://crates.io/crates/openssl-sys
[rdkafka]: https://docs.rs/rdkafka
[sasl2-sys]: https://docs.rs/sasl2-sys
[zstd-sys]: https://crates.io/crates/zstd-sys
