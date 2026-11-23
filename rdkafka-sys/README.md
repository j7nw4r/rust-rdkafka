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

The **`schannel`** feature selects experimental native Windows TLS and
crypto backends. See [Schannel and BCrypt](#schannel-and-bcrypt-experimental-windows-only).

All features are disabled by default unless noted otherwise above. The build
process is defined in [`build.rs`].

### Schannel and BCrypt (experimental, Windows only)

The bundled librdkafka comes from the experimental
[`feat/tls-crypto-backends`] branch. It adds build-selected backends for broker
TLS and for authentication crypto. Read its [`docs/TLS-CRYPTO-BACKENDS.md`] and
[`docs/SCHANNEL-ADAPTER.md`] for their scope and limits.

The **`schannel`** feature enables `cmake-build` and builds librdkafka
statically with `RDKAFKA_TLS_BACKEND=schannel`,
`RDKAFKA_CRYPTO_BACKEND=bcrypt` and `WITH_SASL=ON`. Broker TLS uses
Schannel/SSPI and Crypt32. SASL SCRAM and the other authentication crypto use
BCrypt/NCrypt. There is no OpenSSL. The build links `secur32`, `crypt32`,
`bcrypt`, `ncrypt` and `ws2_32`.

  * The build fails on a target that is not Windows.
  * The build fails if you also enable `ssl`, `ssl-vendored`, `gssapi`,
    `gssapi-vendored`, `sasl`, `dynamic-linking` or `static-linking`.
  * `curl-static` adds HTTPS for SASL OAUTHBEARER OIDC. curl-sys builds its
    bundled curl with Schannel on Windows, so HTTPS also has no OpenSSL.
    `curl` alone can link a system or vcpkg libcurl with an unknown TLS
    library, so the build fails unless curl-sys builds the bundled curl.
    Do not enable the curl-sys `windows-static-ssl` feature, which builds
    curl with OpenSSL. HTTPS trust comes from `https.ca.location` (a PEM
    file) or `https.ca.pem`, or from the Windows system store when both are
    unset. A CA directory and `https.ca.location=probe` fail.
  * `builtin.features` includes `ssl`, `crypto`, `tls_schannel`,
    `crypto_bcrypt` and `sasl_scram`, and also `http` and `oidc` with
    `curl-static`.
  * Schannel uses TLS 1.2 only. CA trust comes from `ssl.ca.location` or
    `ssl.ca.pem`, or from the Windows system store when both are unset.
  * For mTLS, the client identity must be a certificate with a private key in
    a Windows store: `ssl.schannel.certificate=CurrentUser/My/<thumbprint>` or
    `LocalMachine/My/<thumbprint>`, with the 40-hex SHA-1 thumbprint.
    A PEM key from `ssl.key.location` or `ssl.key.pem` is imported in
    memory, and Schannel can reject it. On the windows-latest CI runner,
    client creation fails with `Schannel cannot use this ephemeral
    private-key identity (0x8009030d)`. `ssl.schannel.certificate` cannot be
    combined with those settings.

```sh
cargo build --release --features schannel,libz-static --example tls_check
```

The `tls_check` example takes `-X key=value` librdkafka properties and checks
a broker connection. By default it creates a new topic, produces records with
distinct keys, consumes them back and compares every key and value. Run it
with `--help` for the other modes and options. It needs no Windows-specific
code, so it also builds without `schannel`.

#### Windows alternatives to `gssapi`, `dynamic-linking` and `static-linking`

These features conflict with `schannel`. Each of the features below enables
`schannel` and fails on a target that is not Windows.

  * **`gssapi-sspi`**: SASL GSSAPI (Kerberos) through Windows SSPI, with the
    credentials of the logged-on user. It needs no Cyrus SASL or OpenSSL.
    The `schannel` build already includes SSPI, so this feature is an
    explicit request for it: the build fails unless librdkafka was built
    with `WITH_SASL`. Set `sasl.mechanisms=GSSAPI` and
    `sasl.kerberos.service.name`. CI checks only that librdkafka selects the
    `Win32 SSPI` provider. Kerberos against a real KDC or Active Directory
    has not been tested.
  * **`schannel-dynamic-linking`**: builds librdkafka as `rdkafka.dll` with
    the same Schannel and BCrypt settings and links it dynamically. The
    build checks the generated `config.h`. `cargo run` and `cargo test`
    find the DLL in `target/<profile>/build/rdkafka-sys-*/out/bin`. To run
    the binary in another way, put `rdkafka.dll` next to the `.exe`.
  * **`schannel-static-linking`**: links a prebuilt static librdkafka
    instead of building one. Set `DEP_LIBRDKAFKA_STATIC_ROOT` to its CMake
    install prefix, which must have `lib/rdkafka.lib`. The build reads the
    object names in that archive. It fails unless the archive has the
    Schannel and BCrypt objects and no OpenSSL TLS object, so an import
    library or an OpenSSL build is refused. Build the library with the same
    CMake settings as the `schannel` feature and `RDKAFKA_BUILD_STATIC=ON`.
    If it uses zlib, zstd or curl, also link those libraries.

```sh
cmake -S rdkafka-sys/librdkafka -B build -DRDKAFKA_TLS_BACKEND=schannel \
  -DRDKAFKA_CRYPTO_BACKEND=bcrypt -DRDKAFKA_BUILD_STATIC=ON -DWITH_SASL=ON \
  -DWITH_CURL=OFF -DWITH_ZLIB=OFF -DWITH_ZSTD=OFF -DENABLE_LZ4_EXT=OFF
cmake --build build --config Release
cmake --install build --config Release --prefix C:/rdkafka-schannel
set DEP_LIBRDKAFKA_STATIC_ROOT=C:/rdkafka-schannel
cargo build --no-default-features --features schannel-static-linking
```

[`feat/tls-crypto-backends`]: https://github.com/j7nw4r/librdkafka/tree/feat/tls-crypto-backends
[`docs/TLS-CRYPTO-BACKENDS.md`]: https://github.com/j7nw4r/librdkafka/blob/32a4e8ec3374afb63c58fd4391cf67eced0f14ee/docs/TLS-CRYPTO-BACKENDS.md
[`docs/SCHANNEL-ADAPTER.md`]: https://github.com/j7nw4r/librdkafka/blob/32a4e8ec3374afb63c58fd4391cf67eced0f14ee/docs/SCHANNEL-ADAPTER.md

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
