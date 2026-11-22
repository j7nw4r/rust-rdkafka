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

The **`rustls`** and **`rustls-fips`** features select an experimental
broker TLS backend. See [rustls and FIPS](#rustls-and-fips-experimental).

All features are disabled by default unless noted otherwise above. The build
process is defined in [`build.rs`].

### rustls and FIPS (experimental)

The bundled librdkafka comes from the experimental
[`feat/tls-backends-fips`] branch. That branch adds a rustls broker TLS
backend (rustls-ffi 0.15.7) and the `ssl.fips.mode=required` policy. Read
its [`docs/TLS-BACKENDS.md`] for the scope of the policy. A passing runtime
FIPS check is not a FIPS 140-3 deployment qualification.

  * The **`rustls`** feature enables `cmake-build` and passes
    `RDKAFKA_TLS_BACKEND=rustls` to CMake. CMake finds rustls-ffi 0.15.7
    with pkg-config, so put the directory that holds its `rustls.pc` in
    `PKG_CONFIG_PATH`. librdkafka is built as a shared library and linked
    dynamically. This keeps the rustls-ffi Rust static library out of your
    Rust binary. A rustls build has no OpenSSL, SASL GSSAPI, SCRAM, OAuth or
    curl. The build fails if you also enable `ssl`, `ssl-vendored`,
    `gssapi`, `gssapi-vendored`, `sasl`, `curl`, `curl-static`,
    `dynamic-linking` or `static-linking`.
  * The **`rustls-fips`** feature enables `rustls`. After CMake runs, the
    build reads the generated `config.h` and fails unless
    `WITH_RUSTLS_FIPS` is 1. Without this check, CMake silently builds a
    non-FIPS library when its FIPS link probe fails, and
    `ssl.fips.mode=required` then refuses every client at run time.

A rustls-ffi release archive is not FIPS. For `rustls-fips`, build
rustls-ffi v0.15.7 from source with Go, Perl, CMake and a C compiler:

```sh
git clone --branch v0.15.7 https://github.com/rustls/rustls-ffi.git
cd rustls-ffi
cargo build --locked --release -p rustls-ffi --no-default-features --features fips,aws-lc-rs
```

Then make a prefix with `include/rustls.h` (from `librustls/src/`),
`lib/librustls.a` (a copy of `target/release/librustls_ffi.a`), the
aws-lc-fips-sys crypto library, and a `lib/pkgconfig/rustls.pc` that you
write. CMake reads `Libs`, not `Libs.private`, so put every link input in
`Libs`. On macOS arm64 aws-lc-fips-sys 0.14.3 makes
`libaws_lc_fips_0_14_3_crypto.dylib` in
`target/release/build/aws-lc-fips-sys-*/out/build/artifacts/`, and this file
worked:

```text
prefix=/opt/rustls-fips
libdir=${prefix}/lib
includedir=${prefix}/include

Name: rustls
Description: rustls-ffi 0.15.7 fips,aws-lc-rs (aws-lc-fips-sys 0.14.3)
Version: 0.15.7
Libs: -L${libdir} -lrustls -laws_lc_fips_0_14_3_crypto -framework Security -framework CoreFoundation -liconv -lSystem -lc -lm
Cflags: -I${includedir}
```

To get the link inputs for another platform, run
`cargo rustc --locked --release -p rustls-ffi --no-default-features --features fips,aws-lc-rs --lib -- --print native-static-libs`.
The `rustls.pc` in a release archive has a relative `prefix`. Change it to
the absolute path of the extracted archive.

```sh
export PKG_CONFIG_PATH=/opt/rustls-fips/lib/pkgconfig
cargo build --features rustls-fips
```

The CMake build sets `CMAKE_INSTALL_RPATH_USE_LINK_PATH`, so the built
librdkafka finds a shared rustls-ffi or crypto library in the directory it
was linked from. The build cannot add a run-time search path to your
binary: `cargo:rustc-link-arg` from a `-sys` crate does not reach the final
binary. `cargo run` and `cargo test` find librdkafka because it is inside
the target directory. To run the binary in another way, or to ship it, put
librdkafka and the libraries it loads where the loader looks:

  * Linux: set `LD_LIBRARY_PATH`, or add an rpath to your binary.
  * macOS: set `DYLD_LIBRARY_PATH`, or add an rpath to your binary.
  * Windows: put the DLLs in the directory of the `.exe`, or in `PATH`.

The build puts librdkafka in `target/<profile>/build/rdkafka-sys-*/out/lib`.
On Windows, CMake puts `rdkafka.dll` in `out/bin` (not tested).

#### Linking an installed rustls librdkafka

You can also build librdkafka yourself and use the `dynamic-linking`
feature without `rustls`. Set `CMAKE_INSTALL_PREFIX` when you configure,
because `cmake --install --prefix` does not change the prefix in
`rdkafka.pc`:

```sh
export PKG_CONFIG_PATH=/opt/rustls-fips/lib/pkgconfig
cmake -S rdkafka-sys/librdkafka -B build -DCMAKE_BUILD_TYPE=Release \
  -DRDKAFKA_TLS_BACKEND=rustls -DWITH_CURL=OFF -DWITH_SASL=OFF \
  -DRDKAFKA_BUILD_TESTS=OFF -DRDKAFKA_BUILD_EXAMPLES=OFF \
  -DCMAKE_INSTALL_PREFIX=/opt/rdkafka-fips
grep WITH_RUSTLS_FIPS build/generated/config.h
cmake --build build && cmake --install build
export PKG_CONFIG_PATH=/opt/rdkafka-fips/lib/pkgconfig:/opt/rustls-fips/lib/pkgconfig
cargo build --features dynamic-linking
```

Make sure that `config.h` has `#define WITH_RUSTLS_FIPS 1`. This route does
not have the `rustls-fips` build check. `rdkafka.pc` has
`Requires.private: rustls`, so `rustls.pc` must also be in
`PKG_CONFIG_PATH`. The installed library is outside the target directory,
so `cargo run` and `cargo test` also need the loader path above.

#### Windows

These notes come from the librdkafka branch's
[`packaging/rustls/WINDOWS-CHECKS.md`]. Nobody has tested them with this
crate on Windows.

  * The `dynamic-linking` route above is the likely route on Windows.
    Build librdkafka with CMake inside a Visual Studio developer prompt.
    Set `PKG_CONFIG_EXECUTABLE` or put `pkg-config.exe` in `PATH`.
  * The aws-lc-fips-sys build needs Perl (for example Strawberry Perl),
    Go, NASM, Ninja (`CMAKE_GENERATOR=Ninja`), CMake and libclang
    (`LIBCLANG_PATH`). Without Perl it stops with
    `Could NOT find Perl (missing: PERL_EXECUTABLE)`.
  * The FIPS `rustls.pc` must be written by hand, with every link input in
    `Libs`. `lib/rustls.lib` is a copy of `rustls_ffi.lib`:

    ```text
    prefix=C:/deps/rustls-fips
    libdir=${prefix}/lib
    includedir=${prefix}/include

    Name: rustls
    Description: rustls-ffi 0.15.7 fips,aws-lc-rs (aws-lc-fips-sys 0.14.3)
    Version: 0.15.7
    Libs: -L${libdir} -lrustls -laws_lc_fips_0_14_3_crypto -llegacy_stdio_definitions -lkernel32 -lntdll -luserenv -lws2_32 -ldbghelp
    Cflags: -I${includedir}
    ```

  * Ship `rdkafka.dll` and `aws_lc_fips_0_14_3_crypto.dll` (FIPS) or
    `rustls.dll` (ordinary release zip) next to the `.exe`. A missing DLL
    fails at start with `STATUS_DLL_NOT_FOUND` (`0xC0000135`).

[`feat/tls-backends-fips`]: https://github.com/j7nw4r/librdkafka/tree/feat/tls-backends-fips
[`docs/TLS-BACKENDS.md`]: https://github.com/j7nw4r/librdkafka/blob/83d4be551f353632f9cbe251ac20755eaff7511e/docs/TLS-BACKENDS.md
[`packaging/rustls/WINDOWS-CHECKS.md`]: https://github.com/j7nw4r/librdkafka/blob/83d4be551f353632f9cbe251ac20755eaff7511e/packaging/rustls/WINDOWS-CHECKS.md

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
