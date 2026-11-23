use rustls::crypto::CryptoProvider;

/// Names of the crypto providers that this build includes.
pub const PROVIDERS: &[&str] = &[
    #[cfg(feature = "aws-lc-rs-fips")]
    "aws-lc-rs-fips",
    #[cfg(all(windows, feature = "cng"))]
    "cng",
    #[cfg(all(windows, feature = "cng"))]
    "cng-fips",
    #[cfg(feature = "ring")]
    "ring",
];

/// Returns the named crypto provider, or `None` if this build does not
/// include it.
pub fn provider(name: &str) -> Option<CryptoProvider> {
    match name {
        #[cfg(feature = "aws-lc-rs-fips")]
        "aws-lc-rs-fips" => Some(rustls::crypto::default_fips_provider()),
        #[cfg(all(windows, feature = "cng"))]
        "cng" => Some(rustls_cng_crypto::default_provider()),
        // Empty unless Windows runs in FIPS mode.
        #[cfg(all(windows, feature = "cng"))]
        "cng-fips" => Some(rustls_cng_crypto::fips_provider()),
        #[cfg(feature = "ring")]
        "ring" => Some(rustls::crypto::ring::default_provider()),
        _ => None,
    }
}
