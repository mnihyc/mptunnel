#[cfg(feature = "lab-diagnostics")]
pub(super) fn lab_diagnostic(event: &str, fields: std::fmt::Arguments<'_>) {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if !*ENABLED.get_or_init(|| {
        std::env::var("MPTUNNEL_LAB_DIAG")
            .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
            .unwrap_or(false)
    }) {
        return;
    }
    eprintln!("mptunnel_lab_diag event={event} {fields}");
}
