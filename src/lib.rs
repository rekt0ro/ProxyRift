pub mod clash;
pub mod consumer_history;
pub mod intelligence;
pub mod light_gbm;
pub mod light_training;
pub mod singbox;
pub mod source_discovery;
pub mod validator;

use std::sync::OnceLock;

static COMPACT_LOGS_ENABLED: OnceLock<bool> = OnceLock::new();

/// Returns whether routine informational logs should be suppressed.
pub fn compact_logs_enabled() -> bool {
    *COMPACT_LOGS_ENABLED.get_or_init(|| {
        std::env::var("PROXYRIFT_LOG_MODE")
            .is_ok_and(|mode| mode.trim().eq_ignore_ascii_case("compact"))
    })
}

/// Emits a progress line only when verbose logging or a compact checkpoint is enabled.
pub fn should_emit_compact_progress(
    completed: usize,
    batch_size: usize,
    interval: usize,
    terminal: bool,
) -> bool {
    if !compact_logs_enabled() || terminal || interval == 0 {
        return true;
    }

    completed / interval > completed.saturating_sub(batch_size) / interval
}

#[macro_export]
macro_rules! emit_log_if {
    ($condition:expr; $($arg:tt)*) => {
        if $condition {
            println!($($arg)*);
        }
    };
}
