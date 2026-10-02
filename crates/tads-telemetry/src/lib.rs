//! Observability bootstrap shared by every Lambda.
//!
//! Phase 1 delivers the parts that work without the (deferred) infra:
//! - structured **JSON logs** to stdout → CloudWatch Logs, honouring `RUST_LOG`;
//! - **per-request spans** carrying the X-Ray trace id ([`xray_trace_id`]) so logs and spans
//!   correlate across Lambdas in the X-Ray console / CloudWatch;
//! - **EMF metrics** ([`emit_metric`]) → CloudWatch Metrics.
//!
//! The OTLP → ADOT-layer → X-Ray *span export* pipeline is wired when the ADOT Lambda layer is
//! provisioned (infra task): add the `tracing-opentelemetry` layer in [`init`] behind a build
//! feature. Until then the X-Ray trace id is still captured on every span, so operational
//! correlation already works end-to-end.

use std::io::Write;
use std::sync::Once;

use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;

static INIT: Once = Once::new();

/// Initialise the global tracing subscriber. Idempotent and safe to call from every Lambda
/// `main` (and from tests). Emits newline-delimited JSON log events including span
/// open/close, so handler spans and their fields land in CloudWatch Logs.
pub fn init() {
    INIT.call_once(|| {
        let filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,tads=debug"));

        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .with_current_span(true)
            .with_span_list(false)
            .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
            .with_target(true)
            .flatten_event(true)
            .init();
    });
}

/// The current invocation's X-Ray trace id, read from the `_X_AMZN_TRACE_ID` environment
/// variable Lambda injects. Attach it as a span field (`xray_trace_id`) so every log line can
/// be pivoted back to a single distributed trace. Returns `None` outside Lambda.
pub fn xray_trace_id() -> Option<String> {
    std::env::var("_X_AMZN_TRACE_ID").ok().and_then(|raw| {
        // Format: `Root=1-5759e988-...;Parent=...;Sampled=1`. We want the `Root` value.
        raw.split(';')
            .find_map(|part| part.strip_prefix("Root="))
            .map(str::to_owned)
    })
}

/// Emit a single CloudWatch EMF metric line to stdout.
///
/// EMF is a wire protocol: CloudWatch extracts metrics from a specifically-shaped JSON log line,
/// so this *must* be written as raw stdout JSON (not a `tracing` event, whose JSON shape differs).
/// This is the one sanctioned direct-stdout write — it is metrics emission, not logging.
pub fn emit_metric(
    namespace: &str,
    name: &str,
    value: f64,
    unit: &str,
    dimensions: &[(&str, &str)],
) {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or_default();

    let mut root = serde_json::Map::new();
    let dim_keys: Vec<serde_json::Value> = dimensions
        .iter()
        .map(|(k, _)| serde_json::Value::from(*k))
        .collect();

    root.insert(
        "_aws".to_string(),
        serde_json::json!({
            "Timestamp": now_ms,
            "CloudWatchMetrics": [{
                "Namespace": namespace,
                "Dimensions": [dim_keys],
                "Metrics": [{ "Name": name, "Unit": unit }],
            }],
        }),
    );
    for (k, v) in dimensions {
        root.insert((*k).to_string(), serde_json::Value::from(*v));
    }
    root.insert(name.to_string(), serde_json::json!(value));

    if let Ok(line) = serde_json::to_string(&serde_json::Value::Object(root)) {
        let mut stdout = std::io::stdout().lock();
        let _ = writeln!(stdout, "{line}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xray_trace_id_extracts_root() {
        unsafe {
            std::env::set_var(
                "_X_AMZN_TRACE_ID",
                "Root=1-5759e988-abcdef;Parent=53995c3f;Sampled=1",
            );
        }
        assert_eq!(xray_trace_id().as_deref(), Some("1-5759e988-abcdef"));
        unsafe {
            std::env::remove_var("_X_AMZN_TRACE_ID");
        }
    }
}
