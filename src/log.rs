// SPDX-License-Identifier: GPL-3.0-or-later

use std::{env::var_os, path::Path};
use tracing_appender::{
    non_blocking::WorkerGuard,
    rolling::{RollingFileAppender, Rotation},
};
use tracing_subscriber::{
    EnvFilter, Layer, Registry, fmt, fmt::format::FmtSpan, layer::SubscriberExt,
    util::SubscriberInitExt,
};

use crate::types::BoxError;

pub fn init_log(state_dir: &Path) -> Result<WorkerGuard, BoxError> {
    // Initialize a file writer that automatically rotates the log file every day
    let file_appender = RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_prefix("xcplane")
        .filename_suffix("log")
        .build(state_dir)?;

    /*
     Spawn a dedicated background thread to handle actual disk IO:

    - file_writer: A handle used by the logging layers to enqueue log lines
      instantly without blocking the application.

    - guard: A WorkerGuard that must be kept alive (returned by the function).
      When this guard is dropped, it flushes any remaining queued logs to the
      file, ensuring no data is lost.
     */
    let (file_writer, guard) = tracing_appender::non_blocking(file_appender);

    // Create a dynamic filter that determines log levels
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    /*
    Handle output to the console:

    - Writer: Sends logs to standard output stdout

    - Timer: Formats timestamps in RFC 3339 format

    - Span Events: Configured to CLOSE/NONE which shows/skips a performance log
      entry whenever an #[instrument] span finishes based on env variable
      XCPLANE_TIMIMNGS
     */
    let span_events = if var_os("XCPLANE_TIMINGS").is_some() {
        FmtSpan::CLOSE
    } else {
        FmtSpan::NONE
    };

    let stdout_layer = fmt::layer()
        .with_writer(std::io::stdout)
        .with_timer(fmt::time::ChronoLocal::rfc_3339())
        .with_span_events(span_events.clone())
        .with_filter(filter.clone());

    // As well as the same options used for stdout, json output can be enabled
    // with XCPLANE_JSON_LOG for output to the rolling file
    let file_layer = fmt::layer()
        .json()
        .with_writer(file_writer.clone())
        .with_timer(fmt::time::ChronoLocal::rfc_3339())
        .with_span_events(span_events.clone())
        .with_filter(filter.clone());

    let file_layer_json = fmt::layer()
        .with_writer(file_writer)
        .with_timer(fmt::time::ChronoLocal::rfc_3339())
        .with_span_events(span_events)
        .with_filter(filter);

    // Set this entire stack as the global default subscriber with the defined
    // layers (stdout & file)
    if var_os("XCPLANE_JSON_LOG").is_some() {
        Registry::default()
            .with(stdout_layer)
            .with(file_layer)
            .init();
    } else {
        Registry::default()
            .with(stdout_layer)
            .with(file_layer_json)
            .init();
    }

    Ok(guard)
}
