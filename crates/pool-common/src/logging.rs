use std::fmt;
use std::io::IsTerminal;

use chrono::Local;
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::fmt::format::{DefaultFields, Writer};
use tracing_subscriber::fmt::{FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::EnvFilter;

const DEFAULT_LOG_FILTER: &str = "info,postgres=warn,tokio_postgres=warn";
const COMPONENT_COL_WIDTH: usize = 8;

pub fn init_logging() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_FILTER));
    let ansi = std::io::stderr().is_terminal();

    tracing_subscriber::registry()
        .with(filter)
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_ansi(ansi)
                .fmt_fields(DefaultFields::new())
                .event_format(PoolEventFormatter),
        )
        .init();
}

#[derive(Debug, Clone, Copy, Default)]
struct PoolEventFormatter;

impl<S, N> FormatEvent<S, N> for PoolEventFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();
        let ansi = writer.has_ansi_escapes();

        write_colored(
            &mut writer,
            ansi,
            "2;37",
            &Local::now().format("%H:%M:%S").to_string(),
        )?;
        writer.write_char(' ')?;

        let (level_label, level_style) = match *meta.level() {
            Level::ERROR => ("ERR", "1;31"),
            Level::WARN => ("WRN", "1;33"),
            Level::INFO => ("INF", "1;32"),
            Level::DEBUG => ("DBG", "1;34"),
            Level::TRACE => ("TRC", "1;35"),
        };
        write_colored(&mut writer, ansi, level_style, level_label)?;
        writer.write_char(' ')?;

        let target = meta.target();
        let component = pad_or_truncate_ascii(
            target.split("::").next().unwrap_or(target),
            COMPONENT_COL_WIDTH,
        );
        write_colored(&mut writer, ansi, "36", &component)?;
        write!(&mut writer, " | ")?;

        ctx.format_fields(writer.by_ref(), event)?;
        writeln!(writer)
    }
}

fn write_colored(writer: &mut Writer<'_>, ansi: bool, style: &str, text: &str) -> fmt::Result {
    if ansi {
        write!(writer, "\x1b[{style}m{text}\x1b[0m")
    } else {
        writer.write_str(text)
    }
}

fn pad_or_truncate_ascii(value: &str, width: usize) -> String {
    let truncated = value.chars().take(width).collect::<String>();
    format!("{truncated:<width$}")
}

#[cfg(test)]
mod tests {
    use super::pad_or_truncate_ascii;

    #[test]
    fn pad_or_truncate_ascii_pads_short_values() {
        assert_eq!(pad_or_truncate_ascii("api", 8), "api     ");
    }

    #[test]
    fn pad_or_truncate_ascii_truncates_long_values() {
        assert_eq!(pad_or_truncate_ascii("tokio_postgres", 8), "tokio_po");
    }
}
