// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Some of the code in this file has been taken from tokio-rs/tracing
// https://github.com/tokio-rs/tracing/blob/8aae1c37b091963aafdd336b1168fe5a24c0b4f0/tracing-subscriber/src/fmt/format/pretty.rs
// License MIT

use std::fmt;
use std::fmt::Write as _;

use tracing::{
    field::{self, Field},
    Event, Subscriber,
};
use tracing_subscriber::{
    field::{MakeVisitor, VisitFmt, VisitOutput},
    fmt::{
        format::Writer,
        time::{FormatTime, SystemTime},
        FmtContext, FormatEvent, FormatFields, FormattedFields,
    },
    registry::LookupSpan,
};

const MESSAGE_INDENT: &str = "  ";
const MESSAGE_NEW_LINE_INDENT: &str = "    ";
const FIELD_INDENT: &str = "    ";
const FIELD_NEW_LINE_INDENT: &str = "      ";

/// A pretty event formatter with multi-line indented field layout and no ANSI styling.
#[derive(Debug, Clone)]
pub struct Pretty<T = SystemTime> {
    timer: T,
    display_timestamp: bool,
    display_target: bool,
    display_level: bool,
    display_thread_name: bool,
    display_thread_id: bool,
}

impl Default for Pretty<SystemTime> {
    fn default() -> Self {
        Self {
            timer: SystemTime,
            display_timestamp: true,
            display_target: true,
            display_level: true,
            display_thread_name: false,
            display_thread_id: false,
        }
    }
}

impl<T> Pretty<T> {
    pub fn with_timer<T2>(self, timer: T2) -> Pretty<T2> {
        Pretty {
            timer,
            display_timestamp: self.display_timestamp,
            display_target: self.display_target,
            display_level: self.display_level,
            display_thread_name: self.display_thread_name,
            display_thread_id: self.display_thread_id,
        }
    }

    pub fn without_time(mut self) -> Self {
        self.display_timestamp = false;
        self
    }

    pub fn with_target(mut self, display_target: bool) -> Self {
        self.display_target = display_target;
        self
    }

    pub fn with_level(mut self, display_level: bool) -> Self {
        self.display_level = display_level;
        self
    }

    pub fn with_thread_names(mut self, display_thread_name: bool) -> Self {
        self.display_thread_name = display_thread_name;
        self
    }

    pub fn with_thread_ids(mut self, display_thread_id: bool) -> Self {
        self.display_thread_id = display_thread_id;
        self
    }
}

impl<C, N, T> FormatEvent<C, N> for Pretty<T>
where
    C: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
    T: FormatTime,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, C, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        let meta = event.metadata();

        if self.display_timestamp {
            if self.timer.format_time(&mut writer).is_err() {
                writer.write_str("<unknown time>")?;
            }
            writer.write_char(' ')?;
        }

        if self.display_level {
            write!(writer, "{} ", meta.level())?;
        }

        if self.display_target {
            write!(writer, "{}", meta.target())?;
        }

        let mut v = PrettyVisitor::new(writer.by_ref());
        event.record(&mut v);
        v.finish()?;
        writer.write_char('\n')?;

        if self.display_thread_name || self.display_thread_id {
            write!(writer, "on ")?;
            let thread = std::thread::current();
            if self.display_thread_name {
                if let Some(name) = thread.name() {
                    write!(writer, "{}", name)?;
                    if self.display_thread_id {
                        writer.write_char(' ')?;
                    }
                }
            }
            if self.display_thread_id {
                write!(writer, "{:?}", thread.id())?;
            }
            writer.write_char('\n')?;
        }

        let span = event
            .parent()
            .and_then(|id| ctx.span(id))
            .or_else(|| ctx.lookup_current());

        let scope = span.into_iter().flat_map(|span| span.scope());

        for span in scope {
            let meta = span.metadata();
            if self.display_target {
                write!(writer, "  in {}::{}", meta.target(), meta.name())?;
            } else {
                write!(writer, "  in {}", meta.name())?;
            }

            let ext = span.extensions();
            let fields = &ext
                .get::<FormattedFields<N>>()
                .expect("Unable to find FormattedFields in extensions; this is a bug");
            if !fields.is_empty() {
                write!(writer, "{}", fields)?;
            }
            writer.write_char('\n')?;
        }

        Ok(())
    }
}

/// A field formatter that produces pretty, multi-line indented field output.
#[derive(Debug, Default)]
pub struct PrettyFields;

impl<'a> MakeVisitor<Writer<'a>> for PrettyFields {
    type Visitor = PrettyVisitor<'a>;

    #[inline]
    fn make_visitor(&self, target: Writer<'a>) -> Self::Visitor {
        PrettyVisitor::new(target)
    }
}

/// The visitor produced by [`PrettyFields`] and by [`Pretty`]'s event formatter.
#[derive(Debug)]
pub struct PrettyVisitor<'a> {
    writer: Writer<'a>,
    result: fmt::Result,
}

impl<'a> PrettyVisitor<'a> {
    fn new(writer: Writer<'a>) -> Self {
        Self {
            writer,
            result: Ok(()),
        }
    }

    fn write_padded(
        &mut self,
        value: &impl fmt::Debug,
        first_line_indent: &'static str,
        newline_indent: &'static str,
    ) {
        self.result = self
            .result
            .and_then(|_| write!(self.writer, "\n{}", first_line_indent))
            .and_then(|_| {
                write!(
                    indented_skipping_first_line(&mut self.writer, newline_indent),
                    "{:?}",
                    value
                )
            });
    }
}

impl field::Visit for PrettyVisitor<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        if self.result.is_err() {
            return;
        }

        if field.name() == "message" {
            self.record_debug(field, &format_args!("{}", value))
        } else {
            self.record_debug(field, &value)
        }
    }

    fn record_error(&mut self, field: &Field, value: &(dyn std::error::Error + 'static)) {
        if let Some(source) = value.source() {
            self.record_debug(
                field,
                &format_args!("{}, {}.sources: {}", value, field, ErrorSourceList(source)),
            )
        } else {
            self.record_debug(field, &format_args!("{}", value))
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if self.result.is_err() {
            return;
        }
        match field.name() {
            "message" => self.write_padded(
                &format_args!("{:?}", value),
                MESSAGE_INDENT,
                MESSAGE_NEW_LINE_INDENT,
            ),
            // Skip fields that are actually log metadata that have already been handled
            name if name.starts_with("log.") => self.result = Ok(()),
            name if name.starts_with("r#") => self.write_padded(
                &format_args!("{}: {:?}", &name[2..], value),
                FIELD_INDENT,
                FIELD_NEW_LINE_INDENT,
            ),
            name => self.write_padded(
                &format_args!("{}: {:?}", name, value),
                FIELD_INDENT,
                FIELD_NEW_LINE_INDENT,
            ),
        };
    }
}

impl VisitOutput<fmt::Result> for PrettyVisitor<'_> {
    fn finish(self) -> fmt::Result {
        self.result
    }
}

impl VisitFmt for PrettyVisitor<'_> {
    fn writer(&mut self) -> &mut dyn fmt::Write {
        &mut self.writer
    }
}

/// Renders an error's source chain.
struct ErrorSourceList<'a>(&'a (dyn std::error::Error + 'static));

impl fmt::Display for ErrorSourceList<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut list = f.debug_list();
        let mut curr = Some(self.0);
        while let Some(curr_err) = curr {
            list.entry(&format_args!("{}", curr_err));
            curr = curr_err.source();
        }
        list.finish()
    }
}

/// Inspired by https://docs.rs/indenter/0.3.3/indenter/index.html, MIT license
struct Indented<'a, W: ?Sized> {
    inner: &'a mut W,
    needs_indent: bool,
    indentation: &'static str,
}

/// Indents with the given static indentation, but skipping indenting the first line.
fn indented_skipping_first_line<'a, W: ?Sized>(
    f: &'a mut W,
    indentation: &'static str,
) -> Indented<'a, W> {
    Indented {
        inner: f,
        needs_indent: false,
        indentation,
    }
}

impl<T> fmt::Write for Indented<'_, T>
where
    T: fmt::Write + ?Sized,
{
    fn write_str(&mut self, s: &str) -> fmt::Result {
        for (ind, line) in s.split('\n').enumerate() {
            if ind > 0 {
                self.inner.write_char('\n')?;
                self.needs_indent = true;
            }

            if self.needs_indent {
                // Don't render the line unless it actually has text on it
                if line.is_empty() {
                    continue;
                }

                self.inner.write_str(self.indentation)?;
                self.needs_indent = false;
            }

            self.inner.write_str(line)?;
        }

        Ok(())
    }
}
