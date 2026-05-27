// Thanks! https://github.com/tokio-rs/console/blob/5f6faa22d944735c2b8c312cac03b35a4ab228ef/console-api/tests/bootstrap.rs
// MIT License
//
// Extended with a post-processor that rewrites prost-build's output to use
// our `restate_wire_derive::Message` (instead of `::prost::Message`) and to
// type payload-bearing bytes fields as `crate::Buffer` so they take the
// host-aware encode/decode path. See `src/wire/mod.rs` and the β'' plan.

use std::{path::PathBuf, process::Command};

#[test]
fn bootstrap() {
    let root_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let out_dir = root_dir.join("src/service_protocol/generated");

    if let Err(error) = prost_build::Config::new()
        .bytes(["."])
        .protoc_arg("--experimental_allow_proto3_optional")
        .type_attribute(".", "#[allow(dead_code)]")
        .enum_attribute(".", "#[allow(clippy::enum_variant_names)]")
        .out_dir(out_dir.clone())
        .compile_protos(
            &[root_dir.join("service-protocol/dev/restate/service/protocol.proto")],
            &[root_dir.join("service-protocol")],
        )
    {
        panic!("failed to compile `console-api` protobuf: {error}");
    }

    let generated = out_dir.join("dev.restate.service.protocol.rs");
    post_process(&generated).expect("post-processing generated file");

    let status = Command::new("git")
        .arg("diff")
        .arg("--exit-code")
        .arg("--")
        .arg(out_dir)
        .status();
    match status {
        Ok(status) if !status.success() => panic!("You should commit the protobuf files"),
        Err(error) => panic!("failed to run `git diff`: {error}"),
        Ok(_) => {}
    }
}

// =====================================================================
// Post-processor
// =====================================================================
//
// We rewrite the generated file in two passes. Each pass has a *loud*
// failure mode: if a match count drops to zero (e.g. because prost-build's
// output format changes), the test fails with a clear diagnostic rather
// than silently shipping a half-converted file.
//
// Pass 1: swap `::prost::Message`, `::prost::Oneof`, `::prost::Enumeration`
// derive references for their `::restate_wire_derive::*` counterparts.
//
// Pass 2: rewrite specific payload-bearing bytes fields from
// `::prost::bytes::Bytes` (encoded as `bytes = "bytes"`) to `crate::Buffer`
// (encoded as `bytes = "buffer"`). The list is the canonical set of fields
// that the SDK considers "user payload" — bytes that may legitimately be
// multi-megabyte and that we want to keep in host memory rather than copy
// into shared-core's address space.

fn post_process(path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let contents = std::fs::read_to_string(path)?;
    let after_derives = swap_derives(&contents)?;
    let after_buffers = swap_buffer_fields(&after_derives)?;
    std::fs::write(path, after_buffers)?;
    Ok(())
}

fn swap_derives(input: &str) -> Result<String, Box<dyn std::error::Error>> {
    let swaps = &[
        ("::prost::Message", "::restate_wire_derive::Message"),
        ("::prost::Oneof", "::restate_wire_derive::Oneof"),
        ("::prost::Enumeration", "::restate_wire_derive::Enumeration"),
    ];
    let mut out = input.to_string();
    for (from, to) in swaps {
        let count = out.matches(from).count();
        if count == 0 {
            return Err(format!(
                "derive-swap matcher `{from}` found 0 occurrences; \
                 prost-build output may have changed — update the matcher \
                 or remove this swap if it's no longer needed"
            )
            .into());
        }
        out = out.replace(from, to);
        eprintln!("post-process: swapped {count} occurrences of `{from}` → `{to}`");
    }
    Ok(out)
}

fn swap_buffer_fields(input: &str) -> Result<String, Box<dyn std::error::Error>> {
    // (struct_name, field_name, tag) tuples. The struct name is matched as
    // the closest `pub struct` declaration before the field — we walk line
    // by line and track the most recent `pub struct ... {` we saw. This is
    // robust against arbitrary inter-field comments and prost's exact
    // attribute-line formatting.
    //
    // `tag` is included as a defence-in-depth check: if the proto schema
    // ever renumbers a field, the match fails loudly rather than silently
    // converting the wrong bytes field.
    let buffer_fields: &[(&str, &str, &str)] = &[
        // The canonical payload wrapper. Used everywhere the protocol
        // carries user data: outputs, state values, signal values,
        // awakeable completion values, promise completions, etc.
        ("Value", "content", "1"),
        // State map entry value in StartMessage. State values can be
        // user-sized (per-key arbitrary bytes).
        ("StateEntry", "value", "2"),
        // Call argument bytes — the input to a sys_call.
        ("CallCommandMessage", "parameter", "3"),
        // Send argument bytes — the input to a sys_send.
        ("OneWayCallCommandMessage", "parameter", "3"),
    ];

    // (enum_name, variant_name, tag) tuples for tuple-variant payload
    // bytes — protobuf `oneof` variants typed `bytes` directly, which
    // prost emits as bare `#[prost(bytes, tag = "N")]` (no `= "bytes"`).
    // The only entry today is `ProposeRunCompletionMessage`'s `Result::Value`
    // — every other oneof wraps user payloads in a `Value` struct
    // (caught above by the struct-field pass).
    let buffer_variants: &[(&str, &str, &str)] = &[
        // ProposeRunCompletionMessage carries the user-side run output
        // bytes directly in the oneof rather than via a `Value` wrapper.
        ("Result", "Value", "14"),
    ];

    let mut out = String::with_capacity(input.len());
    let mut current_struct: Option<&str> = None;
    let mut current_enum: Option<&str> = None;
    let mut remaining_fields: Vec<(&str, &str, &str)> = buffer_fields.to_vec();
    let mut remaining_variants: Vec<(&str, &str, &str)> = buffer_variants.to_vec();

    let mut lines = input.lines().peekable();
    while let Some(line) = lines.next() {
        // Track which struct we're "inside" — the most recent `pub struct X {`
        // line. A `pub enum X {` resets `current_struct` and sets
        // `current_enum`. Going the other way works the same. Sub-modules
        // are ignored here: prost emits oneof enums inside their own
        // `pub mod` block but our keying is by enum name alone, which is
        // sufficient as long as the buffer_variants list stays small and
        // the loud-fail count check catches drift.
        if let Some(name) = parse_struct_name(line) {
            current_struct = buffer_fields
                .iter()
                .find(|(s, _, _)| *s == name)
                .map(|(s, _, _)| *s);
            current_enum = None;
            out.push_str(line);
            out.push('\n');
            continue;
        }
        if let Some(name) = parse_enum_name(line) {
            current_enum = buffer_variants
                .iter()
                .find(|(e, _, _)| *e == name)
                .map(|(e, _, _)| *e);
            current_struct = None;
            out.push_str(line);
            out.push('\n');
            continue;
        }

        // Try to match a struct-field buffer declaration:
        //     `    #[prost(bytes = "bytes", tag = "N")]`
        //     `    pub <field>: ::prost::bytes::Bytes,`
        if let Some(struct_name) = current_struct {
            if let Some((leading, tag)) = match_prost_bytes_attr(line) {
                if let Some(next_line) = lines.peek() {
                    if let Some(field) = match_prost_bytes_field(next_line) {
                        if let Some(idx) = remaining_fields
                            .iter()
                            .position(|(s, f, t)| *s == struct_name && *f == field && *t == tag)
                        {
                            out.push_str(&format!(
                                "{leading}#[prost(bytes = \"buffer\", tag = \"{tag}\")]\n"
                            ));
                            let next = lines.next().unwrap();
                            let leading_next: String =
                                next.chars().take_while(|c| c.is_whitespace()).collect();
                            out.push_str(&format!("{leading_next}pub {field}: crate::Buffer,\n"));
                            remaining_fields.remove(idx);
                            continue;
                        }
                    }
                }
            }
        }

        // Try to match a tuple-variant buffer declaration:
        //     `    #[prost(bytes, tag = "N")]`
        //     `    <Variant>(::prost::bytes::Bytes),`
        if let Some(enum_name) = current_enum {
            if let Some((leading, tag)) = match_prost_bare_bytes_attr(line) {
                if let Some(next_line) = lines.peek() {
                    if let Some(variant) = match_prost_bytes_tuple_variant(next_line) {
                        if let Some(idx) = remaining_variants
                            .iter()
                            .position(|(e, v, t)| *e == enum_name && *v == variant && *t == tag)
                        {
                            out.push_str(&format!(
                                "{leading}#[prost(bytes = \"buffer\", tag = \"{tag}\")]\n"
                            ));
                            let next = lines.next().unwrap();
                            let leading_next: String =
                                next.chars().take_while(|c| c.is_whitespace()).collect();
                            out.push_str(&format!("{leading_next}{variant}(crate::Buffer),\n"));
                            remaining_variants.remove(idx);
                            continue;
                        }
                    }
                }
            }
        }

        out.push_str(line);
        out.push('\n');
    }

    if !remaining_fields.is_empty() {
        return Err(format!(
            "buffer-field swap missed entries (list may be out of sync with \
             .proto): {remaining_fields:?}"
        )
        .into());
    }
    if !remaining_variants.is_empty() {
        return Err(format!(
            "buffer-variant swap missed entries (list may be out of sync with \
             .proto): {remaining_variants:?}"
        )
        .into());
    }
    eprintln!(
        "post-process: swapped {} buffer-bearing fields and {} buffer-bearing \
         variants to crate::Buffer",
        buffer_fields.len(),
        buffer_variants.len()
    );
    // Trailing newline from `lines()` loss: prost-build emits a final newline.
    if input.ends_with('\n') && !out.ends_with('\n') {
        out.push('\n');
    }
    Ok(out)
}

/// Returns the struct name if the line is a `pub struct Name {` declaration.
/// Handles arbitrary leading whitespace (prost uses 4-space indent for
/// nested modules).
fn parse_struct_name(line: &str) -> Option<&str> {
    parse_decl_name(line, "pub struct ")
}

/// Returns the enum name if the line is a `pub enum Name {` declaration.
/// Used for oneof variants — prost emits each oneof as `pub enum Result`
/// (or similar) inside a `pub mod` block.
fn parse_enum_name(line: &str) -> Option<&str> {
    parse_decl_name(line, "pub enum ")
}

fn parse_decl_name<'a>(line: &'a str, prefix: &str) -> Option<&'a str> {
    let trimmed = line.trim_start();
    let rest = trimmed.strip_prefix(prefix)?;
    let end = rest
        .find(|c: char| !c.is_ascii_alphanumeric() && c != '_')
        .unwrap_or(rest.len());
    let name = &rest[..end];
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

/// Matches `#[prost(bytes = "bytes", tag = "N")]` and returns the leading
/// whitespace and the tag number.
fn match_prost_bytes_attr(line: &str) -> Option<(&str, &str)> {
    let leading_len = line
        .char_indices()
        .find(|(_, c)| !c.is_whitespace())
        .map(|(i, _)| i)
        .unwrap_or(line.len());
    let (leading, rest) = line.split_at(leading_len);
    let after_open = rest.strip_prefix("#[prost(bytes = \"bytes\", tag = \"")?;
    let close = after_open.find("\")]")?;
    let tag = &after_open[..close];
    if !tag.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    // Defence: ensure nothing follows the closing `)]` except whitespace.
    let tail = &after_open[close + 3..];
    if !tail.trim().is_empty() {
        return None;
    }
    Some((leading, tag))
}

/// Matches `pub <field>: ::prost::bytes::Bytes,` and returns the field name.
fn match_prost_bytes_field(line: &str) -> Option<&str> {
    let trimmed = line.trim_start();
    let rest = trimmed.strip_prefix("pub ")?;
    let colon = rest.find(":")?;
    let field = &rest[..colon];
    if !field.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return None;
    }
    let after = rest[colon..].trim_start_matches(':').trim();
    if after == "::prost::bytes::Bytes," {
        Some(field)
    } else {
        None
    }
}

/// Matches the bare `#[prost(bytes, tag = "N")]` attribute form prost
/// emits for protobuf `oneof` variants typed directly as `bytes` (no
/// `= "bytes"` quantifier — that form is reserved for struct fields).
/// Returns leading whitespace + tag.
fn match_prost_bare_bytes_attr(line: &str) -> Option<(&str, &str)> {
    let leading_len = line
        .char_indices()
        .find(|(_, c)| !c.is_whitespace())
        .map(|(i, _)| i)
        .unwrap_or(line.len());
    let (leading, rest) = line.split_at(leading_len);
    let after_open = rest.strip_prefix("#[prost(bytes, tag = \"")?;
    let close = after_open.find("\")]")?;
    let tag = &after_open[..close];
    if !tag.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    let tail = &after_open[close + 3..];
    if !tail.trim().is_empty() {
        return None;
    }
    Some((leading, tag))
}

/// Matches `<Variant>(::prost::bytes::Bytes),` and returns the variant
/// name. Used to spot the tuple-variant form prost emits for oneof
/// variants carrying raw bytes (e.g. `Value(::prost::bytes::Bytes)`).
fn match_prost_bytes_tuple_variant(line: &str) -> Option<&str> {
    let trimmed = line.trim_start();
    let paren = trimmed.find('(')?;
    let variant = &trimmed[..paren];
    if variant.is_empty()
        || !variant.chars().next().unwrap().is_ascii_uppercase()
        || !variant
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return None;
    }
    let after = trimmed[paren..].trim();
    if after == "(::prost::bytes::Bytes)," {
        Some(variant)
    } else {
        None
    }
}
