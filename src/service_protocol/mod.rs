// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod encoding;
mod header;
pub(crate) mod messages;
mod version;

pub(crate) use encoding::{Decoder, DecodingError, Encoder, RawMessage};
pub(crate) use header::{MessageHeader, MessageType};
pub(crate) use version::UnsupportedVersionError;
pub use version::Version;

pub(crate) type NotificationId = messages::notification_template::Id;
pub(crate) type NotificationResult = messages::notification_template::Result;
pub(crate) type CompletionId = u32;

#[derive(Debug)]
pub(crate) struct Notification {
    pub(crate) id: NotificationId,
    pub(crate) result: NotificationResult,
}

pub(crate) const CANCEL_SIGNAL_ID: u32 = 1;
