//! L1: where a database's checkpoints go once they outgrow Restate, an
//! object store, named by a URL ([`ObjectStoreL1::from_url`]) or by the
//! environment ([`ObjectStoreL1::from_env`], `RESTATE_SQL_OBJECT_STORE`).
//!
//! A database keeps its head, tail, manifests and its last few MiB of packs
//! (L0) in the object's state either way. With an object store, a checkpoint
//! that finds L0 holding [`super::StateOptions::l0_bytes`] promotes it: one
//! pack, uploaded to the store, and L0 empties. A database under that never
//! touches the store. Without one, every pack stays in Restate. See
//! `docs/ARCHITECTURE.md` ("L0 and L1", "With L1 in S3").
//!
//! ```rust,ignore
//! // Once per process: the store holds the HTTP client and its connections.
//! let l1 = ObjectStoreL1::from_env()?;
//! // Per database.
//! let mut open = Database::open(kv);
//! if let Some(l1) = &l1 {
//!     open = open.checkpoints(Arc::new(l1.blobs(namespace)));
//! }
//! ```
//!
//! Each namespace gets its own directory in the store, `<base>/<segment>`,
//! where the segment is the namespace escaped to one path segment
//! ([`segment`]). No namespace's packs are then under another's, so one
//! namespace's garbage collection, which deletes what it lists under its own
//! directory and does not need, never sees another's packs.
//!
//! Which stores a URL may name is the crate's features': a directory
//! (`file://`) always, S3 with `object_store_aws`, GCS with
//! `object_store_gcp`, Azure with `object_store_azure`.
use super::ObjectStoreBlobs;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreScheme};
use std::fmt::Write;
use std::io;
use std::sync::Arc;

/// The environment variable [`ObjectStoreL1::from_env`] reads.
pub const OBJECT_STORE_VAR: &str = "RESTATE_SQL_OBJECT_STORE";

/// An object store, and the directory in it under which every database's
/// packs go, each in its own ([`Self::blobs`]). Make one per process, and
/// share it: clones share the store, and its HTTP client.
#[derive(Clone)]
pub struct ObjectStoreL1 {
    store: Arc<dyn ObjectStore>,
    /// The base directory, as the store names it: empty for the root.
    base: String,
}

impl ObjectStoreL1 {
    /// The databases' packs in `store`, under `base` (`""` for its root): for
    /// a store made otherwise than from a URL, or with options the
    /// environment does not give. `base` must be a path the store keeps as
    /// it is: no empty segment, no leading or trailing `/`, nothing it
    /// escapes (such as `%`).
    pub fn new(store: Arc<dyn ObjectStore>, base: &str) -> io::Result<Self> {
        // A base that its store escapes would be escaped again under each
        // pack's name, and listed under a name the packs' own does not start
        // with: garbage collection would never find them.
        if Path::from(base).as_ref() != base {
            return Err(io::Error::other(format!(
                "the object store's base {base:?} is not a path the store keeps as it is"
            )));
        }
        Ok(Self {
            store,
            base: base.to_owned(),
        })
    }

    /// The object store `url` names, with the base its path gives:
    /// `s3://bucket/path`, `gs://bucket/path`, `az://container/path` (or the
    /// providers' `https://` URLs), or `file:///a/directory`. Credentials and
    /// options come from the provider's own variables (`AWS_*`, `GOOGLE_*`,
    /// `AZURE_*`), as each one's SDK reads them: an S3-compatible store
    /// (MinIO, say) is `s3://bucket/path` with `AWS_ENDPOINT` naming it.
    ///
    /// Making the store does no I/O. Its requests are reqwest's, so a
    /// database whose packs are in S3, GCS or Azure must be polled inside a
    /// tokio runtime's context (`Handle::enter` will do), as everything
    /// reqwest does is.
    pub fn from_url(url: &str) -> io::Result<Self> {
        let fail = |why: &dyn std::fmt::Display| io::Error::other(format!("{url}: {why}"));
        let parsed = url::Url::parse(url).map_err(|e| fail(&e))?;
        let (scheme, base) = ObjectStoreScheme::parse(&parsed).map_err(|e| fail(&e))?;
        let store: Arc<dyn ObjectStore> = match scheme {
            ObjectStoreScheme::Local => Arc::new(LocalFileSystem::new()),
            #[cfg(feature = "object_store_aws")]
            ObjectStoreScheme::AmazonS3 => Arc::new(
                (object_store::aws::AmazonS3Builder::from_env().with_url(url))
                    .build()
                    .map_err(|e| fail(&e))?,
            ),
            #[cfg(feature = "object_store_gcp")]
            ObjectStoreScheme::GoogleCloudStorage => Arc::new(
                (object_store::gcp::GoogleCloudStorageBuilder::from_env().with_url(url))
                    .build()
                    .map_err(|e| fail(&e))?,
            ),
            #[cfg(feature = "object_store_azure")]
            ObjectStoreScheme::MicrosoftAzure => Arc::new(
                (object_store::azure::MicrosoftAzureBuilder::from_env().with_url(url))
                    .build()
                    .map_err(|e| fail(&e))?,
            ),
            #[cfg(not(feature = "object_store_aws"))]
            ObjectStoreScheme::AmazonS3 => {
                return Err(fail(&"S3 needs the feature `object_store_aws`"))
            }
            #[cfg(not(feature = "object_store_gcp"))]
            ObjectStoreScheme::GoogleCloudStorage => {
                return Err(fail(&"GCS needs the feature `object_store_gcp`"))
            }
            #[cfg(not(feature = "object_store_azure"))]
            ObjectStoreScheme::MicrosoftAzure => {
                return Err(fail(&"Azure needs the feature `object_store_azure`"))
            }
            // In memory, a restart loses packs the heads in Restate still
            // name. A plain HTTP server is not one of the stores: an
            // S3-compatible one is `s3://` with `AWS_ENDPOINT`.
            _ => return Err(fail(&"use s3://, gs://, az:// or file://")),
        };
        Self::new(store, base.as_ref()).map_err(|e| fail(&e))
    }

    /// The object store `RESTATE_SQL_OBJECT_STORE` names ([`Self::from_url`]);
    /// `None` when it is unset or empty: every pack then stays in Restate.
    pub fn from_env() -> io::Result<Option<Self>> {
        match std::env::var(OBJECT_STORE_VAR)
            .ok()
            .filter(|value| !value.is_empty())
        {
            Some(value) => Self::from_url(&value)
                .map(Some)
                .map_err(|e| io::Error::other(format!("{OBJECT_STORE_VAR}={e}"))),
            None => Ok(None),
        }
    }

    /// The packs of `namespace` (the key of what its Restate state holds, an
    /// object's, say), in a directory of their own: for
    /// [`crate::sql::Open::checkpoints`]. Every session of the database must
    /// use the same store, base and namespace.
    pub fn blobs(&self, namespace: &str) -> ObjectStoreBlobs {
        ObjectStoreBlobs::new(self.store.clone(), self.prefix(namespace))
    }

    /// The store itself.
    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    /// The directory of `namespace`'s packs.
    fn prefix(&self, namespace: &str) -> String {
        match self.base.is_empty() {
            true => segment(namespace),
            false => format!("{}/{}", self.base, segment(namespace)),
        }
    }
}

/// `namespace` as one path segment, injectively: letters, digits, `-` and
/// `_` as they are, every other byte as `.` and two hex digits (`Ledger/a`
/// is `Ledger.2Fa`), and the empty namespace as `.empty`, which no other's
/// escaping makes. Never empty, nothing an object store escapes, and no `/`,
/// so it is one directory, and no other namespace's is inside it.
pub(crate) fn segment(namespace: &str) -> String {
    if namespace.is_empty() {
        return ".empty".to_owned();
    }
    let mut out = String::with_capacity(namespace.len());
    for byte in namespace.bytes() {
        match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' => out.push(byte as char),
            _ => write!(out, ".{byte:02X}").unwrap(),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{segment, ObjectStoreL1};
    use crate::state::BlobStore;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use std::sync::Arc;

    #[test]
    fn a_namespace_is_one_segment_its_store_keeps_as_it_is() {
        for (namespace, escaped) in [
            ("Ledger/a", "Ledger.2Fa"),
            ("Ledger/a%20b/notes", "Ledger.2Fa.2520b.2Fnotes"),
            ("Ledger/../l1", "Ledger.2F.2E.2E.2Fl1"),
            ("Ledger/", "Ledger.2F"),
            ("", ".empty"),
        ] {
            assert_eq!(segment(namespace), escaped);
            assert_eq!(Path::from(escaped).as_ref(), escaped);
        }
    }

    #[test]
    fn namespaces_never_share_a_segment() {
        // `.` is escaped too, so an escape never reads as a namespace's own text.
        assert_ne!(segment("a.2Fb"), segment("a/b"));
        assert_ne!(segment("a/l1"), segment("a"));
        assert_ne!(segment(".empty"), segment(""));
    }

    #[test]
    fn a_url_the_crate_cannot_serve_is_refused() {
        assert!(ObjectStoreL1::from_url("file:///tmp/a%25b").is_err());
        assert!(ObjectStoreL1::from_url("memory:///").is_err());
        assert!(ObjectStoreL1::from_url("https://example.com/packs").is_err());
        assert!(ObjectStoreL1::from_url("not a url").is_err());
    }

    #[test]
    fn a_base_its_store_would_escape_is_refused() {
        let store = || Arc::new(InMemory::new());
        for base in ["", "packs", "a/b"] {
            assert!(ObjectStoreL1::new(store(), base).is_ok(), "{base}");
        }
        for base in ["/packs", "packs/", "a//b", "a%b"] {
            assert!(ObjectStoreL1::new(store(), base).is_err(), "{base}");
        }
    }

    #[test]
    fn a_namespace_goes_under_the_urls_path() {
        let l1 = ObjectStoreL1::from_url("file:///var/lib/sessions").unwrap();
        assert_eq!(l1.prefix("Ledger/a"), "var/lib/sessions/Ledger.2Fa");
        let root = ObjectStoreL1::new(Arc::new(InMemory::new()), "").unwrap();
        assert_eq!(root.prefix("Ledger/a"), "Ledger.2Fa");
    }

    /// Each namespace lists only its own packs, whatever the base, the
    /// empty namespace included.
    #[test]
    fn a_namespace_lists_only_its_own_packs() {
        futures::executor::block_on(async {
            for base in ["", "packs"] {
                let l1 = ObjectStoreL1::new(Arc::new(InMemory::new()), base).unwrap();
                let namespaces = ["", "a", "l1", "a/l1"];
                for namespace in namespaces {
                    let blobs = l1.blobs(namespace);
                    blobs.put("l1/1-x", Bytes::from_static(b"1")).await.unwrap();
                }
                for namespace in namespaces {
                    assert_eq!(
                        l1.blobs(namespace).list("l1/").await.unwrap(),
                        vec!["l1/1-x".to_owned()],
                        "{base:?} {namespace:?}"
                    );
                }
            }
        })
    }

    #[cfg(not(feature = "object_store_aws"))]
    #[test]
    fn s3_without_its_feature_names_the_feature() {
        let error = ObjectStoreL1::from_url("s3://bucket/packs").err().unwrap();
        assert!(error.to_string().contains("object_store_aws"), "{error}");
    }

    /// An S3-compatible store (MinIO, say) is S3 at another endpoint: the
    /// URL names the bucket, the environment the endpoint.
    #[cfg(feature = "object_store_aws")]
    #[test]
    fn an_s3_url_builds_a_store_under_its_path() {
        let l1 = ObjectStoreL1::from_url("s3://bucket/packs").unwrap();
        assert_eq!(l1.prefix("Ledger/a"), "packs/Ledger.2Fa");
    }
}
