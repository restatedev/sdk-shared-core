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
//! ([`segment`]: `Ledger/a` is `_4cedger_2fa`, a long one is hashed). No
//! namespace's packs are then under another's, so one
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
use sha2::{Digest, Sha256};
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
    /// `s3://bucket/path`, `gs://bucket/path`, `az://container/path`, or
    /// `file:///a/directory`. Credentials and options come from the
    /// environment, as `object_store`'s builders read it, not as each
    /// provider's own SDK does. For S3: `AWS_*` variables, then web identity
    /// (IRSA), ECS and EKS Pod Identity, then the instance metadata service;
    /// never `~/.aws` profiles or SSO. For GCS: `GOOGLE_*` variables, then
    /// the application default credentials file, then the metadata server.
    /// For Azure: `AZURE_*` variables. An S3-compatible store (MinIO, say) is
    /// `s3://bucket/path` with `AWS_ENDPOINT` naming it.
    ///
    /// Making the store sends no request; GCS reads its credentials file, if
    /// any. Its requests are reqwest's, so a database whose packs are in
    /// S3, GCS or Azure must be polled inside the context of a tokio runtime
    /// with IO and time enabled, which is running: a multi-thread runtime
    /// (`Handle::enter` will do), or a current-thread one inside its
    /// `block_on`. Entered but not driven, a request never completes;
    /// without time enabled, its retries panic.
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

    /// The packs of `namespace`, in a directory of their own: for
    /// [`crate::sql::Open::checkpoints`]. Every session of the database must
    /// use the same store, base and namespace.
    ///
    /// A namespace must be unique among the databases sharing the base:
    /// name the service as well as the key (`Ledger/42`, not `42`), as two
    /// services' objects may share a key. And a base belongs to one Restate
    /// cluster: two clusters' databases of the same namespace would share a
    /// directory, and each one's garbage collection deletes the other's
    /// packs.
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

/// The longest [`segment`]: one path component fits any filesystem (255
/// bytes), and a key (1024 bytes in S3, GCS and Azure) keeps room for the
/// base and a pack's name.
const MAX_SEGMENT: usize = 128;

/// How much of an escaped namespace a hashed [`segment`] keeps, so that a
/// person can still tell which database a directory is.
const HASHED_PREFIX: usize = 48;

/// `namespace` as one path segment, injectively, so no namespace's packs
/// are ever under another's:
///
/// - Lowercase letters, digits and `-` as they are, and every other byte,
///   uppercase letters included, as `_` and two lowercase hex digits:
///   `Ledger/a` is `_4cedger_2fa`. All lowercase, so namespaces that differ
///   only in case stay apart on a case-insensitive filesystem (macOS,
///   Windows), where a `file://` store may be. The escape is `_`, not `.`,
///   so that a directory is never hidden.
/// - The empty namespace as `_empty`.
/// - One whose escape would pass [`MAX_SEGMENT`] as the start of its escape
///   and `_h` and its SHA-256: `_h` and `_em` are no escape (`h` and `m`
///   are no hex digits), so neither form is ever another namespace's escape.
///
/// Never empty, never `.` or `..`, nothing an object store escapes, and no
/// `/`, so it is one directory, and no other namespace's is inside it.
pub(crate) fn segment(namespace: &str) -> String {
    if namespace.is_empty() {
        return "_empty".to_owned();
    }
    // Each escaped byte ends a boundary between whole escapes.
    let mut out = String::with_capacity(namespace.len());
    let mut boundaries = Vec::with_capacity(namespace.len());
    for byte in namespace.bytes() {
        match byte {
            b'a'..=b'z' | b'0'..=b'9' | b'-' => out.push(byte as char),
            _ => write!(out, "_{byte:02x}").unwrap(),
        }
        boundaries.push(out.len());
    }
    if out.len() <= MAX_SEGMENT {
        return out;
    }
    let keep = (boundaries.into_iter())
        .take_while(|&end| end <= HASHED_PREFIX)
        .last()
        .unwrap_or(0);
    out.truncate(keep);
    out.push_str("_h");
    for byte in Sha256::digest(namespace.as_bytes()) {
        write!(out, "{byte:02x}").unwrap();
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
            ("Ledger/a", "_4cedger_2fa"),
            ("Ledger/a%20b/notes", "_4cedger_2fa_2520b_2fnotes"),
            ("Ledger/../l1", "_4cedger_2f_2e_2e_2fl1"),
            ("Ledger/", "_4cedger_2f"),
            ("orders-42", "orders-42"),
            ("my_svc.v2", "my_5fsvc_2ev2"),
            ("", "_empty"),
            (".", "_2e"),
            ("..", "_2e_2e"),
        ] {
            assert_eq!(segment(namespace), escaped);
            assert_eq!(Path::from(escaped).as_ref(), escaped);
        }
    }

    #[test]
    fn namespaces_never_share_a_segment() {
        // `_` is escaped too, so an escape never reads as a namespace's own text.
        assert_ne!(segment("a_2fb"), segment("a/b"));
        assert_ne!(segment("a/l1"), segment("a"));
        assert_ne!(segment("_empty"), segment(""));
    }

    /// On a case-insensitive filesystem (macOS, Windows), a `file://` store's
    /// directories differing only in case are one: every segment is lowercase.
    #[test]
    fn namespaces_differing_in_case_differ_in_lowercase() {
        for (a, b) in [("Ledger/A", "Ledger/a"), ("LEDGER", "ledger"), ("Ä", "ä")] {
            assert_eq!(segment(a), segment(a).to_lowercase());
            assert_ne!(segment(a), segment(b));
        }
    }

    /// A namespace whose escape would not fit a path component, or a key
    /// beside its base, is hashed: a readable start, then its SHA-256.
    #[test]
    fn a_long_namespace_is_hashed() {
        // `Orders/` escapes to 11 bytes and each `日` (3 bytes) to 9: 272.
        let long = format!("Orders/{}", "日".repeat(29));
        let hashed = segment(&long);
        assert!(hashed.len() <= super::MAX_SEGMENT, "{hashed}");
        assert!(hashed.starts_with("_4frders_2f"), "{hashed}");
        let (start, hash) = hashed.rsplit_once("_h").unwrap();
        assert!(start.len() <= super::HASHED_PREFIX, "{start}");
        assert_eq!(hash.len(), 64);
        assert_eq!(Path::from(hashed.as_str()).as_ref(), hashed);
        // Namespaces that share their first bytes, or differ only past them.
        assert_ne!(segment(&format!("{long}x")), hashed);
        assert_ne!(segment(&format!("{long}X")), segment(&format!("{long}x")));
        // An escape exactly at the limit is kept; one byte more is hashed.
        let at_limit = "a".repeat(super::MAX_SEGMENT);
        assert_eq!(segment(&at_limit), at_limit);
        assert!(segment(&format!("{at_limit}a")).contains("_h"));
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
        assert_eq!(l1.prefix("Ledger/a"), "var/lib/sessions/_4cedger_2fa");
        let root = ObjectStoreL1::new(Arc::new(InMemory::new()), "").unwrap();
        assert_eq!(root.prefix("Ledger/a"), "_4cedger_2fa");
    }

    /// Each namespace lists only its own packs, whatever the base: the empty
    /// namespace, ones differing in case, and long, hashed ones included.
    #[test]
    fn a_namespace_lists_only_its_own_packs() {
        futures::executor::block_on(async {
            for base in ["", "packs"] {
                let l1 = ObjectStoreL1::new(Arc::new(InMemory::new()), base).unwrap();
                let long = format!("Orders/{}", "日".repeat(29));
                let longer = format!("{long}/a");
                let namespaces = ["", "a", "A", "l1", "a/l1", &long, &longer];
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
        assert_eq!(l1.prefix("Ledger/a"), "packs/_4cedger_2fa");
    }
}
