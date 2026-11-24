//! A commit of the state layer ([`crate::state`]'s `KvStore::commit`) as one atomic group: the
//! commit path of a `KvStore` over the VM.
//!
//! ```text
//! begin_commit(vm, StateScope::Object, puts, deletes, head_key, head, ATOMIC_GROUP_BUDGET)?;
//! // what else commits with it, synchronously: the transaction's sends, with cmd_send
//! let commit = end_commit(vm)?;        // on any error before it: vm.atomic_group_abort()
//! // only now await `commit`'s notification: the commit is durable when it comes
//! ```
//!
//! Nothing between [`begin_commit`] and [`end_commit`] may await: the VM is modal while the group
//! is open.

use super::{ActionOptions, ExplicitRecoveryVM, RecordLevel, StateScope};
use crate::{NotificationHandle, VMResult};
use bytes::Bytes;

/// The most a commit puts in its atomic group, in bytes of keys and values: a quarter of the
/// runtime's default message limit (32 MiB). The runtime does not tell its limit, so the SDK
/// keeps its own budget.
pub const ATOMIC_GROUP_BUDGET: usize = 8 * 1024 * 1024;

/// Begin a commit: `puts`, then `deletes`, then `head` under `head_key`, the write that commits.
///
/// What does not fit in `budget` leaves first, as ordinary writes with no result: puts whose keys
/// nothing names until the head does, so a crash can cut them anywhere (G2). Then it opens an
/// atomic group, with the rest of the puts, the deletes and the head. Every write is
/// [`RecordLevel::None`] and asks for no result.
///
/// On success the group is open: add what commits with it, then call [`end_commit`], or
/// [`ExplicitRecoveryVM::atomic_group_abort`]. On error the attempt failed: the VM already wrote
/// `ErrorMessage`.
pub fn begin_commit<VM: ExplicitRecoveryVM>(
    vm: &mut VM,
    scope: StateScope,
    puts: Vec<(String, Bytes)>,
    deletes: &[String],
    head_key: &str,
    head: Bytes,
    budget: usize,
) -> VMResult<()> {
    let options = || ActionOptions::default().recorded(RecordLevel::None);
    let size = |key: &str, value: &Bytes| key.len() + value.len();
    let mut in_group = size(head_key, &head)
        + deletes.iter().map(String::len).sum::<usize>()
        + puts.iter().map(|(k, v)| size(k, v)).sum::<usize>();

    let mut puts = puts.into_iter().peekable();
    while in_group > budget {
        let Some((key, value)) = puts.next() else {
            break;
        };
        in_group -= size(&key, &value);
        vm.cmd_state_set(scope, key, value, options(), false)?;
    }

    vm.atomic_group_start()?;
    for (key, value) in puts {
        vm.cmd_state_set(scope, key, value, options(), false)?;
    }
    for key in deletes {
        vm.cmd_state_clear(scope, key.clone(), options(), false)?;
    }
    vm.cmd_state_set(scope, head_key.to_owned(), head, options(), false)?;
    Ok(())
}

/// Close the commit's group and send it. Its result, `Void`, is the commit: every write of it
/// is applied and durable.
pub fn end_commit<VM: ExplicitRecoveryVM>(vm: &mut VM) -> VMResult<NotificationHandle> {
    Ok(vm
        .atomic_group_end(ActionOptions::named("commit"), true)?
        .expect("the commit's result is wanted"))
}
