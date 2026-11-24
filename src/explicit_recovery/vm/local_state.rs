use crate::explicit_recovery::StateScope;
use bytes::Bytes;
use std::collections::HashMap;

pub(crate) enum LocalGet {
    /// Not known here: the VM asks the runtime, which sees the attempt's own writes (G6).
    Unknown,
    Absent,
    Value(Bytes),
}

pub(crate) enum LocalGetKeys {
    Unknown,
    Keys(Vec<String>),
}

#[derive(Debug)]
enum Entry {
    Value(Bytes),
    /// The key has no value: preloaded so, or cleared by this attempt.
    Absent,
    /// This attempt wrote a value the VM did not keep.
    Written,
}

/// A change this attempt makes to a scope.
#[derive(Debug)]
pub(crate) enum StateOp {
    Set(StateScope, String, Bytes),
    Clear(StateScope, String),
    ClearAll(StateScope),
}

#[derive(Debug)]
struct ScopeState {
    /// Whether keys not in `entries` may have a value.
    is_partial: bool,
    entries: HashMap<String, Entry>,
}

impl ScopeState {
    fn get(&self, key: &str) -> LocalGet {
        match self.entries.get(key) {
            Some(Entry::Value(value)) => LocalGet::Value(value.clone()),
            Some(Entry::Absent) => LocalGet::Absent,
            Some(Entry::Written) => LocalGet::Unknown,
            None if self.is_partial => LocalGet::Unknown,
            None => LocalGet::Absent,
        }
    }

    fn get_keys(&self) -> LocalGetKeys {
        if self.is_partial {
            return LocalGetKeys::Unknown;
        }
        let mut keys: Vec<_> = self
            .entries
            .iter()
            .filter(|(_, entry)| !matches!(entry, Entry::Absent))
            .map(|(key, _)| key.clone())
            .collect();
        keys.sort();
        LocalGetKeys::Keys(keys)
    }
}

/// The state known without asking the runtime, by state key (a scope, and a key in it): what
/// the start message preloaded, and what this attempt wrote since, as far as it was kept.
#[derive(Debug)]
pub(crate) struct LocalState {
    object: ScopeState,
    invocation: ScopeState,
}

impl LocalState {
    /// `preloaded`: each state key with its value, or `None` when it is known to have none.
    pub(crate) fn new(
        preloaded: Vec<(StateScope, String, Option<Bytes>)>,
        is_object_scope_partial: bool,
        is_invocation_scope_partial: bool,
    ) -> Self {
        let mut this = Self {
            object: ScopeState {
                is_partial: is_object_scope_partial,
                entries: Default::default(),
            },
            invocation: ScopeState {
                is_partial: is_invocation_scope_partial,
                entries: Default::default(),
            },
        };
        for (scope, key, value) in preloaded {
            this.scope_mut(scope).entries.insert(
                key,
                match value {
                    Some(value) => Entry::Value(value),
                    None => Entry::Absent,
                },
            );
        }
        this
    }

    fn scope(&self, scope: StateScope) -> &ScopeState {
        match scope {
            StateScope::Object => &self.object,
            StateScope::Invocation => &self.invocation,
        }
    }

    fn scope_mut(&mut self, scope: StateScope) -> &mut ScopeState {
        match scope {
            StateScope::Object => &mut self.object,
            StateScope::Invocation => &mut self.invocation,
        }
    }

    pub(crate) fn get(&self, scope: StateScope, key: &str) -> LocalGet {
        self.scope(scope).get(key)
    }

    pub(crate) fn get_keys(&self, scope: StateScope) -> LocalGetKeys {
        self.scope(scope).get_keys()
    }

    /// Apply a change the attempt sent. A value larger than `keep_up_to` is not kept.
    pub(crate) fn apply(&mut self, op: StateOp, keep_up_to: usize) {
        match op {
            StateOp::Set(scope, key, value) => {
                let scope = self.scope_mut(scope);
                if value.len() <= keep_up_to {
                    scope.entries.insert(key, Entry::Value(value));
                } else if scope.is_partial {
                    // A key that is not there is asked for anyway: nothing to remember.
                    scope.entries.remove(&key);
                } else {
                    scope.entries.insert(key, Entry::Written);
                }
            }
            StateOp::Clear(scope, key) => {
                self.scope_mut(scope).entries.insert(key, Entry::Absent);
            }
            StateOp::ClearAll(scope) => {
                let scope = self.scope_mut(scope);
                scope.entries.clear();
                scope.is_partial = false;
            }
        }
    }
}
