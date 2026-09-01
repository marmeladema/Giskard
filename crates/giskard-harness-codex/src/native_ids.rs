use std::borrow::Borrow;
use std::fmt;

use giskard_core::ids::{ThreadId, TurnId};

macro_rules! native_string_id {
    ($name:ident, $($method:item),* $(,)?) => {
        #[derive(Clone, Debug, PartialEq, Eq, Hash)]
        pub(super) struct $name(String);

        impl $name {
            pub(super) fn new(value: String) -> Self {
                Self(value)
            }

            $($method)*
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

native_string_id!(
    NativeThreadId,
    pub(super) fn as_str(&self) -> &str {
        &self.0
    },
    pub(super) fn into_inner(self) -> String {
        self.0
    }
);
native_string_id!(
    NativeTurnId,
    pub(super) fn as_str(&self) -> &str {
        &self.0
    }
);
native_string_id!(NativeItemId,);
native_string_id!(NativeProcessId,);

impl Borrow<str> for NativeThreadId {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct NativeTurnKey {
    pub(super) thread_id: ThreadId,
    pub(super) native_turn_id: NativeTurnId,
}

impl NativeTurnKey {
    pub(super) fn new(thread_id: ThreadId, native_turn_id: NativeTurnId) -> Self {
        Self {
            thread_id,
            native_turn_id,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct NativeItemKey {
    pub(super) thread_id: ThreadId,
    pub(super) turn_id: TurnId,
    pub(super) native_item_id: NativeItemId,
}

impl NativeItemKey {
    pub(super) fn new(thread_id: ThreadId, turn_id: TurnId, native_item_id: NativeItemId) -> Self {
        Self {
            thread_id,
            turn_id,
            native_item_id,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct NativeProcessKey {
    pub(super) thread_id: ThreadId,
    pub(super) native_process_id: NativeProcessId,
}

impl NativeProcessKey {
    pub(super) fn new(thread_id: ThreadId, native_process_id: NativeProcessId) -> Self {
        Self {
            thread_id,
            native_process_id,
        }
    }
}
