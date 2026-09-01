use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use giskard_core::error::HarnessError;
use giskard_core::event::AgentEvent;
use giskard_core::ids::ThreadId;
use giskard_harness::{AgentEventStream, DiscoveryTicket, ThreadAttachment, ThreadHandle};
use tokio::sync::broadcast;

use crate::native_ids::NativeThreadId;

const EVENT_ROUTE_CAPACITY: usize = 256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct SlotId(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ActivationKey(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ClaimKey(u64);

/// Unforgeable permission to map and deliver through one active route activation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ActiveRoute {
    slot: SlotId,
    activation: ActivationKey,
    thread_id: ThreadId,
    native_thread_id: NativeThreadId,
}

impl ActiveRoute {
    pub(super) fn thread_id(&self) -> ThreadId {
        self.thread_id
    }
}

pub(super) struct RouteDiscovery {
    pub(super) route: ActiveRoute,
    pub(super) ticket: Option<DiscoveryTicket>,
}

pub(super) enum FreshRouteConflict {
    AuthoritativeNative { thread: ThreadId },
    NewNativeForBoundThread { existing_native: String },
}

pub(super) enum ReplaceRouteFailure {
    AuthoritativeNative { thread: ThreadId },
    NewProviderRoute(HarnessError),
}

impl FreshRouteConflict {
    pub(super) fn as_error(&self, native: &str, proposed: ThreadId) -> HarnessError {
        match self {
            Self::AuthoritativeNative { thread } => HarnessError::Protocol(format!(
                "fresh native thread {native} is already authoritative for {thread}, not {proposed}"
            )),
            Self::NewNativeForBoundThread { existing_native } => HarnessError::Protocol(format!(
                "thread {proposed} is already bound to native thread {existing_native}, not {native}"
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DiscoveryState {
    Idle,
    Pending,
    Queued(ClaimKey),
}

enum Custody {
    Unattached {
        receiver: AgentEventStream,
        discovery: DiscoveryState,
    },
    Attaching(ClaimKey),
    Owned(ClaimKey),
}

struct ActiveSlot {
    activation: ActivationKey,
    sender: broadcast::Sender<AgentEvent>,
    custody: Custody,
}

enum SlotState {
    Active(ActiveSlot),
    Tombstoned,
}

struct RouteSlot {
    native: NativeThreadId,
    thread: ThreadId,
    state: SlotState,
}

struct RouteTable {
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Index native Codex identities into their one authoritative route slot.
    // Source of truth: Bootstrap, explicit open, parent claim, or eligible traffic.
    // Structural reason: Protocol frames must resolve without depending on server state.
    // Synchronization: Both indexes and the slot mutate under `CodexRouteAuthority`'s lock.
    // Invalidation/removal: Tombstones retain identity; shutdown clears the table.
    by_native: HashMap<NativeThreadId, SlotId>,
    // ENTITY-AUTHORITY-EXCEPTION:
    // Role: Enforce the reverse Giskard-to-native half of the same route slots.
    // Source of truth: The same transition that updates `by_native` updates this index.
    // Structural reason: Bijection conflicts must fail before mapper or delivery mutation.
    // Synchronization: Both indexes and the slot mutate under `CodexRouteAuthority`'s lock.
    // Invalidation/removal: Exact replacement updates both indexes; shutdown clears the table.
    by_thread: HashMap<ThreadId, SlotId>,
    // ENTITY-AUTHORITY-OWNER:
    // Role: Own each route slot addressed by the two identity indexes above.
    // Source of truth: Codex route establishment and transition operations.
    // Structural reason: Both indexes must share one physical delivery and lifecycle record.
    // Synchronization: Slots and indexes mutate atomically under `CodexRouteAuthority`'s lock.
    // Invalidation/removal: Tombstones retain slots; shutdown clears every slot.
    slots: HashMap<SlotId, RouteSlot>,
    next_slot: u64,
    next_activation: u64,
    next_claim: u64,
    closed: bool,
}

impl Default for RouteTable {
    fn default() -> Self {
        Self {
            by_native: HashMap::new(),
            by_thread: HashMap::new(),
            slots: HashMap::new(),
            next_slot: 1,
            next_activation: 1,
            next_claim: 1,
            closed: false,
        }
    }
}

/// Sole in-process authority for route identity, delivery custody, discovery, and tombstones.
#[derive(Clone, Default)]
pub(super) struct CodexRouteAuthority {
    inner: Arc<Mutex<RouteTable>>,
}

impl CodexRouteAuthority {
    pub(super) fn bootstrap(
        &self,
        native: String,
        thread: ThreadId,
    ) -> Result<ActiveRoute, HarnessError> {
        let native = normalized_native(native)?;
        let mut table = self.lock()?;
        table.ensure_open()?;
        let slot = table.establish(native, thread)?;
        table.active_route(slot)
    }

    /// Empty native IDs retain the caller's explicitly scoped active fallback.
    pub(super) fn resolve(
        &self,
        native: &str,
        fallback: ThreadId,
    ) -> Result<ActiveRoute, HarnessError> {
        let native = native.trim();
        let table = self.lock()?;
        if native.is_empty() {
            return table
                .by_thread
                .get(&fallback)
                .copied()
                .and_then(|slot| table.active_route(slot).ok())
                .ok_or_else(stale_error);
        }
        let Some(slot) = table.by_native.get(native).copied() else {
            return Err(HarnessError::Protocol(format!(
                "native thread {native} has no active route"
            )));
        };
        table.active_route(slot)
    }

    pub(super) fn active_for_thread(&self, thread: ThreadId) -> Result<ActiveRoute, HarnessError> {
        let table = self.lock()?;
        let slot = table
            .by_thread
            .get(&thread)
            .copied()
            .ok_or_else(stale_error)?;
        table.active_route(slot)
    }

    pub(super) fn discover(
        &self,
        native: String,
        proposed: ThreadId,
    ) -> Result<RouteDiscovery, HarnessError> {
        let native = normalized_native(native)?;
        let mut table = self.lock()?;
        table.ensure_open()?;
        let slot = table.establish(native, proposed)?;
        let route = table.active_route(slot)?;
        let ticket = table.queue_idle(slot, &self.inner)?;
        Ok(RouteDiscovery { route, ticket })
    }

    pub(super) fn take_pending_discovery(&self) -> Result<Option<DiscoveryTicket>, HarnessError> {
        let mut table = self.lock()?;
        let slot = table.slots.iter().find_map(|(id, slot)| {
            matches!(
                slot.state,
                SlotState::Active(ActiveSlot {
                    custody: Custody::Unattached {
                        discovery: DiscoveryState::Pending,
                        ..
                    },
                    ..
                })
            )
            .then_some(*id)
        });
        slot.map(|slot| table.queue_pending(slot, &self.inner))
            .transpose()
    }

    /// Parent claims may establish a route, but may not reactivate a tombstone.
    pub(super) fn claim_parent<F>(
        &self,
        native: String,
        proposed: ThreadId,
        make_handle: F,
    ) -> Result<ThreadAttachment, HarnessError>
    where
        F: FnOnce(ThreadId) -> ThreadHandle,
    {
        let native = normalized_native(native)?;
        let mut table = self.lock()?;
        table.ensure_open()?;
        let slot = table.establish(native, proposed)?;
        table.claim_attachment(slot, make_handle, &self.inner)
    }

    /// Claim a provider-created native identity, classifying collisions before any mutation.
    pub(super) fn claim_fresh<F>(
        &self,
        native: String,
        proposed: ThreadId,
        make_handle: F,
    ) -> Result<Result<ThreadAttachment, FreshRouteConflict>, HarnessError>
    where
        F: FnOnce(ThreadId) -> ThreadHandle,
    {
        let native = normalized_native(native)?;
        let mut table = self.lock()?;
        table.ensure_open()?;
        if let Some(slot) = table.by_native.get(&native).copied() {
            return Ok(Err(FreshRouteConflict::AuthoritativeNative {
                thread: table.slot(slot)?.thread,
            }));
        }
        if let Some(slot) = table.by_thread.get(&proposed).copied() {
            return Ok(Err(FreshRouteConflict::NewNativeForBoundThread {
                existing_native: table.slot(slot)?.native.as_str().to_owned(),
            }));
        }
        let slot = table.establish(native, proposed)?;
        table
            .claim_attachment(slot, make_handle, &self.inner)
            .map(Ok)
    }

    /// Explicit durable reopen is the only claim that reactivates an exact tombstone.
    pub(super) fn reattach<F>(
        &self,
        native: String,
        thread: ThreadId,
        make_handle: F,
    ) -> Result<ThreadAttachment, HarnessError>
    where
        F: FnOnce(ThreadId) -> ThreadHandle,
    {
        let native = normalized_native(native)?;
        let mut table = self.lock()?;
        table.ensure_open()?;
        let slot_id = table.exact_slot(native.as_str(), thread)?;
        if matches!(table.slot(slot_id)?.state, SlotState::Tombstoned) {
            let activation = table.allocate_activation()?;
            let (sender, receiver) = broadcast::channel(EVENT_ROUTE_CAPACITY);
            table.slot_mut(slot_id)?.state = SlotState::Active(ActiveSlot {
                activation,
                sender,
                custody: Custody::Unattached {
                    receiver: AgentEventStream::new(receiver),
                    discovery: DiscoveryState::Idle,
                },
            });
        }
        table.claim_attachment(slot_id, make_handle, &self.inner)
    }

    /// A provider-confirmed resume may establish an absent route or reactivate an exact tombstone.
    pub(super) fn resume<F>(
        &self,
        native: String,
        thread: ThreadId,
        make_handle: F,
    ) -> Result<ThreadAttachment, HarnessError>
    where
        F: FnOnce(ThreadId) -> ThreadHandle,
    {
        let native = normalized_native(native)?;
        let mut table = self.lock()?;
        table.ensure_open()?;
        let slot_id = table.establish(native, thread)?;
        if matches!(table.slot(slot_id)?.state, SlotState::Tombstoned) {
            let activation = table.allocate_activation()?;
            let (sender, receiver) = broadcast::channel(EVENT_ROUTE_CAPACITY);
            table.slot_mut(slot_id)?.state = SlotState::Active(ActiveSlot {
                activation,
                sender,
                custody: Custody::Unattached {
                    receiver: AgentEventStream::new(receiver),
                    discovery: DiscoveryState::Idle,
                },
            });
        }
        table.claim_attachment(slot_id, make_handle, &self.inner)
    }

    /// Replace one exact unattached activation after resume fallback.
    pub(super) fn replace<F>(
        &self,
        expected_native: String,
        new_native: String,
        thread: ThreadId,
        make_handle: F,
    ) -> Result<ThreadAttachment, HarnessError>
    where
        F: FnOnce(ThreadId) -> ThreadHandle,
    {
        let expected = normalized_native(expected_native)?;
        let new_native = normalized_native(new_native)?;
        let mut table = self.lock()?;
        table.ensure_open()?;
        let slot_id = table.exact_slot(expected.as_str(), thread)?;
        if new_native != expected && table.by_native.contains_key(&new_native) {
            return Err(HarnessError::Protocol(format!(
                "native thread {new_native} is already bound"
            )));
        }
        let retained = match &mut table.slot_mut(slot_id)?.state {
            SlotState::Active(ActiveSlot {
                sender,
                custody: Custody::Unattached { receiver, .. },
                ..
            }) => Some((sender.clone(), std::mem::replace(receiver, closed_stream()))),
            SlotState::Tombstoned => None,
            SlotState::Active(_) => {
                return Err(HarnessError::Protocol(format!(
                    "cannot replace attached native thread {expected}"
                )));
            }
        };
        let activation = table.allocate_activation()?;
        let fresh_delivery = broadcast::channel(EVENT_ROUTE_CAPACITY);
        let (sender, receiver) = match retained {
            Some(delivery) => delivery,
            None => (fresh_delivery.0, AgentEventStream::new(fresh_delivery.1)),
        };
        table.by_native.remove(&expected);
        let slot = table.slot_mut(slot_id)?;
        slot.native = new_native.clone();
        slot.state = SlotState::Active(ActiveSlot {
            activation,
            sender,
            custody: Custody::Unattached {
                receiver,
                discovery: DiscoveryState::Idle,
            },
        });
        table.by_native.insert(new_native, slot_id);
        table.claim_attachment(slot_id, make_handle, &self.inner)
    }

    /// Replace after a failed resume while preserving an already-authoritative returned native ID.
    pub(super) fn replace_fresh<F>(
        &self,
        expected_native: String,
        new_native: String,
        thread: ThreadId,
        make_handle: F,
    ) -> Result<ThreadAttachment, ReplaceRouteFailure>
    where
        F: FnOnce(ThreadId) -> ThreadHandle,
    {
        let authoritative = {
            let table = self.lock().map_err(ReplaceRouteFailure::NewProviderRoute)?;
            table
                .by_native
                .get(new_native.trim())
                .copied()
                .filter(|_| new_native.trim() != expected_native.trim())
                .and_then(|slot| table.slot(slot).ok().map(|route| route.thread))
        };
        if let Some(thread) = authoritative {
            return Err(ReplaceRouteFailure::AuthoritativeNative { thread });
        }
        self.replace(expected_native, new_native, thread, make_handle)
            .map_err(ReplaceRouteFailure::NewProviderRoute)
    }

    /// Invalidate all outstanding capabilities and close delivery before provider deletion.
    pub(super) fn tombstone(&self, native: &str, thread: ThreadId) -> Result<(), HarnessError> {
        let mut table = self.lock()?;
        let slot = table.exact_slot(native.trim(), thread)?;
        table.slot_mut(slot)?.state = SlotState::Tombstoned;
        Ok(())
    }

    pub(super) fn deliver(
        &self,
        route: &ActiveRoute,
        event: AgentEvent,
    ) -> Result<(), HarnessError> {
        let table = self.lock()?;
        let slot = table.slot(route.slot)?;
        let SlotState::Active(active) = &slot.state else {
            return Err(stale_error());
        };
        if active.activation != route.activation {
            return Err(stale_error());
        }
        if event.thread_id() != route.thread_id {
            return Err(HarnessError::Protocol(format!(
                "mapped event for thread {} cannot use route for {}",
                event.thread_id(),
                route.thread_id
            )));
        }
        active.sender.send(event).map(|_| ()).map_err(|_| {
            HarnessError::Transport(format!(
                "native thread {} has no event receiver",
                route.native_thread_id
            ))
        })
    }

    pub(super) fn close(&self) {
        let mut table = match self.inner.lock() {
            Ok(table) => table,
            Err(poisoned) => poisoned.into_inner(),
        };
        table.closed = true;
        table.by_native.clear();
        table.by_thread.clear();
        table.slots.clear();
    }

    fn lock(&self) -> Result<MutexGuard<'_, RouteTable>, HarnessError> {
        lock_table(&self.inner)
    }
}

impl RouteTable {
    fn ensure_open(&self) -> Result<(), HarnessError> {
        (!self.closed)
            .then_some(())
            .ok_or_else(|| HarnessError::Transport("Codex route authority is closed".into()))
    }

    fn establish(
        &mut self,
        native: NativeThreadId,
        proposed: ThreadId,
    ) -> Result<SlotId, HarnessError> {
        if let Some(slot) = self.by_native.get(&native).copied() {
            if self
                .by_thread
                .get(&proposed)
                .is_some_and(|other| *other != slot)
            {
                return Err(binding_error(proposed));
            }
            return Ok(slot);
        }
        if self.by_thread.contains_key(&proposed) {
            return Err(binding_error(proposed));
        }
        let slot = self.allocate_slot()?;
        let activation = self.allocate_activation()?;
        let (sender, receiver) = broadcast::channel(EVENT_ROUTE_CAPACITY);
        self.slots.insert(
            slot,
            RouteSlot {
                native: native.clone(),
                thread: proposed,
                state: SlotState::Active(ActiveSlot {
                    activation,
                    sender,
                    custody: Custody::Unattached {
                        receiver: AgentEventStream::new(receiver),
                        discovery: DiscoveryState::Idle,
                    },
                }),
            },
        );
        self.by_native.insert(native, slot);
        self.by_thread.insert(proposed, slot);
        Ok(slot)
    }

    fn active_route(&self, slot: SlotId) -> Result<ActiveRoute, HarnessError> {
        let route = self.slot(slot)?;
        let SlotState::Active(active) = &route.state else {
            return Err(HarnessError::Protocol(format!(
                "native thread {} is tombstoned",
                route.native
            )));
        };
        Ok(ActiveRoute {
            slot,
            activation: active.activation,
            thread_id: route.thread,
            native_thread_id: route.native.clone(),
        })
    }

    fn queue_idle(
        &mut self,
        slot_id: SlotId,
        authority: &Arc<Mutex<RouteTable>>,
    ) -> Result<Option<DiscoveryTicket>, HarnessError> {
        let claim = self.allocate_claim()?;
        let slot = self.slot_mut(slot_id)?;
        match &mut slot.state {
            SlotState::Active(ActiveSlot {
                activation,
                custody:
                    Custody::Unattached {
                        discovery: state @ DiscoveryState::Idle,
                        ..
                    },
                ..
            }) => {
                *state = DiscoveryState::Queued(claim);
                Ok(Some(make_ticket(
                    authority,
                    slot_id,
                    *activation,
                    claim,
                    slot.thread,
                    slot.native.clone(),
                )))
            }
            SlotState::Active(_) => Ok(None),
            SlotState::Tombstoned => Err(stale_error()),
        }
    }

    fn queue_pending(
        &mut self,
        slot_id: SlotId,
        authority: &Arc<Mutex<RouteTable>>,
    ) -> Result<DiscoveryTicket, HarnessError> {
        let claim = self.allocate_claim()?;
        let slot = self.slot_mut(slot_id)?;
        let SlotState::Active(active) = &mut slot.state else {
            return Err(stale_error());
        };
        let Custody::Unattached { discovery, .. } = &mut active.custody else {
            return Err(stale_error());
        };
        if *discovery != DiscoveryState::Pending {
            return Err(stale_error());
        }
        *discovery = DiscoveryState::Queued(claim);
        Ok(make_ticket(
            authority,
            slot_id,
            active.activation,
            claim,
            slot.thread,
            slot.native.clone(),
        ))
    }

    fn claim_attachment<F>(
        &mut self,
        slot_id: SlotId,
        make_handle: F,
        authority: &Arc<Mutex<RouteTable>>,
    ) -> Result<ThreadAttachment, HarnessError>
    where
        F: FnOnce(ThreadId) -> ThreadHandle,
    {
        let claim = self.allocate_claim()?;
        let slot = self.slot_mut(slot_id)?;
        let SlotState::Active(active) = &mut slot.state else {
            return Err(HarnessError::Protocol(format!(
                "native thread {} is tombstoned",
                slot.native
            )));
        };
        let Custody::Unattached { receiver, .. } = &mut active.custody else {
            return Err(HarnessError::Protocol(format!(
                "native thread {} already has an attachment or owner",
                slot.native
            )));
        };
        let stream = std::mem::replace(receiver, closed_stream());
        let handle = make_handle(slot.thread);
        if handle.thread != slot.thread || handle.harness_thread_id.trim() != slot.native.as_str() {
            *receiver = stream;
            return Err(HarnessError::Protocol(
                "attachment handle does not match its authoritative route".into(),
            ));
        }
        active.custody = Custody::Attaching(claim);
        Ok(make_attachment(
            authority,
            slot_id,
            active.activation,
            claim,
            handle,
            stream,
        ))
    }

    fn exact_slot(&self, native: &str, thread: ThreadId) -> Result<SlotId, HarnessError> {
        let slot = self.by_native.get(native).copied().ok_or_else(|| {
            HarnessError::Protocol(format!("native thread {native} is not bound"))
        })?;
        if self.by_thread.get(&thread) != Some(&slot) {
            return Err(HarnessError::Protocol(format!(
                "native thread {native} is not bound to thread {thread}"
            )));
        }
        Ok(slot)
    }

    fn slot(&self, id: SlotId) -> Result<&RouteSlot, HarnessError> {
        self.slots.get(&id).ok_or_else(stale_error)
    }

    fn slot_mut(&mut self, id: SlotId) -> Result<&mut RouteSlot, HarnessError> {
        self.slots.get_mut(&id).ok_or_else(stale_error)
    }

    fn allocate_slot(&mut self) -> Result<SlotId, HarnessError> {
        let id = self.next_slot;
        self.next_slot = checked_next(id, "route slot")?;
        Ok(SlotId(id))
    }

    fn allocate_activation(&mut self) -> Result<ActivationKey, HarnessError> {
        let id = self.next_activation;
        self.next_activation = checked_next(id, "route activation")?;
        Ok(ActivationKey(id))
    }

    fn allocate_claim(&mut self) -> Result<ClaimKey, HarnessError> {
        let id = self.next_claim;
        self.next_claim = checked_next(id, "route claim")?;
        Ok(ClaimKey(id))
    }
}

fn make_ticket(
    authority: &Arc<Mutex<RouteTable>>,
    slot: SlotId,
    activation: ActivationKey,
    claim: ClaimKey,
    thread: ThreadId,
    native: NativeThreadId,
) -> DiscoveryTicket {
    let for_claim = Arc::downgrade(authority);
    let for_defer = Arc::downgrade(authority);
    let for_drop = Arc::downgrade(authority);
    DiscoveryTicket::from_route_with_defer(
        thread,
        native.into_inner(),
        move |workspace| claim_ticket(&for_claim, slot, activation, claim, workspace),
        move || defer_ticket(&for_defer, slot, activation, claim),
        move || return_ticket(&for_drop, slot, activation, claim),
    )
}

fn defer_ticket(
    authority: &Weak<Mutex<RouteTable>>,
    slot: SlotId,
    activation: ActivationKey,
    claim: ClaimKey,
) -> Result<(), HarnessError> {
    let authority = authority.upgrade().ok_or_else(stale_error)?;
    let mut table = lock_table(&authority)?;
    let route = table.slot_mut(slot)?;
    let SlotState::Active(active) = &mut route.state else {
        return Err(stale_error());
    };
    if active.activation != activation {
        return Err(stale_error());
    }
    let Custody::Unattached {
        discovery: DiscoveryState::Queued(current),
        ..
    } = &mut active.custody
    else {
        return Err(stale_error());
    };
    if *current != claim {
        return Err(stale_error());
    }
    active.custody.set_discovery(DiscoveryState::Pending);
    Ok(())
}

fn claim_ticket(
    authority: &Weak<Mutex<RouteTable>>,
    slot_id: SlotId,
    activation: ActivationKey,
    claim: ClaimKey,
    workspace: PathBuf,
) -> Result<ThreadAttachment, HarnessError> {
    let authority = authority.upgrade().ok_or_else(stale_error)?;
    let mut table = lock_table(&authority)?;
    let slot = table.slot_mut(slot_id)?;
    let SlotState::Active(active) = &mut slot.state else {
        return Err(stale_error());
    };
    if active.activation != activation {
        return Err(stale_error());
    }
    let Custody::Unattached {
        receiver,
        discovery: DiscoveryState::Queued(current),
    } = &mut active.custody
    else {
        return Err(stale_error());
    };
    if *current != claim {
        return Err(stale_error());
    }
    let stream = std::mem::replace(receiver, closed_stream());
    active.custody = Custody::Attaching(claim);
    let handle = ThreadHandle::opened(slot.thread, slot.native.as_str().to_owned(), workspace);
    Ok(make_attachment(
        &authority, slot_id, activation, claim, handle, stream,
    ))
}

fn return_ticket(
    authority: &Weak<Mutex<RouteTable>>,
    slot: SlotId,
    activation: ActivationKey,
    claim: ClaimKey,
) {
    let Some(authority) = authority.upgrade() else {
        return;
    };
    let Ok(mut table) = lock_table(&authority) else {
        return;
    };
    let Ok(slot) = table.slot_mut(slot) else {
        return;
    };
    let SlotState::Active(active) = &mut slot.state else {
        return;
    };
    if active.activation == activation
        && let Custody::Unattached {
            discovery: DiscoveryState::Queued(current),
            ..
        } = &mut active.custody
        && *current == claim
    {
        active.custody.set_discovery(DiscoveryState::Idle);
    }
}

fn make_attachment(
    authority: &Arc<Mutex<RouteTable>>,
    slot: SlotId,
    activation: ActivationKey,
    claim: ClaimKey,
    handle: ThreadHandle,
    stream: AgentEventStream,
) -> ThreadAttachment {
    let for_commit = Arc::downgrade(authority);
    let for_drop = Arc::downgrade(authority);
    ThreadAttachment::from_route(
        handle,
        stream,
        move || commit_attachment(&for_commit, slot, activation, claim),
        move |stream| return_stream(&for_drop, slot, activation, claim, false, stream),
    )
}

fn commit_attachment(
    authority: &Weak<Mutex<RouteTable>>,
    slot_id: SlotId,
    activation: ActivationKey,
    claim: ClaimKey,
) -> Result<Box<dyn FnOnce(AgentEventStream) + Send + 'static>, HarnessError> {
    let Some(authority) = authority.upgrade() else {
        return Err(stale_error());
    };
    let mut table = lock_table(&authority)?;
    let Ok(slot) = table.slot_mut(slot_id) else {
        return Err(stale_error());
    };
    let SlotState::Active(active) = &mut slot.state else {
        return Err(stale_error());
    };
    if active.activation != activation
        || !matches!(active.custody, Custody::Attaching(current) if current == claim)
    {
        return Err(stale_error());
    }
    active.custody = Custody::Owned(claim);
    drop(table);
    let for_drop = Arc::downgrade(&authority);
    Ok(Box::new(move |stream| {
        return_stream(&for_drop, slot_id, activation, claim, true, stream)
    }))
}

fn return_stream(
    authority: &Weak<Mutex<RouteTable>>,
    slot: SlotId,
    activation: ActivationKey,
    claim: ClaimKey,
    owned: bool,
    stream: AgentEventStream,
) {
    let Some(authority) = authority.upgrade() else {
        return;
    };
    let Ok(mut table) = lock_table(&authority) else {
        return;
    };
    let Ok(slot) = table.slot_mut(slot) else {
        return;
    };
    let SlotState::Active(active) = &mut slot.state else {
        return;
    };
    let exact = if owned {
        matches!(active.custody, Custody::Owned(current) if current == claim)
    } else {
        matches!(active.custody, Custody::Attaching(current) if current == claim)
    };
    if active.activation == activation && exact {
        active.custody = Custody::Unattached {
            receiver: stream,
            discovery: DiscoveryState::Idle,
        };
    }
}

impl Custody {
    fn set_discovery(&mut self, state: DiscoveryState) {
        if let Self::Unattached { discovery, .. } = self {
            *discovery = state;
        }
    }
}

fn normalized_native(value: String) -> Result<NativeThreadId, HarnessError> {
    let value = value.trim();
    if value.is_empty() {
        Err(HarnessError::Protocol(
            "cannot establish an empty native thread id".into(),
        ))
    } else {
        Ok(NativeThreadId::new(value.to_owned()))
    }
}

fn checked_next(value: u64, name: &str) -> Result<u64, HarnessError> {
    value
        .checked_add(1)
        .ok_or_else(|| HarnessError::Protocol(format!("{name} id space exhausted")))
}

fn binding_error(thread: ThreadId) -> HarnessError {
    HarnessError::Protocol(format!(
        "thread {thread} is already bound to another native thread"
    ))
}

fn stale_error() -> HarnessError {
    HarnessError::Protocol("route capability is stale".into())
}

fn closed_stream() -> AgentEventStream {
    let (sender, receiver) = broadcast::channel(1);
    drop(sender);
    AgentEventStream::new(receiver)
}

fn lock_table(table: &Mutex<RouteTable>) -> Result<MutexGuard<'_, RouteTable>, HarnessError> {
    match table.lock() {
        Ok(table) => Ok(table),
        Err(poisoned) => {
            let mut table = poisoned.into_inner();
            table.closed = true;
            table.by_native.clear();
            table.by_thread.clear();
            table.slots.clear();
            Err(HarnessError::Transport(
                "Codex route authority lock poisoned; authority closed".into(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handle(thread: ThreadId, native: &str) -> ThreadHandle {
        ThreadHandle::opened(thread, native.into(), PathBuf::from("/workspace"))
    }

    fn event(thread: ThreadId) -> AgentEvent {
        AgentEvent::Notice {
            thread,
            turn: None,
            message: "test".into(),
        }
    }

    #[test]
    fn convergence_is_bijective_and_authoritative() {
        let routes = CodexRouteAuthority::default();
        let first = ThreadId::new();
        let proposal = ThreadId::new();
        let other = ThreadId::new();
        let route = routes.bootstrap("native-a".into(), first).unwrap();
        assert_eq!(
            routes.bootstrap("native-a".into(), proposal).unwrap(),
            route
        );
        routes.bootstrap("native-b".into(), other).unwrap();
        assert!(routes.bootstrap("native-a".into(), other).is_err());
    }

    #[test]
    fn fresh_claim_classifies_native_and_reverse_collisions_without_mutation() {
        let routes = CodexRouteAuthority::default();
        let first = ThreadId::new();
        let second = ThreadId::new();
        routes.bootstrap("native-a".into(), first).unwrap();

        assert!(matches!(
            routes
                .claim_fresh("native-a".into(), second, |id| handle(id, "native-a"))
                .unwrap(),
            Err(FreshRouteConflict::AuthoritativeNative { thread }) if thread == first
        ));
        assert!(matches!(
            routes
                .claim_fresh("native-new".into(), first, |id| handle(id, "native-new"))
                .unwrap(),
            Err(FreshRouteConflict::NewNativeForBoundThread { .. })
        ));
        assert!(routes.resolve("native-new", first).is_err());
        assert!(routes.active_for_thread(first).is_ok());
    }

    #[test]
    fn ticket_drop_restores_discovery_eligibility() {
        let routes = CodexRouteAuthority::default();
        let thread = ThreadId::new();
        let ticket = routes
            .discover("native-a".into(), thread)
            .unwrap()
            .ticket
            .unwrap();
        assert!(
            routes
                .discover("native-a".into(), thread)
                .unwrap()
                .ticket
                .is_none()
        );
        drop(ticket);
        assert!(
            routes
                .discover("native-a".into(), thread)
                .unwrap()
                .ticket
                .is_some()
        );
    }

    #[test]
    fn bounded_deferral_has_one_pending_ticket_and_parent_supersedes_it() {
        let routes = CodexRouteAuthority::default();
        let thread = ThreadId::new();
        let ticket = routes
            .discover("native-a".into(), thread)
            .unwrap()
            .ticket
            .unwrap();
        ticket.defer().unwrap();
        assert!(
            routes
                .discover("native-a".into(), thread)
                .unwrap()
                .ticket
                .is_none()
        );
        let queued = routes.take_pending_discovery().unwrap().unwrap();
        let attachment = routes
            .claim_parent("native-a".into(), thread, |id| handle(id, "native-a"))
            .unwrap();
        assert!(queued.claim(PathBuf::from("/workspace")).is_err());
        drop(attachment);
        assert!(
            routes
                .discover("native-a".into(), thread)
                .unwrap()
                .ticket
                .is_some()
        );
    }

    #[tokio::test]
    async fn attachment_drop_preserves_the_exact_buffered_receiver() {
        let routes = CodexRouteAuthority::default();
        let thread = ThreadId::new();
        let route = routes.bootstrap("native-a".into(), thread).unwrap();
        let attachment = routes
            .claim_parent("native-a".into(), thread, |id| handle(id, "native-a"))
            .unwrap();
        routes.deliver(&route, event(thread)).unwrap();
        drop(attachment);
        let mut owner = routes
            .claim_parent("native-a".into(), thread, |id| handle(id, "native-a"))
            .unwrap()
            .commit()
            .unwrap();
        assert!(matches!(owner.recv().await, Ok(AgentEvent::Notice { .. })));
    }

    #[tokio::test]
    async fn exact_replacement_preserves_unattached_buffered_delivery() {
        let routes = CodexRouteAuthority::default();
        let thread = ThreadId::new();
        let old = routes.bootstrap("native-old".into(), thread).unwrap();
        routes.deliver(&old, event(thread)).unwrap();
        let mut owner = routes
            .replace("native-old".into(), "native-new".into(), thread, |id| {
                handle(id, "native-new")
            })
            .unwrap()
            .commit()
            .unwrap();
        assert!(matches!(owner.recv().await, Ok(AgentEvent::Notice { .. })));
        assert!(routes.resolve("native-old", thread).is_err());
    }

    #[test]
    fn tombstone_invalidates_capabilities_and_only_exact_reattach_reactivates() {
        let routes = CodexRouteAuthority::default();
        let thread = ThreadId::new();
        let route = routes.bootstrap("native-a".into(), thread).unwrap();
        let attachment = routes
            .claim_parent("native-a".into(), thread, |id| handle(id, "native-a"))
            .unwrap();
        assert!(routes.tombstone("native-a", ThreadId::new()).is_err());
        assert!(
            routes.deliver(&route, event(thread)).is_ok(),
            "identity mismatch must leave the active route and provider delivery untouched"
        );
        routes.tombstone("native-a", thread).unwrap();
        drop(attachment);
        assert!(routes.deliver(&route, event(thread)).is_err());
        assert!(
            routes
                .claim_parent("native-a".into(), thread, |id| handle(id, "native-a"))
                .is_err()
        );
        assert!(
            routes
                .reattach("native-a".into(), ThreadId::new(), |id| handle(
                    id, "native-a"
                ))
                .is_err()
        );
        assert!(
            routes
                .reattach("native-a".into(), thread, |id| handle(id, "native-a"))
                .is_ok()
        );
    }

    #[tokio::test]
    async fn owner_drop_returns_exact_receiver_without_creating_a_subscriber() {
        let routes = CodexRouteAuthority::default();
        let thread = ThreadId::new();
        let route = routes.bootstrap("native-a".into(), thread).unwrap();
        let owner = routes
            .claim_parent("native-a".into(), thread, |id| handle(id, "native-a"))
            .unwrap()
            .commit()
            .unwrap();
        assert!(
            routes
                .claim_parent("native-a".into(), thread, |id| handle(id, "native-a"))
                .is_err()
        );
        routes.deliver(&route, event(thread)).unwrap();
        drop(owner);
        let mut owner = routes
            .claim_parent("native-a".into(), thread, |id| handle(id, "native-a"))
            .unwrap()
            .commit()
            .unwrap();
        assert!(matches!(owner.recv().await, Ok(AgentEvent::Notice { .. })));
        routes.close();
        drop(owner);
        assert!(
            routes
                .bootstrap("native-b".into(), ThreadId::new())
                .is_err()
        );
    }

    #[test]
    fn stale_ticket_cannot_mutate_resume_fallback_replacement() {
        let routes = CodexRouteAuthority::default();
        let thread = ThreadId::new();
        let ticket = routes
            .discover("native-old".into(), thread)
            .unwrap()
            .ticket
            .unwrap();
        let attachment = routes
            .replace("native-old".into(), "native-new".into(), thread, |id| {
                handle(id, "native-new")
            })
            .unwrap();

        drop(ticket);
        let owner = attachment.commit().unwrap();
        assert!(routes.resolve("native-old", thread).is_err());
        assert!(routes.resolve("native-new", thread).is_ok());
        drop(owner);
        assert!(
            routes
                .claim_parent("native-new".into(), thread, |id| handle(id, "native-new"))
                .is_ok()
        );
    }

    #[test]
    fn resume_fallback_never_cleans_up_an_authoritative_native_route() {
        let routes = CodexRouteAuthority::default();
        let expected_thread = ThreadId::new();
        let authoritative_thread = ThreadId::new();
        routes
            .bootstrap("native-old".into(), expected_thread)
            .unwrap();
        routes
            .bootstrap("native-authoritative".into(), authoritative_thread)
            .unwrap();

        assert!(matches!(
            routes.replace_fresh(
                "native-old".into(),
                "native-authoritative".into(),
                expected_thread,
                |id| handle(id, "native-authoritative")
            ),
            Err(ReplaceRouteFailure::AuthoritativeNative { thread })
                if thread == authoritative_thread
        ));
        assert!(routes.resolve("native-old", expected_thread).is_ok());
        assert!(
            routes
                .resolve("native-authoritative", authoritative_thread)
                .is_ok()
        );
    }

    #[test]
    fn stale_attachment_and_owner_cannot_reopen_reactivated_tombstone() {
        let routes = CodexRouteAuthority::default();
        let thread = ThreadId::new();
        routes.bootstrap("native-a".into(), thread).unwrap();
        let stale_attachment = routes
            .claim_parent("native-a".into(), thread, |id| handle(id, "native-a"))
            .unwrap();
        routes.tombstone("native-a", thread).unwrap();
        let current_owner = routes
            .reattach("native-a".into(), thread, |id| handle(id, "native-a"))
            .unwrap()
            .commit()
            .unwrap();

        drop(stale_attachment);
        assert!(
            routes
                .claim_parent("native-a".into(), thread, |id| handle(id, "native-a"))
                .is_err()
        );
        routes.tombstone("native-a", thread).unwrap();
        let replacement = routes
            .reattach("native-a".into(), thread, |id| handle(id, "native-a"))
            .unwrap();
        drop(current_owner);
        assert!(replacement.commit().is_ok());
    }

    #[test]
    fn repeated_tombstone_does_not_reactivate_delivery() {
        let routes = CodexRouteAuthority::default();
        let thread = ThreadId::new();
        routes.bootstrap("native-a".into(), thread).unwrap();
        routes.tombstone("native-a", thread).unwrap();
        routes.tombstone("native-a", thread).unwrap();

        assert!(routes.resolve("native-a", thread).is_err());
        assert!(
            routes
                .claim_parent("native-a".into(), thread, |id| handle(id, "native-a"))
                .is_err()
        );
    }

    #[test]
    fn attaching_and_owned_routes_do_not_duplicate_discovery() {
        let routes = CodexRouteAuthority::default();
        let thread = ThreadId::new();
        let first = routes
            .discover("native-a".into(), thread)
            .unwrap()
            .ticket
            .unwrap();
        let attachment = first.claim(PathBuf::from("/workspace")).unwrap();

        assert!(
            routes
                .discover("native-a".into(), ThreadId::new())
                .unwrap()
                .ticket
                .is_none(),
            "an attaching route must not admit another discovery"
        );
        let owner = attachment.commit().unwrap();
        assert!(
            routes
                .discover("native-a".into(), ThreadId::new())
                .unwrap()
                .ticket
                .is_none(),
            "an owned route must not admit another discovery"
        );
        drop(owner);
    }

    #[tokio::test]
    async fn attaching_and_owned_routes_continue_delivering_to_the_exact_receiver() {
        let routes = CodexRouteAuthority::default();
        let thread = ThreadId::new();
        let discovery = routes.discover("native-live".into(), thread).unwrap();
        let route = discovery.route;
        let attachment = discovery
            .ticket
            .unwrap()
            .claim(PathBuf::from("/workspace"))
            .unwrap();
        routes.deliver(&route, event(thread)).unwrap();
        let mut owner = attachment.commit().unwrap();
        assert!(matches!(owner.recv().await, Ok(AgentEvent::Notice { .. })));

        routes.deliver(&route, event(thread)).unwrap();
        assert!(matches!(owner.recv().await, Ok(AgentEvent::Notice { .. })));
    }

    #[test]
    fn physically_retained_persistence_blocked_owner_suppresses_discovery() {
        let routes = CodexRouteAuthority::default();
        let thread = ThreadId::new();
        routes.bootstrap("native-blocked".into(), thread).unwrap();
        let persistence_blocked_owner = Box::new(
            routes
                .claim_parent("native-blocked".into(), thread, |id| {
                    handle(id, "native-blocked")
                })
                .unwrap()
                .commit()
                .unwrap(),
        );

        assert!(
            routes
                .discover("native-blocked".into(), ThreadId::new())
                .unwrap()
                .ticket
                .is_none()
        );
        drop(persistence_blocked_owner);
        assert!(
            routes
                .discover("native-blocked".into(), ThreadId::new())
                .unwrap()
                .ticket
                .is_some()
        );
    }

    #[test]
    fn every_late_capability_kind_is_inert_after_close() {
        let routes = CodexRouteAuthority::default();
        let ticket = routes
            .discover("native-ticket".into(), ThreadId::new())
            .unwrap()
            .ticket
            .unwrap();
        let attaching_thread = ThreadId::new();
        let attachment = routes
            .claim_parent("native-attachment".into(), attaching_thread, |id| {
                handle(id, "native-attachment")
            })
            .unwrap();
        let owned_thread = ThreadId::new();
        let owner = routes
            .claim_parent("native-owner".into(), owned_thread, |id| {
                handle(id, "native-owner")
            })
            .unwrap()
            .commit()
            .unwrap();

        routes.close();
        drop(ticket);
        drop(attachment);
        drop(owner);

        assert!(
            routes
                .bootstrap("new-native".into(), ThreadId::new())
                .is_err()
        );
        assert!(routes.resolve("native-ticket", ThreadId::new()).is_err());
        assert!(
            routes
                .reattach("native-attachment".into(), attaching_thread, |id| {
                    handle(id, "native-attachment")
                })
                .is_err()
        );
        assert!(
            routes
                .reattach("native-owner".into(), owned_thread, |id| handle(
                    id,
                    "native-owner"
                ))
                .is_err()
        );
    }

    #[test]
    fn conflicting_bootstrap_does_not_partially_mutate_existing_bindings() {
        let routes = CodexRouteAuthority::default();
        let first = ThreadId::new();
        let second = ThreadId::new();
        routes.bootstrap("native-a".into(), first).unwrap();
        routes.bootstrap("native-b".into(), second).unwrap();

        assert!(routes.bootstrap("native-a".into(), second).is_err());
        assert_eq!(
            routes.resolve("native-a", first).unwrap().thread_id(),
            first
        );
        assert_eq!(
            routes.resolve("native-b", second).unwrap().thread_id(),
            second
        );
    }

    #[test]
    fn empty_native_id_resolves_only_the_explicit_active_fallback() {
        let routes = CodexRouteAuthority::default();
        let fallback = ThreadId::new();
        let other = ThreadId::new();
        routes
            .bootstrap("native-fallback".into(), fallback)
            .unwrap();
        routes.bootstrap("native-other".into(), other).unwrap();

        assert_eq!(routes.resolve("", fallback).unwrap().thread_id(), fallback);
        assert_eq!(routes.resolve("   ", other).unwrap().thread_id(), other);
        assert!(routes.resolve("", ThreadId::new()).is_err());
    }

    #[test]
    fn poisoned_authority_closes_all_routes_and_returns_fatal_error() {
        let routes = CodexRouteAuthority::default();
        routes
            .bootstrap("native-a".into(), ThreadId::new())
            .unwrap();
        let authority = routes.inner.clone();
        let poisoner = std::thread::spawn(move || {
            let _guard = authority.lock().unwrap();
            panic!("poison route authority");
        });
        assert!(poisoner.join().is_err());

        let error = routes
            .bootstrap("native-b".into(), ThreadId::new())
            .unwrap_err();
        assert!(matches!(error, HarnessError::Transport(_)), "{error}");
        assert!(error.to_string().contains("authority closed"), "{error}");
        let table = match routes.inner.lock() {
            Ok(_) => panic!("route authority should remain poisoned"),
            Err(poisoned) => poisoned.into_inner(),
        };
        assert!(table.closed);
        assert!(table.by_native.is_empty());
        assert!(table.by_thread.is_empty());
        assert!(table.slots.is_empty());
    }
}
