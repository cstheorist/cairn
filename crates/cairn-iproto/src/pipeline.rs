//! Optional bounded IPROTO multiplexing. The shared waiter/buffer architecture
//! follows Tarn's MIT-licensed client (see NOTICE). Cairn additionally retires
//! unknown/nonfinal replies and retains cancelled ownership until exact drain.
use super::*;
use std::{collections::HashMap, sync::Arc};
use tokio::{io::AsyncRead, sync::Notify, task::AbortHandle, time::Instant};

const MAX_REQUESTS: usize = 128;
// Encoded request bytes, including framing, charged until terminal response.
// This is an admission bound, not a total-process/decoded-response memory bound.
const MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;

struct Pending {
    reply: oneshot::Sender<Result<Vec<Value>>>,
    bytes: usize,
    deadline: Instant,
}

struct State {
    closed: bool,
    sync: u64,
    pending: HashMap<u64, Pending>,
    bytes: usize,
    write_buffer: Vec<u8>,
    queued: usize,
}

struct Shared {
    state: std::sync::Mutex<State>,
    write_ready: Notify,
    deadline_changed: Notify,
    tasks: std::sync::Mutex<Vec<AbortHandle>>,
    generation: Arc<AtomicU64>,
    saved_generation: u64,
}

impl Shared {
    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn retire(&self, reason: &str, matched_fence: Option<u64>) {
        let (pending, matched) = {
            let mut state = self.state();
            if state.closed {
                return;
            }
            state.closed = true;
            let matched = matched_fence.and_then(|sync| state.pending.remove(&sync));
            state.bytes = 0;
            state.write_buffer = Vec::new();
            state.queued = 0;
            // An old reader must never invalidate a newly repaired generation.
            let _ = self.generation.compare_exchange(
                self.saved_generation,
                self.saved_generation.saturating_add(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            (std::mem::take(&mut state.pending), matched)
        };
        if let Some(pending) = matched {
            let _ = pending.reply.send(Err(DbError::new("57P03", reason)));
        }
        for pending in pending.into_values() {
            let _ = pending.reply.send(Err(transport(reason)));
        }
        self.write_ready.notify_one();
        self.deadline_changed.notify_one();
        for task in self.tasks.lock().unwrap_or_else(|error| error.into_inner()).drain(..) {
            task.abort();
        }
    }
}

struct Inner {
    shared: Arc<Shared>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.shared.retire("pipeline owner dropped", None);
    }
}

#[derive(Clone)]
pub(super) struct Pipeline {
    inner: Arc<Inner>,
}

impl Pipeline {
    pub(super) fn start(connection: Connection, generation: Arc<AtomicU64>, saved_generation: u64) -> Self {
        // Authentication used the ordinary strict request path. Preserve any
        // bytes prefetched beyond its response when splitting the socket.
        let buffered = connection.socket.buffer().to_vec();
        let socket = connection.socket.into_inner();
        let (reader, writer) = socket.into_split();
        let reader = tokio::io::BufReader::with_capacity(16 * 1024, Cursor::new(buffered).chain(reader));
        let shared = Arc::new(Shared {
            state: std::sync::Mutex::new(State {
                closed: false,
                sync: connection.sync,
                pending: HashMap::new(),
                bytes: 0,
                write_buffer: Vec::new(),
                queued: 0,
            }),
            write_ready: Notify::new(),
            deadline_changed: Notify::new(),
            tasks: std::sync::Mutex::new(Vec::new()),
            generation,
            saved_generation,
        });
        // Workers cannot observe state before their abort handles are installed.
        let state = shared.state();
        let workers = [
            tokio::spawn(read_responses(shared.clone(), reader)).abort_handle(),
            tokio::spawn(write_requests(shared.clone(), writer)).abort_handle(),
            tokio::spawn(watch_deadlines(shared.clone())).abort_handle(),
        ];
        shared
            .tasks
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .extend(workers);
        drop(state);
        Self {
            inner: Arc::new(Inner { shared }),
        }
    }

    pub(super) fn usable(&self) -> bool {
        let shared = &self.inner.shared;
        if shared.generation.load(Ordering::Acquire) != shared.saved_generation {
            shared.retire("pipeline generation changed", None);
        }
        !shared.state().closed
    }

    fn enqueue(
        &self,
        function: &str,
        arguments: Vec<Value>,
        deadline: Instant,
    ) -> Result<oneshot::Receiver<Result<Vec<Value>>>> {
        let shared = &self.inner.shared;
        let mut state = shared.state();
        if state.closed
            || shared.generation.load(Ordering::Acquire) != shared.saved_generation
            || Instant::now() >= deadline
        {
            drop(state);
            shared.retire("pipeline unavailable or deadline elapsed", None);
            return Err(transport("pipeline unavailable or deadline elapsed"));
        }
        if state.pending.len() >= MAX_REQUESTS {
            return Err(DbError::new(
                "54000",
                "storage pipeline exceeds 128 outstanding requests",
            ));
        }
        let Some(sync) = state.sync.checked_add(1) else {
            drop(state);
            shared.retire("pipeline sync overflow", None);
            return Err(transport("pipeline sync overflow"));
        };
        let header = Value::Map(vec![(0.into(), 10.into()), (1.into(), sync.into())]);
        let body = Value::Map(vec![
            (0x22.into(), function.into()),
            (0x21.into(), Value::Array(arguments)),
        ]);
        let mut packet = vec![0xce, 0, 0, 0, 0];
        rmpv::encode::write_value(&mut packet, &header).map_err(transport)?;
        rmpv::encode::write_value(&mut packet, &body).map_err(transport)?;
        if packet.len() - 5 > MAX_PACKET as usize {
            return Err(DbError::new("54000", "storage request exceeds 8 MiB"));
        }
        if Instant::now() >= deadline {
            drop(state);
            shared.retire("request deadline elapsed during encoding", None);
            return Err(transport("request deadline elapsed during encoding"));
        }
        let length = packet.len() - 5;
        packet[1..5].copy_from_slice(&(length as u32).to_be_bytes());
        let bytes = state
            .bytes
            .checked_add(packet.len())
            .filter(|bytes| *bytes <= MAX_REQUEST_BYTES)
            .ok_or_else(|| DbError::new("54000", "storage pipeline exceeds 16 MiB of outstanding request bytes"))?;
        let (reply, response) = oneshot::channel();
        let wake = state.write_buffer.is_empty();
        state.sync = sync;
        state.bytes = bytes;
        state.pending.insert(
            sync,
            Pending {
                reply,
                bytes: packet.len(),
                deadline,
            },
        );
        state.write_buffer.extend_from_slice(&packet);
        state.queued += 1;
        drop(state);
        if wake {
            shared.write_ready.notify_one();
        }
        shared.deadline_changed.notify_one();
        Ok(response)
    }

    pub(super) async fn call(&self, function: &str, arguments: Vec<Value>, deadline: Instant) -> Result<Vec<Value>> {
        // Dropping this receiver abandons ownership, not the SYNC entry. The
        // reader still drains its reply, and the watchdog still owns its deadline.
        self.enqueue(function, arguments, deadline)?
            .await
            .map_err(|_| transport("pipeline worker stopped"))?
    }
}

async fn read_packet(reader: &mut (impl AsyncRead + Unpin)) -> Result<Vec<u8>> {
    let marker = reader.read_u8().await.map_err(transport)?;
    let length = match marker {
        0..=0x7f => u64::from(marker),
        0xcc => u64::from(reader.read_u8().await.map_err(transport)?),
        0xcd => u64::from(reader.read_u16().await.map_err(transport)?),
        0xce => u64::from(reader.read_u32().await.map_err(transport)?),
        0xcf => reader.read_u64().await.map_err(transport)?,
        _ => return Err(transport("invalid MessagePack packet length")),
    };
    if length == 0 || length > MAX_PACKET {
        return Err(transport("storage response exceeds packet limit"));
    }
    let mut packet = vec![0; length as usize];
    reader.read_exact(&mut packet).await.map_err(transport)?;
    Ok(packet)
}

async fn read_responses(shared: Arc<Shared>, mut reader: impl AsyncRead + Unpin) {
    loop {
        if shared.state().closed {
            return;
        }
        // This read is never selected against queue arrivals. Only retiring the
        // entire stream may abort a partially consumed prefix or body.
        let parts = match read_packet(&mut reader)
            .await
            .and_then(|packet| response_parts(&packet))
        {
            Ok(parts) => parts,
            Err(error) => {
                shared.retire(&error.message, None);
                return;
            }
        };
        let sync = match response_field(&parts.0, 1) {
            Ok(sync) => sync,
            Err(error) => {
                shared.retire(&error.message, None);
                return;
            }
        };
        let mut state = shared.state();
        if state.closed {
            return;
        }
        let Some(pending) = state.pending.get(&sync) else {
            drop(state);
            shared.retire("unknown or duplicate response sync", None);
            return;
        };
        if shared.generation.load(Ordering::Acquire) != shared.saved_generation || Instant::now() >= pending.deadline {
            drop(state);
            shared.retire("pipeline generation changed or response deadline elapsed", None);
            return;
        }
        // Classification happens only after exact ownership has been found.
        let result = decode_response_parts(&parts.0, parts.1, sync).and_then(|value| match value {
            Value::Array(values) => Ok(values),
            _ => Err(transport("CALL response is not an array")),
        });
        if let Err(error) = &result {
            if matches!(error.code, "08006" | "57P03") {
                drop(state);
                shared.retire(&error.message, (error.code == "57P03").then_some(sync));
                return;
            }
        }
        let pending = state.pending.remove(&sync).unwrap();
        state.bytes -= pending.bytes;
        drop(state);
        let _ = pending.reply.send(result);
        shared.deadline_changed.notify_one();
    }
}

async fn write_requests(shared: Arc<Shared>, mut writer: impl tokio::io::AsyncWrite + Unpin) {
    let mut buffer = Vec::new();
    loop {
        shared.write_ready.notified().await;
        let coalesce = {
            let state = shared.state();
            if state.closed {
                return;
            }
            state.pending.len() > state.queued
        };
        if coalesce {
            tokio::task::yield_now().await;
        }
        {
            let mut state = shared.state();
            if state.closed {
                return;
            }
            if shared.generation.load(Ordering::Acquire) != shared.saved_generation {
                drop(state);
                shared.retire("pipeline generation changed before write", None);
                return;
            }
            if state.pending.values().any(|pending| Instant::now() >= pending.deadline) {
                drop(state);
                shared.retire("request deadline elapsed before write", None);
                return;
            }
            std::mem::swap(&mut buffer, &mut state.write_buffer);
            state.queued = 0;
        }
        if buffer.is_empty() {
            continue;
        }
        if let Err(error) = writer.write_all(&buffer).await {
            shared.retire(&error.to_string(), None);
            return;
        }
        buffer.clear();
    }
}

async fn watch_deadlines(shared: Arc<Shared>) {
    loop {
        let notified = shared.deadline_changed.notified();
        let deadline = {
            let state = shared.state();
            if state.closed {
                return;
            }
            state.pending.values().map(|pending| pending.deadline).min()
        };
        if let Some(deadline) = deadline {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline) => {
                    let expired = shared.state().pending.values().any(|pending| Instant::now() >= pending.deadline);
                    if expired { shared.retire("5-second request deadline elapsed", None); return; }
                }
                _ = notified => {}
            }
        } else {
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests;
