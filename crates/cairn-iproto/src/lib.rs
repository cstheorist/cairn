use base64::{engine::general_purpose::STANDARD, Engine as _};
pub use cairn_common::{Column, Schema};
use cairn_common::{DbError, Result};
use cairn_verified as core;
use rmpv::Value;
use serde::Serialize;
use sha1::{Digest, Sha1};
use std::{
    collections::VecDeque,
    io::Cursor,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{mpsc, oneshot, Mutex, OnceCell},
};

const MAX_PACKET: u64 = 8 * 1024 * 1024;
const TIMEOUT: Duration = Duration::from_secs(5);
const MAX_READ_BATCH: usize = 64;

struct PointRead {
    arguments: Vec<Value>,
    reply: oneshot::Sender<Result<Vec<Value>>>,
}

fn transport(error: impl std::fmt::Display) -> DbError {
    DbError::new(
        "08006",
        format!("Tarantool transport failed ({error}); a mutation's outcome may be unknown; not retried"),
    )
}

fn field(value: &Value, key: u64) -> Option<&Value> {
    value
        .as_map()?
        .iter()
        .find(|(k, _)| k.as_u64() == Some(key))
        .map(|(_, v)| v)
}

struct Connection {
    socket: tokio::io::BufReader<TcpStream>,
    sync: u64,
}

impl Connection {
    async fn connect(address: &str, user: &str, password: &str) -> Result<Self> {
        let socket = TcpStream::connect(address).await.map_err(transport)?;
        socket.set_nodelay(true).map_err(transport)?;
        let mut connection = Self {
            socket: tokio::io::BufReader::with_capacity(16 * 1024, socket),
            sync: 0,
        };
        let mut greeting = [0u8; 128];
        connection.socket.read_exact(&mut greeting).await.map_err(transport)?;
        let encoded = std::str::from_utf8(&greeting[64..108]).map_err(transport)?.trim();
        let salt = STANDARD.decode(encoded).map_err(transport)?;
        if salt.len() < 20 {
            return Err(transport("short authentication salt"));
        }
        let h1 = Sha1::digest(password.as_bytes());
        let h2 = Sha1::digest(h1);
        let mut h3 = Sha1::new();
        h3.update(&salt[..20]);
        h3.update(h2);
        let h3 = h3.finalize();
        let scramble: Vec<u8> = h1.iter().zip(h3.iter()).map(|(a, b)| a ^ b).collect();
        // IPROTO_AUTH expects a MessagePack string, including non-UTF8 bytes.
        let mut encoded_scramble = vec![0xa0 | 20];
        encoded_scramble.extend_from_slice(&scramble);
        let scramble = rmpv::decode::read_value(&mut Cursor::new(encoded_scramble)).map_err(transport)?;
        connection
            .request(
                7,
                Value::Map(vec![
                    (0x23.into(), user.into()),
                    (0x21.into(), Value::Array(vec!["chap-sha1".into(), scramble])),
                ]),
            )
            .await?;
        Ok(connection)
    }

    async fn request(&mut self, kind: u64, body: Value) -> Result<Value> {
        self.sync = self.sync.checked_add(1).ok_or_else(|| transport("sync overflow"))?;
        let header = Value::Map(vec![(0.into(), kind.into()), (1.into(), self.sync.into())]);
        let mut payload = Vec::new();
        rmpv::encode::write_value(&mut payload, &header).map_err(transport)?;
        rmpv::encode::write_value(&mut payload, &body).map_err(transport)?;
        if payload.len() as u64 > MAX_PACKET {
            return Err(DbError::new("54000", "storage request exceeds 8 MiB"));
        }
        let mut packet = Vec::with_capacity(payload.len() + 5);
        packet.push(0xce);
        packet.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        packet.extend_from_slice(&payload);
        self.socket.write_all(&packet).await.map_err(transport)?;
        let marker = self.socket.read_u8().await.map_err(transport)?;
        let length = match marker {
            0..=0x7f => marker as u64,
            0xcc => self.socket.read_u8().await.map_err(transport)? as u64,
            0xcd => self.socket.read_u16().await.map_err(transport)? as u64,
            0xce => self.socket.read_u32().await.map_err(transport)? as u64,
            0xcf => self.socket.read_u64().await.map_err(transport)?,
            _ => return Err(transport("invalid MessagePack packet length")),
        };
        if length == 0 || length > MAX_PACKET {
            return Err(transport("storage response exceeds packet limit"));
        }
        let mut response = vec![0; length as usize];
        self.socket.read_exact(&mut response).await.map_err(transport)?;
        let mut cursor = Cursor::new(response);
        let header = rmpv::decode::read_value(&mut cursor).map_err(transport)?;
        let body = rmpv::decode::read_value(&mut cursor).map_err(transport)?;
        if field(&header, 1).and_then(Value::as_u64) != Some(self.sync) {
            return Err(transport("response sync mismatch"));
        }
        let status = field(&header, 0)
            .and_then(Value::as_u64)
            .ok_or_else(|| transport("missing response status"))?;
        if status != 0 {
            let message = field(&body, 0x31).and_then(Value::as_str).unwrap_or("Tarantool error");
            let code = if message.starts_with("[CAIRN:FENCED]") {
                "57P03"
            } else if message.starts_with("[CAIRN:CONFLICT]")
                || message.starts_with("Transaction has been aborted by conflict")
            {
                "40001"
            } else if message.starts_with("[CAIRN:ABORTED]") {
                "25P02"
            } else if message.starts_with("[CAIRN:LIMIT]") {
                "54000"
            } else if message.starts_with("[CAIRN:RANGE]") {
                "22003"
            } else if message.starts_with("[CAIRN:NOT_NULL]") {
                "23502"
            } else if message.starts_with("[CAIRN:DUPLICATE]") || status & 0x7fff == 3 {
                "23505"
            } else {
                "XX000"
            };
            return Err(DbError::new(code, message));
        }
        Ok(field(&body, 0x30).cloned().unwrap_or(Value::Array(vec![])))
    }

    async fn call(&mut self, function: &str, args: Vec<Value>) -> Result<Vec<Value>> {
        let value = self
            .request(
                10,
                Value::Map(vec![(0x22.into(), function.into()), (0x21.into(), Value::Array(args))]),
            )
            .await?;
        value
            .as_array()
            .cloned()
            .ok_or_else(|| transport("CALL response is not an array"))
    }
}

struct Pool {
    members: Vec<String>,
    leader: Mutex<Option<Leader>>,
    slots: Vec<Mutex<Option<(u64, Connection)>>>,
    cursor: AtomicUsize,
    generation: AtomicU64,
    observed_term: AtomicU64,
    read_queue: OnceCell<mpsc::Sender<PointRead>>,
}

#[derive(Clone)]
struct Leader {
    address: String,
    term: u64,
    incarnation: u64,
}

pub struct Cluster {
    shards: Vec<Pool>,
    user: String,
    password: String,
    profile: Option<std::sync::Mutex<std::collections::BTreeMap<String, RpcCounters>>>,
}

#[derive(Default, Serialize)]
struct RpcCounters {
    samples: u64,
    failures: u64,
    total_ns: u64,
    maximum_ns: u64,
    // Upper bounds in microseconds: 250,500,1000,2000,4000,8000,16000,
    // 32000,64000; the last bucket contains everything above 64000.
    latency_buckets: [u64; 10],
}

impl Cluster {
    pub fn new(addresses: Vec<String>, user: String, password: String) -> Result<Self> {
        let connections = std::env::var("CAIRN_STORAGE_CONNECTIONS")
            .ok()
            .map(|value| value.parse::<usize>())
            .transpose()
            .map_err(|_| DbError::new("22023", "CAIRN_STORAGE_CONNECTIONS must be an integer from 1 to 64"))?
            .unwrap_or(8);
        if !(1..=64).contains(&connections) {
            return Err(DbError::new("22023", "CAIRN_STORAGE_CONNECTIONS must be from 1 to 64"));
        }
        if addresses.is_empty() || addresses.len() > 1024 || addresses.iter().any(|s| s.trim().is_empty()) {
            return Err(DbError::new("22023", "expected 1..1024 nonempty shard addresses"));
        }
        let members: Vec<Vec<String>> = addresses
            .iter()
            .map(|group| group.split('|').map(|address| address.trim().to_owned()).collect())
            .collect();
        if members
            .iter()
            .any(|group| group.len() > 9 || group.iter().any(String::is_empty))
        {
            return Err(DbError::new(
                "22023",
                "each shard requires 1..9 nonempty member addresses, separated by |",
            ));
        }
        let mut unique = members.iter().flatten().cloned().collect::<Vec<_>>();
        let member_count = unique.len();
        unique.sort();
        unique.dedup();
        if unique.len() != member_count {
            return Err(DbError::new("22023", "duplicate shard addresses"));
        }
        Ok(Self {
            shards: members
                .into_iter()
                .map(|members| Pool {
                    members,
                    leader: Mutex::new(None),
                    slots: (0..connections).map(|_| Mutex::new(None)).collect(),
                    cursor: AtomicUsize::new(0),
                    generation: AtomicU64::new(0),
                    observed_term: AtomicU64::new(0),
                    read_queue: OnceCell::new(),
                })
                .collect(),
            user,
            password,
            profile: (std::env::var("CAIRN_PROFILE_RPC").as_deref() == Ok("1"))
                .then(|| std::sync::Mutex::new(std::collections::BTreeMap::new())),
        })
    }

    pub fn owner(&self, key: i64) -> usize {
        // Constructor establishes the verified function's nonzero-count precondition.
        core::shard_for_key(key, self.shards.len() as u64) as usize
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn profiling_enabled(&self) -> bool {
        self.profile.is_some()
    }

    pub fn report_rpc_profile(&self) {
        if let Some(profile) = &self.profile {
            let counters = std::mem::take(&mut *profile.lock().unwrap_or_else(|error| error.into_inner()));
            if !counters.is_empty() {
                if let Ok(encoded) = serde_json::to_string(&counters) {
                    eprintln!("cairn_rpc_profile {encoded}");
                }
            }
        }
    }

    pub async fn read(self: &std::sync::Arc<Self>, shard: usize, arguments: Vec<Value>) -> Result<Vec<Value>> {
        if arguments.len() != 2 {
            return Err(DbError::new("XX000", "point read requires a table and key"));
        }
        let queue = self.shards[shard]
            .read_queue
            .get_or_init(|| async {
                let (sender, mut receiver) = mpsc::channel::<PointRead>(256);
                let weak = std::sync::Arc::downgrade(self);
                tokio::spawn(async move {
                    while let Some(first) = receiver.recv().await {
                        if first.reply.is_closed() {
                            continue;
                        }
                        let mut batch = vec![first];
                        // Collect already pending invocations before starting the
                        // quorum barrier. Arrivals during the RPC stay in the queue.
                        let capture_until = std::time::Instant::now() + Duration::from_micros(50);
                        loop {
                            while batch.len() < MAX_READ_BATCH {
                                match receiver.try_recv() {
                                    Ok(request) if !request.reply.is_closed() => batch.push(request),
                                    Ok(_) => {}
                                    Err(_) => break,
                                }
                            }
                            if batch.len() == MAX_READ_BATCH || std::time::Instant::now() >= capture_until {
                                break;
                            }
                            tokio::task::yield_now().await;
                        }
                        let Some(cluster) = weak.upgrade() else {
                            break;
                        };
                        let mut pieces = VecDeque::from([batch]);
                        while let Some(mut batch) = pieces.pop_front() {
                            batch.retain(|request| !request.reply.is_closed());
                            if batch.is_empty() {
                                continue;
                            }
                            let arguments = Value::Array(
                                batch
                                    .iter()
                                    .map(|request| Value::Array(request.arguments.clone()))
                                    .collect(),
                            );
                            let result =
                                cluster
                                    .call(shard, "cairn.get_many", vec![arguments])
                                    .await
                                    .and_then(|data| {
                                        let rows = data
                                            .into_iter()
                                            .next()
                                            .and_then(|data| match data {
                                                Value::Array(rows) => Some(rows),
                                                _ => None,
                                            })
                                            .ok_or_else(|| DbError::new("XX000", "invalid batched read response"))?;
                                        if !core::valid_read_batch(
                                            batch.len() as u64,
                                            rows.len() as u64,
                                            MAX_READ_BATCH as u64,
                                        ) || !rows.iter().zip(&batch).all(|(row, request)| {
                                            row.as_array().is_some_and(|row| {
                                                row.len() == 3
                                                    && row[0] == request.arguments[0]
                                                    && row[1] == request.arguments[1]
                                                    && matches!(row[2], Value::Array(_))
                                            })
                                        }) {
                                            return Err(DbError::new(
                                                "XX000",
                                                "batched read response does not match its requests",
                                            ));
                                        }
                                        Ok(rows)
                                    });
                            match result {
                                Ok(rows) => {
                                    for (request, row) in batch.into_iter().zip(rows) {
                                        let value = row
                                            .as_array()
                                            .and_then(|row| row.get(2))
                                            .cloned()
                                            .ok_or_else(|| DbError::new("XX000", "invalid read envelope"));
                                        let _ = request.reply.send(value.map(|value| vec![value]));
                                    }
                                }
                                Err(error) => {
                                    if error.code == "54000" && batch.len() > 1 {
                                        let right = batch.split_off(batch.len() / 2);
                                        pieces.push_front(right);
                                        pieces.push_front(batch);
                                        continue;
                                    }
                                    for request in batch {
                                        let _ =
                                            request.reply.send(Err(DbError::new(error.code, error.message.clone())));
                                    }
                                }
                            }
                        }
                    }
                });
                sender
            })
            .await;
        tokio::time::timeout(TIMEOUT, async {
            let (reply, response) = oneshot::channel();
            queue
                .send(PointRead { arguments, reply })
                .await
                .map_err(|_| transport("read queue closed"))?;
            response.await.map_err(|_| transport("read worker stopped"))?
        })
        .await
        .map_err(|_| transport("batched read deadline elapsed"))?
    }

    pub async fn incarnation(&self, shard: usize) -> Result<u64> {
        let incarnation = self.leader(shard).await?.incarnation;
        if incarnation == 0 {
            return Err(DbError::new("55000", "storage does not advertise an incarnation"));
        }
        Ok(incarnation)
    }

    async fn leader(&self, shard: usize) -> Result<Leader> {
        let pool = &self.shards[shard];
        let mut cached = pool.leader.lock().await;
        if let Some(leader) = cached.as_ref() {
            return Ok(leader.clone());
        }
        let mut probes = tokio::task::JoinSet::new();
        for address in &pool.members {
            let address = address.clone();
            let user = self.user.clone();
            let password = self.password.clone();
            probes.spawn(async move {
                let result = tokio::time::timeout(Duration::from_secs(1), async {
                    let mut connection = Connection::connect(&address, &user, &password).await?;
                    connection.call("cairn.ping", vec![]).await
                })
                .await
                .map_err(|_| transport("leader probe timed out"))
                .and_then(|r| r);
                (address, result)
            });
        }
        let mut candidates = Vec::new();
        let mut maximum_term = pool.observed_term.load(Ordering::Acquire);
        while let Some(probe) = probes.join_next().await {
            let (address, result) = probe.map_err(transport)?;
            if let Ok(data) = result {
                let id = data.first().and_then(Value::as_u64).unwrap_or(u64::MAX);
                let count = data.get(1).and_then(Value::as_u64).unwrap_or(0);
                if !core::accepts_topology(id, count, shard as u64, self.shards.len() as u64) {
                    return Err(DbError::new(
                        "55000",
                        format!("member {address} belongs to a different shard topology"),
                    ));
                }
                let term = data.get(2).and_then(Value::as_u64).unwrap_or(0);
                let writable = data.get(3).and_then(Value::as_bool).unwrap_or(false);
                maximum_term = maximum_term.max(term);
                if writable {
                    let incarnation = data.get(7).and_then(Value::as_u64).unwrap_or(0);
                    candidates.push(Leader {
                        address,
                        term,
                        incarnation,
                    });
                }
            }
        }
        pool.observed_term.fetch_max(maximum_term, Ordering::AcqRel);
        let leader = candidates
            .into_iter()
            .filter(|leader| core::accepts_leader(true, leader.term, maximum_term))
            .max_by_key(|leader| leader.term)
            .ok_or_else(|| {
                DbError::new(
                    "08006",
                    format!("no writable leader in shard {shard} at epoch {maximum_term}"),
                )
            })?;
        *cached = Some(leader.clone());
        Ok(leader)
    }

    async fn invalidate(&self, shard: usize) {
        let pool = &self.shards[shard];
        let mut leader = pool.leader.lock().await;
        pool.generation.fetch_add(1, Ordering::AcqRel);
        *leader = None;
    }

    async fn call_once(&self, shard: usize, function: &str, mut args: Vec<Value>) -> Result<Vec<Value>> {
        let pool = &self.shards[shard];
        let leader = self.leader(shard).await?;
        if function != "cairn.ping" {
            args.push(leader.term.into());
        }
        let slot = pool.cursor.fetch_add(1, Ordering::Relaxed) % pool.slots.len();
        let operation = async {
            let mut guard = pool.slots[slot].lock().await;
            let generation = pool.generation.load(Ordering::Acquire);
            // Ownership prevents a cancelled/timed-out request from reusing a desynchronized stream.
            let mut connection = match guard.take() {
                Some((saved_generation, connection)) if saved_generation == generation => connection,
                _ => Connection::connect(&leader.address, &self.user, &self.password).await?,
            };
            let result = connection.call(function, args).await;
            // A fully read, matched storage error leaves the stream aligned.
            // Keep it for known SQL errors; transport failures and fences retire
            // the connection. This does not replay any failed operation.
            if !result
                .as_ref()
                .is_err_and(|error| matches!(error.code, "08006" | "57P03"))
                && pool.generation.load(Ordering::Acquire) == generation
            {
                *guard = Some((generation, connection));
            }
            result
        };
        let result = tokio::time::timeout(TIMEOUT, operation)
            .await
            .map_err(|_| transport("5-second deadline elapsed"))
            .and_then(|result| result);
        if result
            .as_ref()
            .is_err_and(|error| error.code == "08006" || error.code == "57P03")
        {
            // Future requests replace every socket established before this failure.
            // The failed operation itself is never retried, including after timeouts.
            self.invalidate(shard).await;
        }
        result
    }

    pub async fn call(&self, shard: usize, function: &str, args: Vec<Value>) -> Result<Vec<Value>> {
        let started = self.profile.as_ref().map(|_| std::time::Instant::now());
        let mut result = self.call_once(shard, function, args.clone()).await;
        if result.as_ref().is_err_and(|error| error.code == "57P03") {
            // Only this explicit fence is emitted before execution. Unknown transport
            // outcomes are never replayed. Storage rejects an epoch before any mutation.
            result = self.call_once(shard, function, args).await;
        }
        if let (Some(profile), Some(started)) = (&self.profile, started) {
            let elapsed = started.elapsed();
            let nanos = elapsed.as_nanos().min(u64::MAX as u128) as u64;
            let bucket = [250, 500, 1000, 2000, 4000, 8000, 16000, 32000, 64000]
                .iter()
                .position(|upper| elapsed.as_micros() <= *upper)
                .unwrap_or(9);
            let mut counters = profile.lock().unwrap_or_else(|error| error.into_inner());
            let row = counters.entry(format!("{shard}:{function}")).or_default();
            row.samples = row.samples.saturating_add(1);
            row.failures = row.failures.saturating_add(u64::from(result.is_err()));
            row.total_ns = row.total_ns.saturating_add(nanos);
            row.maximum_ns = row.maximum_ns.max(nanos);
            row.latency_buckets[bucket] = row.latency_buckets[bucket].saturating_add(1);
        }
        result
    }

    pub async fn check_topology(&self) -> Result<()> {
        for shard in 0..self.shards.len() {
            let data = self.call(shard, "cairn.ping", vec![]).await?;
            let id = data.first().and_then(Value::as_u64).unwrap_or(u64::MAX);
            let count = data.get(1).and_then(Value::as_u64).unwrap_or(0);
            if !core::accepts_topology(id, count, shard as u64, self.shards.len() as u64) {
                return Err(DbError::new(
                    "55000",
                    format!("shard {shard} topology mismatch; do not reorder or resize a live cluster"),
                ));
            }
        }
        Ok(())
    }

    pub async fn schema(&self, name: &str) -> Result<Schema> {
        let mut found: Option<Schema> = None;
        // DDL is not atomic across shards. Refuse a table unless every shard agrees.
        for shard in 0..self.shards.len() {
            let data = self.call(shard, "cairn.table_schema", vec![name.into()]).await?;
            let json = data
                .first()
                .and_then(Value::as_str)
                .ok_or_else(|| DbError::new("42P01", format!("table {name} is absent on shard {shard}")))?;
            let schema: Schema = serde_json::from_str(json).map_err(|e| DbError::new("XX000", e.to_string()))?;
            if found.as_ref().is_some_and(|previous| previous != &schema) {
                return Err(DbError::new(
                    "55000",
                    "inconsistent shard schemas; repair DDL before accessing this table",
                ));
            }
            found = Some(schema);
        }
        found.ok_or_else(|| DbError::new("42P01", "table not found"))
    }

    pub async fn create(&self, schema: &Schema, if_not_exists: bool) -> Result<()> {
        let json = serde_json::to_string(schema).map_err(|e| DbError::new("XX000", e.to_string()))?;
        for shard in 0..self.shards.len() {
            self.call(
                shard,
                "cairn.create_table",
                vec![json.as_str().into(), if_not_exists.into()],
            )
            .await?;
        }
        Ok(())
    }
}
