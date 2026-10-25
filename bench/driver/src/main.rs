use anyhow::{anyhow, bail, ensure, Result};
use mysql_async::prelude::Queryable;
use serde_json::json;
use std::{
    collections::{BTreeMap, HashSet},
    env,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{watch, Barrier};

enum Connection {
    Postgres {
        client: tokio_postgres::Client,
        read: tokio_postgres::Statement,
        update: tokio_postgres::Statement,
        driver: tokio::task::JoinHandle<()>,
    },
    Mysql {
        connection: mysql_async::Conn,
        read: mysql_async::Statement,
        update: mysql_async::Statement,
    },
}
impl Connection {
    async fn new(kind: &str, url: &str, table: &str) -> Result<Self> {
        if kind == "pg" {
            let (client, driver) = tokio_postgres::connect(url, tokio_postgres::NoTls).await?;
            let driver = tokio::spawn(async move {
                if let Err(error) = driver.await {
                    eprintln!("PostgreSQL connection: {error}");
                }
            });
            if env::var("DB_LABEL").as_deref() == Ok("yugabyte") {
                let isolation = yugabyte_isolation()?;
                client
                    .batch_execute(&format!(
                        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {isolation}"
                    ))
                    .await?;
                let actual: String = client
                    .query_one("SHOW default_transaction_isolation", &[])
                    .await?
                    .try_get(0)?;
                ensure!(
                    actual.eq_ignore_ascii_case(isolation),
                    "YugabyteDB isolation differs: {actual}"
                );
            }
            let read = client
                .prepare(&format!("SELECT id,value,payload FROM {table} WHERE id=$1"))
                .await?;
            let update = client
                .prepare(&format!("UPDATE {table} SET value=value+$1 WHERE id=$2"))
                .await?;
            Ok(Self::Postgres {
                client,
                read,
                update,
                driver,
            })
        } else {
            let options = mysql_async::Opts::from_url(url)?;
            let mut connection = mysql_async::Conn::new(options).await?;
            connection.query_drop("SET SESSION tidb_txn_mode='pessimistic'").await?;
            connection
                .query_drop("SET SESSION tx_isolation='REPEATABLE-READ'")
                .await?;
            let actual: (String, String) = connection
                .query_first("SELECT @@session.tx_isolation,@@session.tidb_txn_mode")
                .await?
                .ok_or_else(|| anyhow!("missing TiDB session settings"))?;
            ensure!(
                actual.0.eq_ignore_ascii_case("REPEATABLE-READ") && actual.1 == "pessimistic",
                "TiDB transaction settings differ: {actual:?}"
            );
            let read = connection
                .prep(format!("SELECT id,value,payload FROM {table} WHERE id=?"))
                .await?;
            let update = connection
                .prep(format!("UPDATE {table} SET value=value+? WHERE id=?"))
                .await?;
            Ok(Self::Mysql {
                connection,
                read,
                update,
            })
        }
    }
    async fn close(self) -> Result<()> {
        match self {
            Self::Postgres {
                client,
                read,
                update,
                driver,
            } => {
                drop(read);
                drop(update);
                drop(client);
                driver.await?;
            }
            Self::Mysql { connection, .. } => connection.disconnect().await?,
        }
        Ok(())
    }
    async fn read(&mut self, id: i64, payload: &str) -> Result<()> {
        let (actual, value, text): (i64, i64, String) = match self {
            Self::Postgres { client, read, .. } => {
                let row = client.query_one(read, &[&id]).await?;
                (row.try_get(0)?, row.try_get(1)?, row.try_get(2)?)
            }
            Self::Mysql { connection, read, .. } => connection
                .exec_first(&*read, (id,))
                .await?
                .ok_or_else(|| anyhow!("missing key {id}"))?,
        };
        ensure!(
            actual == id && text == payload,
            "incorrect point result for {id}: {actual}, {value}"
        );
        Ok(())
    }
    async fn update(&mut self, id: i64, delta: i64) -> Result<()> {
        let count = match self {
            Self::Postgres { client, update, .. } => client.execute(update, &[&delta, &id]).await?,
            Self::Mysql { connection, update, .. } => {
                connection.exec_drop(&*update, (delta, id)).await?;
                connection.affected_rows()
            }
        };
        ensure!(count == 1, "update affected {count} rows for key {id}");
        Ok(())
    }
    async fn control(&mut self, query: &str) -> Result<()> {
        match self {
            Self::Postgres { client, .. } => {
                client.simple_query(query).await?;
            }
            Self::Mysql { connection, .. } => {
                connection.query_drop(query).await?;
            }
        }
        Ok(())
    }
    async fn check(&mut self, table: &str, expected_rows: i64, payload: &str) -> Result<(i128, Vec<i64>)> {
        let query = format!("SELECT id,value,payload FROM {table}");
        let rows: Vec<(i64, i64, String)> = match self {
            Self::Postgres { client, .. } => client
                .query(&query, &[])
                .await?
                .iter()
                .map(|row| Ok((row.try_get(0)?, row.try_get(1)?, row.try_get(2)?)))
                .collect::<Result<_>>()?,
            Self::Mysql { connection, .. } => connection.query(query).await?,
        };
        ensure!(
            rows.len() == expected_rows as usize,
            "unexpected row count {}",
            rows.len()
        );
        let mut seen = HashSet::new();
        let mut sum = 0i128;
        let mut values = vec![0; expected_rows as usize];
        for (id, value, text) in rows {
            ensure!(
                id >= 0 && id < expected_rows && seen.insert(id) && text == payload,
                "incorrect dataset row {id}"
            );
            sum += i128::from(value);
            values[id as usize] = value;
        }
        Ok((sum, values))
    }
}
fn retryable(error: &anyhow::Error) -> bool {
    if let Some(error) = error.downcast_ref::<tokio_postgres::Error>() {
        return error
            .as_db_error()
            .is_some_and(|error| ["40001", "40P01"].contains(&error.code().code()));
    }
    matches!(error.downcast_ref::<mysql_async::Error>(),Some(mysql_async::Error::Server(error)) if [1205,1213,9007].contains(&error.code))
}
fn random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}
#[derive(Default, Debug)]
struct Stats {
    latency: Vec<u64>,
    retries: u64,
    updates: u64,
    warm_updates: u64,
    end: Option<Instant>,
    retry_reasons: BTreeMap<String, u64>,
    deltas: BTreeMap<i64, i64>,
}
struct Work {
    kind: String,
    url: String,
    table: String,
    payload: String,
    rows: i64,
    clients: usize,
    seconds: u64,
    warmup: u64,
    workload: String,
    partitions: Arc<[Vec<i64>; 2]>,
}
async fn operation(connection: &mut Connection, work: &Work, id: i64, choice: u64, from: i64, to: i64) -> Result<u64> {
    match work.workload.as_str() {
        "read" => {
            connection.read(id, &work.payload).await?;
            Ok(0)
        }
        "mixed" if choice % 10 != 0 => {
            connection.read(id, &work.payload).await?;
            Ok(0)
        }
        "mixed" => {
            connection.update(id, 1).await?;
            Ok(1)
        }
        "transfer" => {
            connection.control("BEGIN").await?;
            let result = async {
                connection.update(from, -1).await?;
                connection.update(to, 1).await?;
                connection.control("COMMIT").await?;
                Ok(0)
            }
            .await;
            if let Err(original) = &result {
                if let Err(cleanup) = connection.control("ROLLBACK").await {
                    // A rollback conflict does not make the whole operation
                    // safe to retry: this connection may still be in a block.
                    // Preserve both diagnostics without exposing a retryable
                    // database error through anyhow's downcast chain.
                    bail!("rollback after failed transfer did not complete; original: {original:#}; rollback: {cleanup:#}");
                }
            }
            result
        }
        _ => bail!("unknown workload"),
    }
}
async fn worker(
    work: Arc<Work>,
    index: usize,
    ready: Arc<Barrier>,
    mut start: watch::Receiver<Option<Instant>>,
) -> Result<Stats> {
    let mut connection = Connection::new(&work.kind, &work.url, &work.table).await?;
    let mut state = 0x9e3779b97f4a7c15u64.wrapping_mul(index as u64 + 1);
    let mut stats = Stats::default();
    let warm_end = Instant::now() + Duration::from_secs(work.warmup);
    let mut measured_start = None;
    loop {
        let measured = measured_start.is_some();
        if !measured && Instant::now() >= warm_end {
            ready.wait().await;
            start.changed().await?;
            measured_start = *start.borrow();
            continue;
        }
        if measured_start.is_some_and(|start| Instant::now() >= start + Duration::from_secs(work.seconds)) {
            break;
        }
        let id = (random(&mut state) % work.rows as u64) as i64;
        let choice = random(&mut state);
        let first = work.partitions[0][random(&mut state) as usize % work.partitions[0].len()];
        let second = work.partitions[1][random(&mut state) as usize % work.partitions[1].len()];
        let (from, to) = if choice & 1 == 0 {
            (first, second)
        } else {
            (second, first)
        };
        let began = Instant::now();
        let mut attempts = 0;
        loop {
            match operation(&mut connection, &work, id, choice, from, to).await {
                Ok(updates) => {
                    if updates > 0 {
                        *stats.deltas.entry(id).or_default() += 1;
                    }
                    if work.workload == "transfer" {
                        *stats.deltas.entry(from).or_default() -= 1;
                        *stats.deltas.entry(to).or_default() += 1;
                    }
                    if measured {
                        stats.updates += updates;
                        stats
                            .latency
                            .push(began.elapsed().as_nanos().min(u64::MAX as u128) as u64);
                    } else {
                        stats.warm_updates += updates;
                    }
                    break;
                }
                Err(error) if retryable(&error) && attempts < 32 => {
                    attempts += 1;
                    if measured {
                        stats.retries += 1;
                        let reason = error
                            .downcast_ref::<tokio_postgres::Error>()
                            .and_then(|error| error.as_db_error())
                            .map(|error| error.message().to_owned())
                            .unwrap_or_else(|| error.to_string());
                        *stats.retry_reasons.entry(reason).or_default() += 1;
                    }
                    tokio::time::sleep(Duration::from_micros(100 * attempts + (random(&mut state) % 500))).await;
                }
                Err(error) => {
                    return Err(error.context(format!(
                        "worker {index}, workload {}, attempts {attempts}",
                        work.workload
                    )))
                }
            }
        }
    }
    stats.end = Some(Instant::now());
    connection.close().await?;
    Ok(stats)
}
async fn run(work: Arc<Work>) -> Result<serde_json::Value> {
    let mut check = Connection::new(&work.kind, &work.url, &work.table).await?;
    let (before, mut expected) = check.check(&work.table, work.rows, &work.payload).await?;
    check.close().await?;
    let ready = Arc::new(Barrier::new(work.clients + 1));
    let (sender, start) = watch::channel(None);
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..work.clients {
        let work = work.clone();
        let ready = ready.clone();
        let start = start.clone();
        tasks.spawn(async move { worker(work, index, ready, start).await });
    }
    // Do not hang at the barrier if a client fails during setup or warmup.
    tokio::select! {
        _=ready.wait()=>{}
        failure=tasks.join_next()=>bail!("worker failed before timing: {:?}",failure),
        _=tokio::time::sleep(Duration::from_secs(60))=>bail!("workers did not finish warmup within 60 seconds"),
    }
    let began = Instant::now();
    sender.send(Some(began))?;
    let mut all = Stats::default();
    let mut end = began;
    while let Some(result) = tasks.join_next().await {
        let stats = result??;
        end = end.max(stats.end.unwrap());
        all.latency.extend(stats.latency);
        all.retries += stats.retries;
        all.updates += stats.updates;
        all.warm_updates += stats.warm_updates;
        for (id, delta) in stats.deltas {
            expected[id as usize] = expected[id as usize]
                .checked_add(delta)
                .ok_or_else(|| anyhow!("expected value overflow for key {id}"))?;
        }
        for (reason, count) in stats.retry_reasons {
            *all.retry_reasons.entry(reason).or_default() += count;
        }
    }
    let elapsed = (end - began).as_secs_f64();
    let mut check = Connection::new(&work.kind, &work.url, &work.table).await?;
    let (after, actual) = check.check(&work.table, work.rows, &work.payload).await?;
    check.close().await?;
    ensure!(
        after == before + i128::from(all.updates + all.warm_updates),
        "committed sum mismatch: before={before}, after={after}, updates={}",
        all.updates + all.warm_updates
    );
    for (id, (actual, expected)) in actual.iter().zip(&expected).enumerate() {
        ensure!(
            actual == expected,
            "wrong value for key {id}: expected {expected}, actual {actual}"
        );
    }
    all.latency.sort_unstable();
    ensure!(!all.latency.is_empty(), "no completed operations");
    let percentile = |p: f64| all.latency[((all.latency.len() - 1) as f64 * p).ceil() as usize] as f64 / 1000.0;
    Ok(
        json!({"workload":work.workload,"clients":work.clients,"operations":all.latency.len(),
        "elapsed_seconds":elapsed,"operations_per_second":all.latency.len() as f64/elapsed,
        "latency_us":{"p50":percentile(0.5),"p95":percentile(0.95),"p99":percentile(0.99),"p999":percentile(0.999)},
        "serialization_retries":all.retries,"retry_reasons":all.retry_reasons,"measured_updates":all.updates,"warmup_updates":all.warm_updates,
        "correctness":{"rows":work.rows,"sum_before":before.to_string(),"sum_after":after.to_string(),"all_key_values_verified":true,"verified":true}}),
    )
}
fn yugabyte_isolation() -> Result<&'static str> {
    match env::var("YB_ISOLATION").as_deref().unwrap_or("repeatable-read") {
        "serializable" => Ok("SERIALIZABLE"),
        "repeatable-read" => Ok("REPEATABLE READ"),
        "read-committed" => Ok("READ COMMITTED"),
        _ => bail!("unsupported YB_ISOLATION"),
    }
}

fn number(name: &str, default: u64) -> Result<u64> {
    Ok(env::var(name)
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(default))
}
#[tokio::main(worker_threads = 4)]
async fn main() -> Result<()> {
    let kind = env::var("DB_KIND").unwrap_or("pg".into());
    let url = env::var("DB_URL")?;
    let label = env::var("DB_LABEL")?;
    let isolation = match label.as_str() {
        "yugabyte" => yugabyte_isolation()?.to_lowercase(),
        "tidb" => "repeatable read with pessimistic writes".to_owned(),
        _ => "strict two-phase locking".to_owned(),
    };
    let table = format!("bench_{}", SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos());
    let rows = number("BENCH_ROWS", 10000)? as i64;
    let clients = number("BENCH_CLIENTS", 16)? as usize;
    let seconds = number("BENCH_SECONDS", 10)?;
    let warmup = number("BENCH_WARMUP", 2)?;
    let repeats = number("BENCH_REPEATS", 3)?;
    let payload = "x".repeat(number("BENCH_PAYLOAD", 128)? as usize);
    ensure!(
        rows >= 4 && rows % 4 == 0 && clients > 0 && clients <= 256 && seconds > 0 && repeats > 0,
        "invalid benchmark bounds"
    );
    // Create before preparing worker statements. The temporary connection uses
    // the same protocol but no table-dependent prepared statements.
    let mut partitions = [Vec::new(), Vec::new()];
    if kind == "pg" {
        let (client, driver) = tokio_postgres::connect(&url, tokio_postgres::NoTls).await?;
        tokio::spawn(async move {
            let _ = driver.await;
        });
        let split = if label == "yugabyte" {
            " SPLIT INTO 2 TABLETS"
        } else {
            ""
        };
        client
            .simple_query(&format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY,value BIGINT NOT NULL,payload TEXT NOT NULL){split}"
            ))
            .await?;
        for first in (0..rows).step_by(256) {
            let values = (first..rows.min(first + 256))
                .map(|id| format!("({id},1000000,'{payload}')"))
                .collect::<Vec<_>>()
                .join(",");
            client
                .simple_query(&format!("INSERT INTO {table} VALUES {values}"))
                .await?;
        }
        if label == "yugabyte" {
            for row in client
                .query(&format!("SELECT id,yb_hash_code(id) FROM {table}"), &[])
                .await?
            {
                let id: i64 = row.try_get(0)?;
                let hash: i32 = row.try_get(1)?;
                partitions[usize::from(hash >= 32768)].push(id);
            }
        } else {
            for id in 0..rows {
                partitions[(id % 2) as usize].push(id);
            }
        }
    } else {
        let mut connection = mysql_async::Conn::new(mysql_async::Opts::from_url(&url)?).await?;
        connection
            .query_drop(format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY CLUSTERED,value BIGINT NOT NULL,payload TEXT NOT NULL)"
            ))
            .await?;
        for first in (0..rows).step_by(256) {
            let values = (first..rows.min(first + 256))
                .map(|id| format!("({id},1000000,'{payload}')"))
                .collect::<Vec<_>>()
                .join(",");
            connection
                .query_drop(format!("INSERT INTO {table} VALUES {values}"))
                .await?;
        }
        connection
            .query_drop(format!("SPLIT TABLE {table} BETWEEN (0) AND ({rows}) REGIONS 2"))
            .await?;
        for id in 0..rows {
            partitions[usize::from(id >= rows / 2)].push(id);
        }
    }
    ensure!(
        !partitions[0].is_empty() && !partitions[1].is_empty(),
        "empty transfer partition"
    );
    let partitions = Arc::new(partitions);
    eprintln!("loaded and initialized {label} table={table} rows={rows}");
    let mut results = Vec::new();
    let path = env::var("BENCH_OUTPUT").unwrap_or(format!("/artifacts/{label}-benchmark.json"));
    let save = |results: &Vec<serde_json::Value>, status: &str, failure: Option<String>| -> Result<()> {
        let report = json!({"database":label,"table":table,"rows":rows,"payload_bytes":payload.len(),
            "seconds_per_run":seconds,"warmup_seconds":warmup,"protocol":"prepared binary parameters and results",
            "client":"Rust async, four runtime threads, one outstanding operation per connection",
            "queueing":"closed loop; latency includes retries; coordinated omission not corrected",
            "transfer_partitions":{"count":2,"rows":[partitions[0].len(),partitions[1].len()],"all_transfers_cross_partitions":true},
            "transaction_isolation":isolation,"status":status,"failure":failure,"results":results});
        let destination = if status == "complete" {
            path.clone()
        } else {
            format!("{path}.{status}")
        };
        std::fs::write(destination, serde_json::to_vec_pretty(&report)?)?;
        Ok(())
    };
    let workloads = env::var("BENCH_WORKLOADS").unwrap_or("read,mixed,transfer".into());
    for workload in workloads.split(',') {
        for repeat in 0..repeats {
            let work = Arc::new(Work {
                kind: kind.clone(),
                url: url.clone(),
                table: table.clone(),
                payload: payload.clone(),
                rows,
                clients,
                seconds,
                warmup,
                workload: workload.into(),
                partitions: partitions.clone(),
            });
            let mut result = match run(work).await {
                Ok(result) => result,
                Err(error) => {
                    save(&results, "failed", Some(format!("{error:#}")))?;
                    return Err(error);
                }
            };
            result["repeat"] = json!(repeat + 1);
            eprintln!(
                "{label} {workload} run {}: {:.0} ops/s p99 {:.0}us retries {}",
                repeat + 1,
                result["operations_per_second"].as_f64().unwrap(),
                result["latency_us"]["p99"].as_f64().unwrap(),
                result["serialization_retries"]
            );
            results.push(result);
            save(&results, "partial", None)?;
        }
    }
    save(&results, "complete", None)?;
    println!("{path}");
    Ok(())
}
