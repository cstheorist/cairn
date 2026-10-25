//! Durable transaction orchestration. Storage authenticates prepare/apply/release
//! receipts and refuses conflicting decisions; recovery is idempotent.
use crate::{core, storage::Cluster, DbError, Result};
use rmpv::Value;
use std::collections::BTreeMap;

#[derive(Clone)]
struct Decision {
    id: String,
    kind: String,
    members: Vec<usize>,
    complete: bool,
    certificate: String,
    completion_certificate: String,
    voted: bool,
    frozen: bool,
    expired: bool,
    scope: String,
    finish_scope: String,
    volatile: bool,
    open_blob: String,
    open_mac: String,
}

fn invalid() -> DbError {
    DbError::new("XX000", "invalid transaction metadata response")
}
fn coordinator_for(id: &str) -> Result<usize> {
    let (identity, suffix) = id.split_once('~').map_or((id, None), |(id, suffix)| (id, Some(suffix)));
    if suffix.is_some_and(|suffix| {
        suffix.len() != 16
            || !suffix
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    }) {
        return Err(invalid());
    }
    let uuid = uuid::Uuid::parse_str(identity).map_err(|_| invalid())?;
    let bytes = uuid.as_bytes();
    Ok(core::transaction_home(u16::from_be_bytes([bytes[0], bytes[1]]), bytes[6] >> 4 == 8) as usize)
}
fn first_string(data: &[Value]) -> Result<String> {
    data.first()
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(invalid)
}
impl Decision {
    fn parse(data: &[Value]) -> Result<Self> {
        let row = data.first().and_then(Value::as_array).ok_or_else(invalid)?;
        let string = |index| {
            row.get(index)
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or_else(invalid)
        };
        let members = row
            .get(2)
            .and_then(Value::as_array)
            .ok_or_else(invalid)?
            .iter()
            .map(|member| member.as_u64().map(|member| member as usize).ok_or_else(invalid))
            .collect::<Result<Vec<_>>>()?;
        let kind = string(1)?;
        if !["OPEN", "COMMIT", "ABORT"].contains(&kind.as_str()) {
            return Err(invalid());
        }
        Ok(Self {
            id: string(0)?,
            kind,
            members,
            complete: row.get(4).and_then(Value::as_bool).ok_or_else(invalid)?,
            certificate: string(6)?,
            completion_certificate: string(7)?,
            voted: row.get(8).and_then(Value::as_bool).unwrap_or(false),
            frozen: row.get(9).and_then(Value::as_bool).unwrap_or(false),
            scope: row.get(10).and_then(Value::as_str).unwrap_or("").to_owned(),
            expired: row.get(11).and_then(Value::as_bool).unwrap_or(false),
            finish_scope: row.get(12).and_then(Value::as_str).unwrap_or("").to_owned(),
            volatile: row.get(13).and_then(Value::as_bool).unwrap_or(false),
            open_blob: row.get(14).and_then(Value::as_str).unwrap_or("").to_owned(),
            open_mac: row.get(15).and_then(Value::as_str).unwrap_or("").to_owned(),
        })
    }
    fn members_value(&self) -> Value {
        Value::Array(self.members.iter().map(|member| (*member).into()).collect())
    }
}

impl Cluster {
    async fn phase_receipts(
        &self,
        function: &'static str,
        requests: impl IntoIterator<Item = (usize, Vec<Value>)>,
    ) -> Result<Vec<Value>> {
        // Independent participant phases run concurrently. try_join_all retains
        // participant order, which the coordinator uses to validate receipts.
        futures_util::future::try_join_all(requests.into_iter().map(|(shard, args)| async move {
            let response = self.phase_call(shard, function, args).await?;
            Ok(first_string(&response)?.into())
        }))
        .await
    }
    async fn phase_call(&self, shard: usize, function: &str, args: Vec<Value>) -> Result<Vec<Value>> {
        let mut retries = 0;
        loop {
            let result = self.call(shard, function, args.clone()).await;
            if retries < 3
                && result
                    .as_ref()
                    .is_err_and(|error| matches!(error.code, "40001" | "08006" | "57P03"))
            {
                retries += 1;
                tokio::time::sleep(std::time::Duration::from_millis(10 * retries)).await;
            } else {
                return result;
            }
        }
    }
    async fn raw_transaction_decision(&self, id: &str, kind: &str, proofs: Vec<Value>) -> Result<Decision> {
        let home = coordinator_for(id)?;
        if home >= self.shard_count() {
            return Err(invalid());
        }
        Decision::parse(
            &self
                .call(
                    home,
                    "cairn.tx_decide",
                    vec![id.into(), kind.into(), Value::Array(proofs)],
                )
                .await?,
        )
    }

    async fn transaction_decision(&self, id: &str, kind: &str, proofs: Vec<Value>) -> Result<Decision> {
        let decision = self.raw_transaction_decision(id, kind, proofs).await?;
        if kind == "ABORT" && decision.kind == "OPEN" && decision.voted {
            self.resolve_votes(decision, true).await?.ok_or_else(invalid)
        } else {
            Ok(decision)
        }
    }

    async fn resolve_votes(&self, decision: Decision, force_abort: bool) -> Result<Option<Decision>> {
        if !decision.voted || !decision.frozen || decision.members.is_empty() || decision.scope.is_empty() {
            return Err(invalid());
        }
        // A successful NO is immutable. If a concurrent prepare wins instead,
        // at least one additional participant is YES on the next iteration.
        for _ in 0..=decision.members.len() {
            let votes = futures_util::future::try_join_all(decision.members.iter().map(|member| async {
                let response = self
                    .phase_call(
                        *member,
                        "cairn.tx_vote_status",
                        vec![
                            decision.id.as_str().into(),
                            decision.members_value(),
                            decision.scope.as_str().into(),
                        ],
                    )
                    .await?;
                let vote = response.first().and_then(Value::as_array).ok_or_else(invalid)?;
                let kind = vote.first().and_then(Value::as_str).ok_or_else(invalid)?.to_owned();
                let receipt = vote.get(1).and_then(Value::as_str).ok_or_else(invalid)?.to_owned();
                if !["YES", "NO", "OPEN"].contains(&kind.as_str()) {
                    return Err(invalid());
                }
                Ok((*member, kind, receipt))
            }))
            .await?;
            if let Some((member, _, receipt)) = votes.iter().find(|(_, kind, _)| kind == "NO") {
                return self
                    .raw_transaction_decision(&decision.id, "ABORT", vec![(*member).into(), receipt.as_str().into()])
                    .await
                    .map(Some);
            }
            let yes = votes.iter().filter(|(_, kind, _)| kind == "YES").count();
            if core::may_decide_commit(yes as u64, decision.members.len() as u64, false) {
                return self
                    .raw_transaction_decision(
                        &decision.id,
                        "COMMIT",
                        votes.iter().map(|(_, _, receipt)| receipt.as_str().into()).collect(),
                    )
                    .await
                    .map(Some);
            }
            if !force_abort {
                return Ok(None);
            }
            let (member, _, _) = votes.iter().find(|(_, kind, _)| kind == "OPEN").ok_or_else(invalid)?;
            self.phase_call(
                *member,
                "cairn.tx_vote_abort",
                vec![
                    decision.id.as_str().into(),
                    decision.members_value(),
                    decision.scope.as_str().into(),
                ],
            )
            .await?;
        }
        Err(invalid())
    }

    async fn resolve_transaction(&self, mut decision: Decision) -> Result<bool> {
        let home = coordinator_for(&decision.id)?;
        if home >= self.shard_count() || decision.members.iter().any(|member| *member >= self.shard_count()) {
            return Err(invalid());
        }
        if decision.kind == "OPEN" {
            return Err(DbError::new("25000", "cannot resolve an undecided transaction"));
        }
        if !decision.finish_scope.is_empty() {
            let released = self
                .phase_receipts(
                    "cairn.tx_finish_vote",
                    decision.members.iter().map(|member| {
                        (
                            *member,
                            vec![
                                decision.id.as_str().into(),
                                decision.kind.as_str().into(),
                                decision.members_value(),
                                decision.certificate.as_str().into(),
                                decision.finish_scope.as_str().into(),
                            ],
                        )
                    }),
                )
                .await?;
            self.phase_call(
                home,
                "cairn.tx_clean",
                vec![decision.id.as_str().into(), Value::Array(released)],
            )
            .await?;
            return Ok(decision.kind == "COMMIT");
        }
        if !decision.complete {
            let applied = self
                .phase_receipts(
                    "cairn.tx_apply",
                    decision.members.iter().map(|member| {
                        (
                            *member,
                            vec![
                                decision.id.as_str().into(),
                                decision.kind.as_str().into(),
                                decision.members_value(),
                                decision.certificate.as_str().into(),
                            ],
                        )
                    }),
                )
                .await?;
            decision = Decision::parse(
                &self
                    .phase_call(
                        home,
                        "cairn.tx_complete",
                        vec![decision.id.as_str().into(), Value::Array(applied)],
                    )
                    .await?,
            )?;
        }
        let released = self
            .phase_receipts(
                "cairn.tx_release",
                decision.members.iter().map(|member| {
                    (
                        *member,
                        vec![
                            decision.id.as_str().into(),
                            decision.kind.as_str().into(),
                            decision.members_value(),
                            decision.completion_certificate.as_str().into(),
                        ],
                    )
                }),
            )
            .await?;
        self.phase_call(
            home,
            "cairn.tx_clean",
            vec![decision.id.as_str().into(), Value::Array(released)],
        )
        .await?;
        Ok(decision.kind == "COMMIT")
    }

    pub async fn recover_transactions(self: &std::sync::Arc<Self>) -> Result<usize> {
        let mut queries = tokio::task::JoinSet::new();
        for home in 0..self.shard_count() {
            let cluster = self.clone();
            queries.spawn(async move { cluster.call(home, "cairn.tx_pending", vec![]).await });
        }
        let mut records = BTreeMap::new();
        let mut failure = None;
        while let Some(response) = queries.join_next().await {
            match response.map_err(|_| invalid()).and_then(|result| result) {
                Ok(data) => {
                    for row in data.first().and_then(Value::as_array).ok_or_else(invalid)? {
                        let decision = Decision::parse(std::slice::from_ref(row))?;
                        records.insert(decision.id.clone(), decision);
                    }
                }
                Err(error) => {
                    failure = Some(error);
                }
            }
        }
        // A remote YES can precede the home prepare. Its authenticated frozen
        // scope is sufficient to recover even when no coordinator row exists.
        let mut orphan_queries = tokio::task::JoinSet::new();
        for shard in 0..self.shard_count() {
            let cluster = self.clone();
            orphan_queries.spawn(async move { cluster.call(shard, "cairn.tx_open_pending", vec![]).await });
        }
        while let Some(response) = orphan_queries.join_next().await {
            match response.map_err(|_| invalid()).and_then(|result| result) {
                Ok(data) => {
                    for row in data.first().and_then(Value::as_array).ok_or_else(invalid)? {
                        let values = row.as_array().ok_or_else(invalid)?;
                        let id = values.first().and_then(Value::as_str).ok_or_else(invalid)?;
                        if records.contains_key(id) {
                            continue;
                        }
                        let home = coordinator_for(id)?;
                        let result = self
                            .call(
                                home,
                                "cairn.tx_open_recover",
                                vec![
                                    values[0].clone(),
                                    values.get(1).ok_or_else(invalid)?.clone(),
                                    values.get(2).ok_or_else(invalid)?.clone(),
                                    false.into(),
                                ],
                            )
                            .await;
                        match result.and_then(|data| Decision::parse(&data)) {
                            Ok(decision) => {
                                records.insert(id.to_owned(), decision);
                            }
                            Err(error) => {
                                failure = Some(error);
                            }
                        }
                    }
                }
                Err(error) => {
                    failure = Some(error);
                }
            }
        }
        let mut tasks = tokio::task::JoinSet::new();
        let limit = std::sync::Arc::new(tokio::sync::Semaphore::new(4));
        for mut decision in records.into_values() {
            let cluster = self.clone();
            let limit = limit.clone();
            tasks.spawn(async move {
                let _permit = limit.acquire_owned().await.map_err(|_| invalid())?;
                if decision.kind == "OPEN" {
                    if decision.voted && decision.frozen {
                        let expired = decision.expired;
                        let Some(resolved) = cluster.resolve_votes(decision, expired).await? else {
                            return Ok(());
                        };
                        decision = resolved;
                    } else {
                        decision = cluster.transaction_decision(&decision.id, "ABORT", vec![]).await?;
                    }
                }
                cluster.resolve_transaction(decision).await.map(|_| ())
            });
        }
        let mut count = 0;
        while let Some(result) = tasks.join_next().await {
            match result.map_err(|_| invalid()).and_then(|result| result) {
                Ok(()) => {
                    count += 1;
                }
                Err(error) => {
                    failure = Some(error);
                }
            }
        }
        if let Some(error) = failure {
            Err(error)
        } else {
            Ok(count)
        }
    }
}

pub struct Transaction {
    id: String,
    started: bool,
    participants: BTreeMap<usize, String>,
    finished: Option<bool>,
    pub read_only: bool,
    voted: bool,
    fast_finish: bool,
    volatile: bool,
    open_scope: Option<Decision>,
    commit_attempted: bool,
}

impl Transaction {
    fn new(read_only: bool) -> Self {
        let mode = std::env::var("CAIRN_VOTE_COMMIT");
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            started: false,
            participants: BTreeMap::new(),
            finished: None,
            read_only,
            voted: matches!(mode.as_deref(), Ok("1") | Ok("2") | Ok("3")),
            fast_finish: matches!(mode.as_deref(), Ok("2") | Ok("3")),
            volatile: mode.as_deref() == Ok("3"),
            open_scope: None,
            commit_attempted: false,
        }
    }

    pub async fn call(
        &mut self,
        cluster: &Cluster,
        shard: usize,
        function: &str,
        mut args: Vec<Value>,
    ) -> Result<Vec<Value>> {
        if self.finished.is_some() {
            return Err(DbError::new("25P02", "transaction is already resolved"));
        }
        if !self.started {
            let mut bytes = *uuid::Uuid::parse_str(&self.id).map_err(|_| invalid())?.as_bytes();
            bytes[..2].copy_from_slice(&(shard as u16).to_be_bytes());
            bytes[6] = (bytes[6] & 0x0f) | 0x80; // UUIDv8: first two bytes name the coordinator.
            self.id = uuid::Uuid::from_bytes(bytes).to_string();
            if self.volatile {
                self.id
                    .push_str(&format!("~{:016x}", cluster.incarnation(shard).await?));
            }
            // Mark before sending: a lost response must still cause durable recovery.
            self.started = true;
            let response = cluster
                .call(
                    shard,
                    if self.volatile {
                        "cairn.tx_open_start"
                    } else if self.fast_finish {
                        "cairn.tx_start_finished"
                    } else if self.voted {
                        "cairn.tx_start_voted"
                    } else {
                        "cairn.tx_start"
                    },
                    vec![self.id.as_str().into(), function.into(), Value::Array(args)],
                )
                .await?;
            let token = first_string(&response)?;
            let data = response.get(1).and_then(Value::as_array).cloned().ok_or_else(invalid)?;
            self.participants.insert(shard, token);
            return Ok(data);
        }
        if !self.participants.contains_key(&shard) {
            let mut enlist = vec![self.id.as_str().into(), shard.into()];
            if self.volatile {
                enlist.push(format!("{:016x}", cluster.incarnation(shard).await?).into());
            }
            let response = cluster
                .call(
                    coordinator_for(&self.id)?,
                    if self.volatile {
                        "cairn.tx_open_enlist"
                    } else {
                        "cairn.tx_enlist"
                    },
                    enlist,
                )
                .await?;
            self.participants.insert(shard, first_string(&response)?);
        }
        args.insert(0, self.id.as_str().into());
        args.push(self.participants[&shard].as_str().into());
        cluster.call(shard, function, args).await
    }

    async fn commit(&mut self, cluster: &Cluster) -> Result<()> {
        if !self.started {
            self.finished = Some(true);
            return Ok(());
        }
        let home = coordinator_for(&self.id)?;
        let only = self.participants.keys().next().copied().unwrap_or(usize::MAX);
        if core::may_commit_locally(self.participants.len() as u64, home as u64, only as u64) {
            self.commit_attempted = true;
            let result = cluster
                .phase_call(
                    home,
                    if self.volatile {
                        "cairn.tx_open_local"
                    } else {
                        "cairn.tx_commit_local"
                    },
                    vec![self.id.as_str().into()],
                )
                .await
                .map_err(|error| {
                    if error.code == "25P02" {
                        DbError::new("40001", "transaction expired before its commit decision")
                    } else {
                        DbError::new(
                            "40003",
                            format!(
                                "transaction {} requires local commit outcome recovery: {error}",
                                self.id
                            ),
                        )
                    }
                })?;
            let decision = Decision::parse(&result)?;
            if decision.kind == "COMMIT" && decision.complete {
                self.finished = Some(true);
                return Ok(());
            }
            if decision.kind == "ABORT" {
                cluster.resolve_transaction(decision).await?;
                self.finished = Some(false);
                return Err(DbError::new("40001", "transaction was aborted before local commit"));
            }
            return Err(DbError::new(
                "40003",
                format!("transaction {} needs commit outcome recovery", self.id),
            ));
        }
        if self.volatile {
            self.commit_attempted = true;
            let response = cluster
                .phase_call(home, "cairn.tx_open_freeze", vec![self.id.as_str().into()])
                .await?;
            let scope = Decision::parse(&response)?;
            if !scope.volatile || !scope.frozen || scope.open_blob.is_empty() || scope.open_mac.is_empty() {
                return Err(invalid());
            }
            self.open_scope = Some(scope);
        }
        let preparation = cluster
            .phase_receipts(
                if self.volatile {
                    "cairn.tx_open_prepare"
                } else {
                    "cairn.tx_prepare"
                },
                self.participants.iter().map(|(shard, token)| {
                    let mut args = vec![self.id.as_str().into(), token.as_str().into()];
                    if let Some(scope) = &self.open_scope {
                        args.push(scope.open_blob.as_str().into());
                        args.push(scope.open_mac.as_str().into());
                    }
                    (*shard, args)
                }),
            )
            .await;
        let prepared = match preparation {
            Ok(receipts) => receipts,
            Err(error) if self.voted => {
                if let Some(scope) = &self.open_scope {
                    cluster
                        .phase_call(
                            home,
                            "cairn.tx_open_recover",
                            vec![
                                self.id.as_str().into(),
                                scope.open_blob.as_str().into(),
                                scope.open_mac.as_str().into(),
                                true.into(),
                            ],
                        )
                        .await
                        .map_err(|cleanup| {
                            DbError::new(
                                "40003",
                                format!(
                                    "transaction {} requires scope recovery after {error}: {cleanup}",
                                    self.id
                                ),
                            )
                        })?;
                }
                // All YES votes may already exist despite a lost response.
                // Never report a retryable rollback if recovery could commit.
                let decision = cluster
                    .transaction_decision(&self.id, "ABORT", vec![])
                    .await
                    .map_err(|cleanup| {
                        DbError::new(
                            "40003",
                            format!(
                                "transaction {} needs vote outcome recovery after {error}: {cleanup}",
                                self.id
                            ),
                        )
                    })?;
                let committed = cluster.resolve_transaction(decision).await.map_err(|cleanup| {
                    DbError::new(
                        "40003",
                        format!("transaction {} needs vote completion recovery: {cleanup}", self.id),
                    )
                })?;
                self.finished = Some(committed);
                return Err(if committed {
                    DbError::new(
                        "40003",
                        format!(
                            "transaction {} committed while its prepare response was uncertain",
                            self.id
                        ),
                    )
                } else {
                    DbError::new("40001", format!("transaction aborted during prepare: {error}"))
                });
            }
            Err(error) => return Err(error),
        };
        if !core::may_decide_commit(prepared.len() as u64, self.participants.len() as u64, false) {
            return Err(DbError::new(
                "XX000",
                "distributed commit requires every participant to prepare",
            ));
        }
        if let Some(scope) = &self.open_scope {
            let unknown = |error: DbError| {
                DbError::new(
                    "40003",
                    format!(
                        "transaction {} has unanimous YES votes and requires finish recovery: {error}",
                        self.id
                    ),
                )
            };
            let released = cluster
                .phase_receipts(
                    "cairn.tx_open_finish",
                    self.participants
                        .keys()
                        .filter(|member| **member != home)
                        .map(|member| {
                            (
                                *member,
                                vec![
                                    self.id.as_str().into(),
                                    scope.open_blob.as_str().into(),
                                    scope.open_mac.as_str().into(),
                                    Value::Array(prepared.clone()),
                                ],
                            )
                        }),
                )
                .await
                .map_err(unknown)?;
            let response = cluster
                .phase_call(
                    home,
                    "cairn.tx_open_finish_last",
                    vec![
                        self.id.as_str().into(),
                        scope.open_blob.as_str().into(),
                        scope.open_mac.as_str().into(),
                        Value::Array(prepared),
                        Value::Array(released),
                    ],
                )
                .await
                .map_err(unknown)?;
            let decision = Decision::parse(&response)?;
            if decision.kind != "COMMIT" || !decision.complete {
                return Err(DbError::new(
                    "40003",
                    "unanimously prepared transaction has an invalid final outcome",
                ));
            }
            self.finished = Some(true);
            return Ok(());
        }
        let unknown = |error: DbError| {
            DbError::new(
                "40003",
                format!(
                    "transaction {} requires outcome recovery after commit: {error}",
                    self.id
                ),
            )
        };
        let decision = cluster
            .transaction_decision(&self.id, "COMMIT", prepared)
            .await
            .map_err(unknown)?;
        let committed = cluster.resolve_transaction(decision).await.map_err(unknown)?;
        self.finished = Some(committed);
        if !committed {
            return Err(DbError::new(
                "40001",
                "transaction was aborted before its commit decision",
            ));
        }
        Ok(())
    }

    async fn abort(&mut self, cluster: &Cluster) -> Result<bool> {
        if let Some(committed) = self.finished {
            return Ok(committed);
        }
        if !self.started {
            self.finished = Some(false);
            return Ok(false);
        }
        if self.volatile {
            let home = coordinator_for(&self.id)?;
            if self.open_scope.is_none() && self.commit_attempted {
                let response = cluster
                    .phase_call(home, "cairn.tx_open_status", vec![self.id.as_str().into()])
                    .await?;
                if response.first().is_some_and(|value| !value.is_nil()) {
                    let decision = Decision::parse(&response)?;
                    if decision.kind != "OPEN" {
                        let committed = cluster.resolve_transaction(decision).await?;
                        self.finished = Some(committed);
                        return Ok(committed);
                    }
                    if decision.frozen {
                        self.open_scope = Some(decision);
                    }
                }
            }
            if let Some(scope) = &self.open_scope {
                cluster
                    .phase_call(
                        home,
                        "cairn.tx_open_recover",
                        vec![
                            self.id.as_str().into(),
                            scope.open_blob.as_str().into(),
                            scope.open_mac.as_str().into(),
                            true.into(),
                        ],
                    )
                    .await?;
            } else {
                let mut requests: Vec<_> = self
                    .participants
                    .iter()
                    .map(|(shard, token)| {
                        (
                            *shard,
                            vec![
                                self.id.as_str().into(),
                                token.as_str().into(),
                                self.commit_attempted.into(),
                            ],
                        )
                    })
                    .collect();
                if !self.participants.contains_key(&home) {
                    requests.push((
                        home,
                        vec![self.id.as_str().into(), "".into(), self.commit_attempted.into()],
                    ));
                }
                futures_util::future::try_join_all(
                    requests
                        .into_iter()
                        .map(|(shard, args)| cluster.phase_call(shard, "cairn.tx_open_abort", args)),
                )
                .await?;
                self.finished = Some(false);
                return Ok(false);
            }
        }
        let decision = cluster.transaction_decision(&self.id, "ABORT", vec![]).await?;
        let committed = cluster.resolve_transaction(decision).await?;
        self.finished = Some(committed);
        Ok(committed)
    }
}

#[derive(Default)]
pub struct Session {
    pub transaction: Option<Transaction>,
    pub failed: bool,
    pub implicit: bool,
}

impl Session {
    pub fn status(&self) -> u8 {
        if self.transaction.is_none() {
            b'I'
        } else if self.failed {
            b'E'
        } else {
            b'T'
        }
    }
    pub fn begin(&mut self, read_only: bool) -> Result<()> {
        if self.implicit {
            if self.failed {
                return Err(DbError::new("25P02", "current transaction is aborted; issue ROLLBACK"));
            }
            // BEGIN adopts the implicit cycle, including its existing read claims
            // and staged writes. Sync must no longer close this transaction.
            if let Some(transaction) = &mut self.transaction {
                transaction.read_only = read_only;
                self.implicit = false;
                return Ok(());
            }
        }
        if self.transaction.is_some() {
            return Err(DbError::new("25001", "a transaction is already active"));
        }
        self.transaction = Some(Transaction::new(read_only));
        self.failed = false;
        self.implicit = false;
        Ok(())
    }
    pub fn begin_implicit(&mut self) -> Result<()> {
        self.begin(false)?;
        self.implicit = true;
        Ok(())
    }
    pub fn reset(&mut self) {
        self.transaction = None;
        self.failed = false;
        self.implicit = false;
    }
    pub async fn finish(&mut self, cluster: &Cluster, commit: bool) -> Result<&'static str> {
        let Some(transaction) = self.transaction.as_mut() else {
            return Ok(if commit { "COMMIT" } else { "ROLLBACK" });
        };
        let tag = if commit && !self.failed {
            transaction.commit(cluster).await?;
            "COMMIT"
        } else {
            if transaction.abort(cluster).await? {
                self.transaction = None;
                self.failed = false;
                self.implicit = false;
                return Err(DbError::new(
                    "40003",
                    "transaction already committed; its earlier outcome was ambiguous",
                ));
            }
            "ROLLBACK"
        };
        self.transaction = None;
        self.failed = false;
        self.implicit = false;
        Ok(tag)
    }
    pub async fn fail(&mut self, cluster: &Cluster) {
        if let Some(transaction) = &mut self.transaction {
            self.failed = true;
            // A failed cleanup leaves a durable record for the recovery worker.
            let _ = transaction.abort(cluster).await;
        }
    }
    pub async fn close(&mut self, cluster: &Cluster) {
        self.fail(cluster).await;
    }
}

#[cfg(test)]
mod session_tests {
    use super::*;

    #[test]
    fn begin_adopts_implicit_identity_participants_and_claims() {
        for read_only in [false, true] {
            let mut session = Session::default();
            session.begin_implicit().unwrap();
            let transaction = session.transaction.as_mut().unwrap();
            transaction.started = true;
            transaction.participants.insert(1, "existing-claim-token".into());
            let identity = transaction.id.clone();
            session.begin(read_only).unwrap();
            let transaction = session.transaction.as_ref().unwrap();
            assert_eq!(transaction.id, identity);
            assert!(transaction.started);
            assert_eq!(transaction.participants[&1], "existing-claim-token");
            assert_eq!(transaction.read_only, read_only);
            assert!(!session.implicit);
            assert_eq!(session.status(), b'T');
            assert_eq!(session.begin(false).unwrap_err().code, "25001");
        }
    }

    #[test]
    fn begin_cannot_revive_a_failed_implicit_transaction() {
        let mut session = Session::default();
        session.begin_implicit().unwrap();
        let identity = session.transaction.as_ref().unwrap().id.clone();
        session.failed = true;
        assert_eq!(session.begin(false).unwrap_err().code, "25P02");
        assert_eq!(session.transaction.as_ref().unwrap().id, identity);
        assert!(session.implicit);
        assert_eq!(session.status(), b'E');
    }
}
