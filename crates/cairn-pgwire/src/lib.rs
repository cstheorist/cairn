//! PostgreSQL v3.0, simple and extended query protocols for the documented SQL subset.
use cairn_common::{Column, DbError, Result};
use cairn_sql::{self as sql, Engine, Output, Plan, Session};
use cairn_verified as core;
use rmpv::Value;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
};

const MAX_FRAME: u32 = 1024 * 1024;
const MAX_OBJECTS: usize = 256;
const MAX_OBJECT_BYTES: usize = 16 * 1024 * 1024;

fn protocol(message: &str) -> DbError {
    DbError::new("08P01", message)
}
fn io(error: impl std::fmt::Display) -> DbError {
    DbError::new("08006", error.to_string())
}

struct Reader<'a> {
    data: &'a [u8],
    position: usize,
}
impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }
    fn bytes(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(length)
            .ok_or_else(|| protocol("field length overflow"))?;
        let result = self
            .data
            .get(self.position..end)
            .ok_or_else(|| protocol("truncated message"))?;
        self.position = end;
        Ok(result)
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.bytes(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.bytes(2)?.try_into().unwrap()))
    }
    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.bytes(4)?.try_into().unwrap()))
    }
    fn string(&mut self) -> Result<&'a str> {
        let length = self.data[self.position..]
            .iter()
            .position(|b| *b == 0)
            .ok_or_else(|| protocol("unterminated string"))?;
        let bytes = self.bytes(length)?;
        self.byte()?;
        std::str::from_utf8(bytes).map_err(|_| DbError::new("22021", "invalid UTF-8"))
    }
    fn end(&self) -> Result<()> {
        if self.position == self.data.len() {
            Ok(())
        } else {
            Err(protocol("trailing message bytes"))
        }
    }
}

fn message(buffer: &mut Vec<u8>, tag: u8, payload: &[u8]) {
    buffer.push(tag);
    buffer.extend_from_slice(&((payload.len() + 4) as u32).to_be_bytes());
    buffer.extend_from_slice(payload);
}
fn cstring(buffer: &mut Vec<u8>, text: &str) {
    buffer.extend(text.bytes().filter(|b| *b != 0));
    buffer.push(0);
}
fn ready(buffer: &mut Vec<u8>, phase: u8) {
    message(buffer, b'Z', &[phase]);
}
fn error(buffer: &mut Vec<u8>, error: &DbError, fatal: bool) {
    let mut payload = vec![b'S'];
    cstring(&mut payload, if fatal { "FATAL" } else { "ERROR" });
    payload.push(b'C');
    cstring(&mut payload, error.code);
    payload.push(b'M');
    cstring(&mut payload, &error.message);
    payload.push(0);
    message(buffer, b'E', &payload);
}

fn row_description(buffer: &mut Vec<u8>, columns: &[Column], formats: &[i16]) {
    if columns.is_empty() {
        message(buffer, b'n', &[]);
        return;
    }
    let mut payload = Vec::new();
    payload.extend_from_slice(&(columns.len() as u16).to_be_bytes());
    for (index, column) in columns.iter().enumerate() {
        cstring(&mut payload, &column.name);
        payload.extend_from_slice(&0i32.to_be_bytes()); // no PostgreSQL catalog relation OID
        payload.extend_from_slice(&0i16.to_be_bytes());
        payload.extend_from_slice(&column.oid().to_be_bytes());
        let size: i16 = match column.kind.as_str() {
            "bigint" => 8,
            "integer" => 4,
            "smallint" => 2,
            "boolean" => 1,
            _ => -1,
        };
        payload.extend_from_slice(&size.to_be_bytes());
        payload.extend_from_slice(&(-1i32).to_be_bytes());
        payload.extend_from_slice(&formats.get(index).copied().unwrap_or(0).to_be_bytes());
    }
    message(buffer, b'T', &payload);
}

fn output(buffer: &mut Vec<u8>, output: &Output, columns: &[Column], formats: &[i16], complete: bool) -> Result<()> {
    if output.tag.is_empty() {
        message(buffer, b'I', &[]);
        return Ok(());
    }
    for row in &output.rows {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(row.len() as u16).to_be_bytes());
        for (index, value) in row.iter().enumerate() {
            if value.is_nil() {
                payload.extend_from_slice(&(-1i32).to_be_bytes());
                continue;
            }
            let bytes = if formats.get(index).copied().unwrap_or(0) == 1 {
                match columns[index].kind.as_str() {
                    "bigint" => value
                        .as_i64()
                        .ok_or_else(|| protocol("invalid stored BIGINT"))?
                        .to_be_bytes()
                        .to_vec(),
                    "integer" => i32::try_from(value.as_i64().ok_or_else(|| protocol("invalid stored INTEGER"))?)
                        .map_err(|_| protocol("stored INTEGER out of range"))?
                        .to_be_bytes()
                        .to_vec(),
                    "smallint" => i16::try_from(value.as_i64().ok_or_else(|| protocol("invalid stored SMALLINT"))?)
                        .map_err(|_| protocol("stored SMALLINT out of range"))?
                        .to_be_bytes()
                        .to_vec(),
                    "boolean" => vec![u8::from(
                        value.as_bool().ok_or_else(|| protocol("invalid stored BOOLEAN"))?,
                    )],
                    _ => value
                        .as_str()
                        .ok_or_else(|| protocol("invalid stored TEXT"))?
                        .as_bytes()
                        .to_vec(),
                }
            } else {
                match value {
                    Value::String(text) => text
                        .as_str()
                        .ok_or_else(|| protocol("invalid stored UTF-8"))?
                        .as_bytes()
                        .to_vec(),
                    Value::Boolean(value) => {
                        if *value {
                            b"t".to_vec()
                        } else {
                            b"f".to_vec()
                        }
                    }
                    _ => value
                        .as_i64()
                        .ok_or_else(|| protocol("invalid stored integer"))?
                        .to_string()
                        .into_bytes(),
                }
            };
            payload.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            payload.extend_from_slice(&bytes);
        }
        message(buffer, b'D', &payload);
    }
    if complete {
        let mut tag = Vec::new();
        cstring(&mut tag, &output.tag);
        message(buffer, b'C', &tag);
    }
    Ok(())
}

async fn body(socket: &mut BufReader<TcpStream>) -> Result<Vec<u8>> {
    let length = socket.read_u32().await.map_err(io)?;
    if !core::valid_frame(length, MAX_FRAME) {
        return Err(protocol("frame length must be 4..1048576 bytes"));
    }
    // The guard above establishes both preconditions of the verified subtraction.
    let mut payload = vec![0; core::payload_length(length, MAX_FRAME) as usize];
    socket.read_exact(&mut payload).await.map_err(io)?;
    Ok(payload)
}

async fn startup(socket: &mut BufReader<TcpStream>, password: &str) -> Result<()> {
    let mut probes = 0;
    let startup = loop {
        let payload = body(socket).await?;
        let mut reader = Reader::new(&payload);
        let version = reader.i32()?;
        if version == 80877103 || version == 80877104 {
            reader.end()?;
            probes += 1;
            if probes > 2 {
                return Err(protocol("too many encryption probes"));
            }
            socket.write_all(b"N").await.map_err(io)?;
            continue;
        }
        if version != 196608 {
            return Err(DbError::new("0A000", "only PostgreSQL wire protocol 3.0 is supported"));
        }
        let mut settings = HashMap::new();
        loop {
            let key = reader.string()?;
            if key.is_empty() {
                break;
            }
            let value = reader.string()?;
            settings.insert(key.to_owned(), value.to_owned());
        }
        reader.end()?;
        break settings;
    };
    if startup.get("user").map(String::as_str) != Some("cairn") || startup.get("database").is_some_and(|s| s != "cairn")
    {
        return Err(DbError::new("28000", "connect as user cairn to database cairn"));
    }
    if startup.get("options").is_some_and(|s| !s.is_empty()) || startup.get("replication").is_some_and(|s| s != "false")
    {
        return Err(DbError::unsupported(
            "startup options and replication connections are not supported",
        ));
    }
    let mut auth = Vec::new();
    message(&mut auth, b'R', &3i32.to_be_bytes());
    socket.write_all(&auth).await.map_err(io)?;
    if socket.read_u8().await.map_err(io)? != b'p' {
        return Err(protocol("expected password message"));
    }
    let payload = body(socket).await?;
    let mut reader = Reader::new(&payload);
    let provided = reader.string()?;
    reader.end()?;
    // Fixed credential comparison without early byte mismatch exits.
    let mut difference = provided.len() ^ password.len();
    for (a, b) in provided.bytes().zip(password.bytes()) {
        difference |= (a ^ b) as usize;
    }
    if difference != 0 {
        return Err(DbError::new("28P01", "password authentication failed"));
    }
    let mut response = Vec::new();
    message(&mut response, b'R', &0i32.to_be_bytes());
    for (key, value) in [
        ("server_version", "16.0"),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("integer_datetimes", "on"),
        ("standard_conforming_strings", "on"),
    ] {
        let mut payload = Vec::new();
        cstring(&mut payload, key);
        cstring(&mut payload, value);
        message(&mut response, b'S', &payload);
    }
    // No BackendKeyData: cancellation is not implemented and must not appear available.
    ready(&mut response, b'I');
    socket.write_all(&response).await.map_err(io)?;
    Ok(())
}

struct Portal {
    plan: Arc<Plan>,
    parameters: Vec<Value>,
    formats: Vec<i16>,
    result: Option<Output>,
    statement: String,
    memory_cost: usize,
}

fn formats(reader: &mut Reader<'_>, count: usize) -> Result<Vec<i16>> {
    let length = reader.u16()? as usize;
    if length != 0 && length != 1 && length != count {
        return Err(protocol("invalid format code count"));
    }
    let mut values = Vec::new();
    for _ in 0..length {
        let value = reader.u16()?;
        if value > 1 {
            return Err(protocol("format code must be text (0) or binary (1)"));
        }
        values.push(value as i16);
    }
    Ok(if length == 1 {
        vec![values[0]; count]
    } else if length == 0 {
        vec![0; count]
    } else {
        values
    })
}

fn binary_scalar<const LENGTH: usize>(bytes: &[u8]) -> Result<[u8; LENGTH]> {
    bytes.try_into().map_err(|_| {
        // PostgreSQL's receiver first consumes the scalar's fixed width, then
        // Bind checks for leftover bytes. Truncation and excess differ in SQLSTATE.
        if bytes.len() < LENGTH {
            protocol("insufficient binary parameter data")
        } else {
            DbError::new("22P03", "incorrect binary parameter length")
        }
    })
}

fn bound_value(bytes: Option<&[u8]>, format: i16, column: &Column) -> Result<Value> {
    let Some(bytes) = bytes else {
        return sql::text_value(None, column);
    };
    if format == 0 || column.kind == "text" {
        let text = std::str::from_utf8(bytes).map_err(|_| DbError::new("22021", "invalid UTF-8 parameter"))?;
        // Bind values are length-delimited. Validate the entire value instead of
        // treating NUL as a terminator; PostgreSQL text cannot contain U+0000.
        if text.contains('\0') {
            return Err(DbError::new("22021", "NUL byte in text parameter"));
        }
        return sql::text_value(Some(text), column);
    }
    match column.kind.as_str() {
        "bigint" => Ok(i64::from_be_bytes(binary_scalar(bytes)?).into()),
        "integer" => Ok((i32::from_be_bytes(binary_scalar(bytes)?) as i64).into()),
        "smallint" => Ok((i16::from_be_bytes(binary_scalar(bytes)?) as i64).into()),
        "boolean" => Ok((binary_scalar::<1>(bytes)?[0] != 0).into()),
        _ => Err(protocol("invalid binary parameter length or value")),
    }
}

struct Frontend<'a> {
    tag: u8,
    payload: &'a [u8],
    followed_by_sync: bool,
}

fn buffered_sync_next(buffer: &[u8]) -> bool {
    // Inspect only bytes already read. Incomplete/unavailable input conservatively
    // takes the transactional path, without delaying Execute or relying on packets.
    buffer.starts_with(&[b'S', 0, 0, 0, 4])
}

async fn process(
    frontend: Frontend<'_>,
    engine: &Engine,
    statements: &mut HashMap<String, Arc<Plan>>,
    portals: &mut HashMap<String, Portal>,
    buffer: &mut Vec<u8>,
    session: &mut Session,
) -> Result<()> {
    let mut reader = Reader::new(frontend.payload);
    match frontend.tag {
        b'Q' => {
            let sql = reader.string()?;
            reader.end()?;
            statements.remove("");
            portals.remove("");
            let plan = engine.prepare(sql).await?;
            if session.transaction.is_none() && plan.needs_simple_transaction() {
                session.begin_implicit()?;
            }
            let result = engine.execute(&plan, &[], session).await?;
            if session.implicit {
                session.finish(&engine.cluster, true).await?;
            }
            deallocate(&plan, statements, portals)?;
            if !plan.columns.is_empty() {
                row_description(buffer, &plan.columns, &[]);
            }
            output(buffer, &result, &plan.columns, &[], true)?;
            if session.transaction.is_none() {
                portals.clear();
            }
            ready(buffer, session.status());
        }
        b'P' => {
            let name = reader.string()?.to_owned();
            let sql = reader.string()?;
            let count = reader.u16()? as usize;
            if count > 128 {
                return Err(protocol("too many parameters"));
            }
            let oids = (0..count).map(|_| reader.i32()).collect::<Result<Vec<_>>>()?;
            reader.end()?;
            if !name.is_empty() && statements.contains_key(&name) {
                return Err(DbError::new("42P05", "prepared statement already exists"));
            }
            if !statements.contains_key(&name) && statements.len() >= MAX_OBJECTS {
                return Err(DbError::new("54000", "too many prepared statements"));
            }
            let mut plan = engine.prepare(sql).await?;
            let used: usize = statements
                .iter()
                .filter(|(key, _)| **key != name)
                .map(|(_, plan)| plan.memory_cost)
                .sum();
            if used.saturating_add(plan.memory_cost) > MAX_OBJECT_BYTES {
                return Err(DbError::new(
                    "54000",
                    "prepared statements exceed the session memory budget",
                ));
            }
            if oids.len() > plan.parameters.len() {
                return Err(protocol("too many declared parameter types"));
            }
            for (oid, column) in oids.iter().zip(&mut plan.parameters) {
                if sql::integral(&column.kind) && [20, 21, 23].contains(oid) {
                    column.kind = match oid {
                        20 => "bigint",
                        21 => "smallint",
                        _ => "integer",
                    }
                    .into();
                }
                if *oid != 0 && *oid != column.oid() {
                    return Err(DbError::new(
                        "42804",
                        "explicit parameter OID differs from inferred type",
                    ));
                }
            }
            if name.is_empty() {
                portals.retain(|_, p| !p.statement.is_empty());
            }
            statements.insert(name, Arc::new(plan));
            message(buffer, b'1', &[]);
        }
        b'B' => {
            let name = reader.string()?.to_owned();
            let statement = reader.string()?.to_owned();
            if !name.is_empty() && portals.contains_key(&name) {
                return Err(DbError::new("42P03", "portal already exists"));
            }
            if !portals.contains_key(&name) && portals.len() >= MAX_OBJECTS {
                return Err(DbError::new("54000", "too many portals"));
            }
            let plan = statements
                .get(&statement)
                .ok_or_else(|| DbError::new("26000", "prepared statement does not exist"))?
                .clone();
            let input_formats = formats(&mut reader, plan.parameters.len())?;
            let count = reader.u16()? as usize;
            if count != plan.parameters.len() {
                return Err(protocol("bind parameter count mismatch"));
            }
            let mut parameters = Vec::new();
            for (index, column) in plan.parameters.iter().enumerate() {
                let length = reader.i32()?;
                if length < -1 {
                    return Err(protocol("invalid parameter length"));
                }
                let value = if length == -1 {
                    None
                } else {
                    Some(reader.bytes(length as usize)?)
                };
                parameters.push(bound_value(value, input_formats[index], column)?);
            }
            let output_formats = formats(&mut reader, plan.columns.len())?;
            reader.end()?;
            let memory_cost = frontend.payload.len().saturating_mul(2);
            let used: usize = portals
                .iter()
                .filter(|(key, _)| **key != name)
                .map(|(_, portal)| portal.memory_cost)
                .sum();
            if used.saturating_add(memory_cost) > MAX_OBJECT_BYTES {
                return Err(DbError::new("54000", "portals exceed the session memory budget"));
            }
            portals.insert(
                name,
                Portal {
                    plan,
                    parameters,
                    formats: output_formats,
                    result: None,
                    statement,
                    memory_cost,
                },
            );
            message(buffer, b'2', &[]);
        }
        b'D' => {
            let kind = reader.byte()?;
            let name = reader.string()?;
            reader.end()?;
            if kind == b'S' {
                let plan = statements
                    .get(name)
                    .ok_or_else(|| DbError::new("26000", "prepared statement does not exist"))?;
                let mut types = Vec::new();
                types.extend_from_slice(&(plan.parameters.len() as u16).to_be_bytes());
                for column in &plan.parameters {
                    types.extend_from_slice(&column.oid().to_be_bytes());
                }
                message(buffer, b't', &types);
                row_description(buffer, &plan.columns, &[]);
            } else if kind == b'P' {
                let portal = portals
                    .get(name)
                    .ok_or_else(|| DbError::new("34000", "portal does not exist"))?;
                row_description(buffer, &portal.plan.columns, &portal.formats);
            } else {
                return Err(protocol("Describe target must be S or P"));
            }
        }
        b'E' => {
            let name = reader.string()?;
            let maximum = reader.i32()?;
            reader.end()?;
            if maximum < 0 {
                return Err(protocol("negative Execute row limit"));
            }
            let other_cost: usize = portals
                .iter()
                .filter(|(key, _)| key.as_str() != name)
                .map(|(_, portal)| portal.memory_cost)
                .sum();
            let portal = portals
                .get_mut(name)
                .ok_or_else(|| DbError::new("34000", "portal does not exist"))?;
            if portal.result.is_none() {
                if session.transaction.is_none() && portal.plan.needs_extended_transaction(frontend.followed_by_sync) {
                    session.begin_implicit()?;
                }
                let result = engine.execute(&portal.plan, &portal.parameters, session).await?;
                let memory = portal.memory_cost.saturating_add(sql::rows_cost(&result.rows));
                if other_cost.saturating_add(memory) > MAX_OBJECT_BYTES {
                    return Err(DbError::new("54000", "portal results exceed the session memory budget"));
                }
                portal.memory_cost = memory;
                portal.result = Some(result);
            }
            let result = portal.result.as_mut().unwrap();
            if maximum > 0 && result.rows.len() > maximum as usize {
                let remaining = result.rows.split_off(maximum as usize);
                let chunk = Output {
                    rows: std::mem::replace(&mut result.rows, remaining),
                    tag: result.tag.clone(),
                };
                output(buffer, &chunk, &portal.plan.columns, &portal.formats, false)?;
                portal.memory_cost = portal.memory_cost.saturating_sub(sql::rows_cost(&chunk.rows));
                message(buffer, b's', &[]);
            } else {
                output(buffer, result, &portal.plan.columns, &portal.formats, true)?;
                portal.memory_cost = portal.memory_cost.saturating_sub(sql::rows_cost(&result.rows));
                // Re-executing an exhausted portal must not replay rows or mutations.
                result.rows.clear();
                if result.tag.starts_with("SELECT ") {
                    result.tag = "SELECT 0".into();
                }
            }
            let plan = portal.plan.clone();
            deallocate(&plan, statements, portals)?;
        }
        b'C' => {
            let kind = reader.byte()?;
            let name = reader.string()?;
            reader.end()?;
            if kind == b'S' {
                statements.remove(name);
                portals.retain(|_, portal| portal.statement != name);
            } else if kind == b'P' {
                portals.remove(name);
            } else {
                return Err(protocol("Close target must be S or P"));
            }
            message(buffer, b'3', &[]);
        }
        b'S' => {
            reader.end()?;
            if session.implicit {
                session.finish(&engine.cluster, !session.failed).await?;
            }
            if session.transaction.is_none() || session.failed {
                portals.clear();
            }
            ready(buffer, session.status());
        }
        b'H' => {
            reader.end()?;
        }
        _ => return Err(DbError::unsupported("frontend message type is not supported")),
    }
    Ok(())
}

fn deallocate(
    plan: &Plan,
    statements: &mut HashMap<String, Arc<Plan>>,
    portals: &mut HashMap<String, Portal>,
) -> Result<()> {
    if let Some(name) = plan.deallocation_target() {
        if name == "all" {
            statements.clear();
            portals.clear();
        } else {
            if statements.remove(name).is_none() {
                return Err(DbError::new("26000", "prepared statement does not exist"));
            }
            portals.retain(|_, portal| portal.statement != name);
        }
    }
    Ok(())
}

pub async fn serve(socket: TcpStream, engine: Arc<Engine>, password: Arc<String>) -> Result<()> {
    let mut socket = BufReader::with_capacity(16 * 1024, socket);
    let handshake = tokio::time::timeout(Duration::from_secs(10), startup(&mut socket, &password))
        .await
        .map_err(|_| protocol("startup timeout"))
        .and_then(|r| r);
    if let Err(failure) = handshake {
        let mut buffer = Vec::new();
        error(&mut buffer, &failure, true);
        socket.write_all(&buffer).await.map_err(io)?;
        return Ok(());
    }
    let mut session = Session::default();
    let result = serve_messages(&mut socket, &engine, &mut session).await;
    session.close(&engine.cluster).await;
    result
}

async fn serve_messages(socket: &mut BufReader<TcpStream>, engine: &Engine, session: &mut Session) -> Result<()> {
    let mut statements = HashMap::new();
    let mut portals = HashMap::new();
    let mut failed = false;
    let log_errors = std::env::var("CAIRN_LOG_ERRORS").as_deref() == Ok("1");
    loop {
        let incoming = tokio::time::timeout(Duration::from_secs(300), async {
            let tag = socket.read_u8().await.map_err(io)?;
            let payload = body(socket).await?;
            Ok::<_, DbError>((tag, payload))
        })
        .await
        .map_err(|_| protocol("idle timeout"))?;
        let (tag, payload) = match incoming {
            Ok(value) => value,
            Err(failure) => {
                if failure.code == "08P01" {
                    let mut buffer = Vec::new();
                    error(&mut buffer, &failure, true);
                    socket.write_all(&buffer).await.map_err(io)?;
                }
                return Ok(());
            }
        };
        if tag == b'X' {
            return Ok(());
        }
        if failed && tag != b'S' {
            continue;
        }
        let mut buffer = Vec::new();
        match process(
            Frontend {
                tag,
                payload: &payload,
                followed_by_sync: buffered_sync_next(socket.buffer()),
            },
            engine,
            &mut statements,
            &mut portals,
            &mut buffer,
            session,
        )
        .await
        {
            Ok(()) => {
                if tag == b'S' {
                    failed = false;
                }
            }
            Err(failure) => {
                if log_errors {
                    eprintln!(
                        "pgwire peer={:?} tag={} state={} sqlstate={}",
                        socket.get_ref().peer_addr().ok(),
                        tag as char,
                        session.status() as char,
                        failure.code
                    );
                }
                session.fail(&engine.cluster).await;
                if (tag == b'S' || tag == b'Q') && session.implicit {
                    session.reset();
                }
                buffer.clear();
                error(&mut buffer, &failure, false);
                if tag == b'Q' || tag == b'S' {
                    portals.clear();
                    ready(&mut buffer, session.status());
                    failed = false;
                } else {
                    failed = true;
                }
            }
        }
        socket.write_all(&buffer).await.map_err(io)?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn buffered_execute_successor_keeps_the_point_fast_path_without_reading_ahead() {
        for suffix_length in 0..=5 {
            let (mut peer, stream) = tokio::io::duplex(64);
            let sync = [b'S', 0, 0, 0, 4];
            // An unnamed Execute frame, followed by an available prefix of Sync.
            let mut input = vec![b'E', 0, 0, 0, 9, 0, 0, 0, 0, 0];
            input.extend_from_slice(&sync[..suffix_length]);
            peer.write_all(&input).await.unwrap();
            let mut socket = BufReader::with_capacity(64, stream);
            assert_eq!(socket.read_u8().await.unwrap(), b'E');
            assert_eq!(socket.read_u32().await.unwrap(), 9);
            let mut execute_payload = [0; 5];
            socket.read_exact(&mut execute_payload).await.unwrap();
            assert_eq!(socket.buffer(), &sync[..suffix_length]);
            assert_eq!(buffered_sync_next(socket.buffer()), suffix_length == sync.len());
            // Classification neither consumes bytes nor waits for a missing suffix.
            assert_eq!(socket.buffer(), &sync[..suffix_length]);
            peer.write_all(&sync[suffix_length..]).await.unwrap();
            assert_eq!(socket.read_u8().await.unwrap(), b'S');
            assert_eq!(socket.read_u32().await.unwrap(), 4);
        }
    }

    #[test]
    fn point_fast_path_requires_a_complete_immediate_empty_sync() {
        let sync = [b'S', 0, 0, 0, 4];
        for length in 0..sync.len() {
            assert!(!buffered_sync_next(&sync[..length]));
        }
        assert!(buffered_sync_next(&sync));
        // Later frames belong to the next cycle, even when already buffered.
        assert!(buffered_sync_next(&[b'S', 0, 0, 0, 4, b'E', 0, 0, 0, 9]));
        for position in 0..sync.len() {
            for byte in 0..=u8::MAX {
                if byte != sync[position] {
                    let mut invalid = sync;
                    invalid[position] = byte;
                    assert!(!buffered_sync_next(&invalid), "{invalid:?}");
                }
            }
        }
        assert!(!buffered_sync_next(&[b'H', 0, 0, 0, 4, b'S', 0, 0, 0, 4]));
    }

    fn column(kind: &str) -> Column {
        Column {
            name: "value".into(),
            kind: kind.into(),
            nullable: true,
        }
    }

    #[test]
    fn text_bind_validates_the_full_parameter_in_both_formats() {
        let column = column("text");
        for format in [0, 1] {
            for bytes in [
                &b"\0"[..],
                &b"prefix\0suffix"[..],
                &b"trailing\0"[..],
                &b"\xff"[..],
                &b"valid\xe9\x9b"[..],
                &b"\xed\xa0\x80"[..],
                &b"\xc0\x80"[..],
            ] {
                assert_eq!(bound_value(Some(bytes), format, &column).unwrap_err().code, "22021");
            }
            for text in ["", "plain", "雪🦀", "\u{1}\u{7f}\u{10ffff}"] {
                assert_eq!(
                    bound_value(Some(text.as_bytes()), format, &column).unwrap(),
                    text.into()
                );
            }
            assert_eq!(bound_value(None, format, &column).unwrap(), Value::Nil);
        }
    }

    #[test]
    fn nul_validation_does_not_reject_binary_integer_zero_bytes() {
        let column = column("bigint");
        assert_eq!(bound_value(Some(&0i64.to_be_bytes()), 1, &column).unwrap(), 0i64.into());
        assert_eq!(bound_value(Some(b"1\0"), 0, &column).unwrap_err().code, "22021");
    }

    #[test]
    fn binary_boolean_treats_every_nonzero_byte_as_true() {
        let column = column("boolean");
        for byte in 0..=u8::MAX {
            assert_eq!(bound_value(Some(&[byte]), 1, &column).unwrap(), Value::from(byte != 0));
        }
    }

    #[test]
    fn binary_scalar_lengths_distinguish_truncated_and_excess_data() {
        for (kind, width) in [("boolean", 1), ("smallint", 2), ("integer", 4), ("bigint", 8)] {
            let column = column(kind);
            for length in 0..=width + 1 {
                let bytes = vec![0; length];
                let result = bound_value(Some(&bytes), 1, &column);
                if length == width {
                    assert!(result.is_ok(), "{kind}: {result:?}");
                } else {
                    assert_eq!(result.unwrap_err().code, if length < width { "08P01" } else { "22P03" });
                }
            }
        }
    }

    #[test]
    fn binary_integers_keep_their_signed_width() {
        for (kind, width, minimum, maximum) in [
            ("smallint", 2, i64::from(i16::MIN), i64::from(i16::MAX)),
            ("integer", 4, i64::from(i32::MIN), i64::from(i32::MAX)),
            ("bigint", 8, i64::MIN, i64::MAX),
        ] {
            let column = column(kind);
            for value in [minimum, -1, 0, 1, maximum] {
                let bytes = value.to_be_bytes();
                assert_eq!(
                    bound_value(Some(&bytes[8 - width..]), 1, &column).unwrap(),
                    Value::from(value)
                );
            }
            assert_eq!(bound_value(None, 1, &column).unwrap(), Value::Nil);
        }
    }
}
