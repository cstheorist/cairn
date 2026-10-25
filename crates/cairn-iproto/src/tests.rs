use super::*;
use tokio::net::TcpListener;

pub(super) fn response(sync: u64, status: u64, body: Value) -> Vec<u8> {
    encode(
        Value::Map(vec![(0.into(), status.into()), (1.into(), sync.into())]),
        body,
    )
}

fn encode(header: Value, body: Value) -> Vec<u8> {
    let mut bytes = Vec::new();
    rmpv::encode::write_value(&mut bytes, &header).unwrap();
    rmpv::encode::write_value(&mut bytes, &body).unwrap();
    bytes
}

pub(super) fn error_body(message: &str) -> Value {
    Value::Map(vec![(0x31.into(), message.into())])
}

#[test]
fn only_final_responses_can_become_sql_errors_or_fences() {
    for status in [0x40, 0x80, 0x81, 0x7fff, 0x10000, 0x18003, u64::MAX] {
        for message in ["[CAIRN:FENCED] old epoch", "[CAIRN:CONFLICT] locked", "push"] {
            let error = decode_response(&response(7, status, error_body(message)), 7).unwrap_err();
            assert_eq!(error.code, "08006", "status={status:#x}, message={message}");
        }
    }
    for (status, message, expected) in [
        (0x8001, "[CAIRN:FENCED] old epoch", "57P03"),
        (0x8061, "Transaction has been aborted by conflict", "40001"),
        (0x8001, "[CAIRN:CONFLICT] locked", "40001"),
        (0x8003, "duplicate key", "23505"),
        (0x8001, "unknown procedure", "XX000"),
    ] {
        assert_eq!(
            decode_response(&response(7, status, error_body(message)), 7)
                .unwrap_err()
                .code,
            expected
        );
    }
}

#[test]
fn ambiguous_or_malformed_response_frames_are_transport_failures() {
    let empty = Value::Map(vec![]);
    let valid_header = Value::Map(vec![(0.into(), 0.into()), (1.into(), 7.into())]);
    let malformed_headers = [
        Value::Array(vec![]),
        empty.clone(),
        Value::Map(vec![(0.into(), 0.into()), (1.into(), 8.into())]),
        Value::Map(vec![(0.into(), 0.into()), (1.into(), "7".into())]),
        Value::Map(vec![(0.into(), 0.into()), (1.into(), 7.into()), (1.into(), 8.into())]),
        Value::Map(vec![(0.into(), 0.into()), (1.into(), 7.into()), (1.into(), 7.into())]),
        Value::Map(vec![
            (0.into(), 0.into()),
            (0.into(), 0x8001.into()),
            (1.into(), 7.into()),
        ]),
    ];
    for header in malformed_headers {
        assert_eq!(
            decode_response(&encode(header, empty.clone()), 7).unwrap_err().code,
            "08006"
        );
    }
    assert_eq!(
        decode_response(&encode(valid_header, Value::Array(vec![])), 7)
            .unwrap_err()
            .code,
        "08006"
    );
    let mut trailing = response(7, 0x8001, error_body("[CAIRN:FENCED] old epoch"));
    trailing.push(0xc0);
    assert_eq!(decode_response(&trailing, 7).unwrap_err().code, "08006");
    let valid = response(7, 0, Value::Map(vec![(0x30.into(), Value::Array(vec![42.into()]))]));
    for length in 0..valid.len() {
        assert_eq!(decode_response(&valid[..length], 7).unwrap_err().code, "08006");
    }
    assert_eq!(decode_response(&valid, 7).unwrap(), Value::Array(vec![42.into()]));
}

#[test]
fn data_extraction_preserves_first_field_and_terminal_error_precedence() {
    let body = Value::Map(vec![
        (0x30.into(), Value::Array(vec![42.into()])),
        (0x30.into(), Value::Array(vec![99.into()])),
        (0x31.into(), "[CAIRN:CONFLICT] locked".into()),
    ]);
    assert_eq!(
        decode_response(&response(7, 0, body.clone()), 7).unwrap(),
        Value::Array(vec![42.into()])
    );
    assert_eq!(
        decode_response(&response(7, 0x8001, body), 7).unwrap_err().code,
        "40001"
    );
    assert_eq!(
        decode_response(&response(7, 0, Value::Map(vec![])), 7).unwrap(),
        Value::Array(vec![])
    );
}

async fn connected_cluster() -> (Cluster, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let socket = TcpStream::connect(&address).await.unwrap();
    let (server, _) = listener.accept().await.unwrap();
    let mut cluster = Cluster::new(vec![address.clone()], "unused".into(), "unused".into()).unwrap();
    cluster.pipeline = false;
    // Install the equivalent of a successfully authenticated pooled connection.
    let pool = &mut cluster.shards[0];
    pool.leader = Mutex::new(Some(Leader {
        address,
        term: 1,
        incarnation: 1,
        generation: 0,
    }));
    pool.slots = vec![Mutex::new(Some((
        0,
        Connection {
            socket: tokio::io::BufReader::new(socket),
            sync: 0,
        },
    )))];
    (cluster, server)
}

pub(super) async fn read_request(socket: &mut TcpStream) -> (u64, Value) {
    assert_eq!(socket.read_u8().await.unwrap(), 0xce);
    let length = socket.read_u32().await.unwrap();
    let mut bytes = vec![0; length as usize];
    socket.read_exact(&mut bytes).await.unwrap();
    let mut cursor = Cursor::new(bytes);
    let header = rmpv::decode::read_value(&mut cursor).unwrap();
    let body = rmpv::decode::read_value(&mut cursor).unwrap();
    assert_eq!(field(&header, 0).and_then(Value::as_u64), Some(10));
    (field(&header, 1).and_then(Value::as_u64).unwrap(), body)
}

pub(super) async fn write_response(socket: &mut TcpStream, bytes: &[u8]) {
    socket.write_u8(0xce).await.unwrap();
    socket.write_u32(bytes.len() as u32).await.unwrap();
    socket.write_all(bytes).await.unwrap();
}

#[tokio::test]
async fn a_push_retires_the_pool_without_replaying_the_mutation() {
    let (cluster, mut socket) = connected_cluster().await;
    let server = tokio::spawn(async move {
        let (sync, body) = read_request(&mut socket).await;
        assert_eq!(field(&body, 0x22).and_then(Value::as_str), Some("cairn.tx_mutate"));
        // A pushed string that looks like a pre-execution fence is not one.
        let bytes = response(sync, 0x80, error_body("[CAIRN:FENCED] pushed text"));
        write_response(&mut socket, &bytes).await;
        // The client must close, even while the real final response is pending.
        let mut next = [0];
        assert_eq!(socket.read(&mut next).await.unwrap(), 0);
    });
    let error = tokio::time::timeout(Duration::from_secs(2), cluster.call(0, "cairn.tx_mutate", vec![]))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code, "08006");
    assert!(error.message.contains("nonfinal"));
    assert_eq!(cluster.shards[0].generation.load(Ordering::Acquire), 1);
    assert!(cluster.shards[0].slots[0].lock().await.is_none());
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn a_matched_final_conflict_keeps_the_stream_aligned() {
    let (cluster, mut socket) = connected_cluster().await;
    let server = tokio::spawn(async move {
        let (first, _) = read_request(&mut socket).await;
        write_response(
            &mut socket,
            &response(first, 0x8061, error_body("[CAIRN:CONFLICT] locked")),
        )
        .await;
        let (second, _) = read_request(&mut socket).await;
        assert_eq!(second, first + 1);
        write_response(
            &mut socket,
            &response(
                second,
                0,
                Value::Map(vec![(0x30.into(), Value::Array(vec![42.into()]))]),
            ),
        )
        .await;
    });
    assert_eq!(
        cluster.call(0, "cairn.tx_mutate", vec![]).await.unwrap_err().code,
        "40001"
    );
    assert!(cluster.shards[0].slots[0].lock().await.is_some());
    assert_eq!(
        cluster.call(0, "cairn.get", vec![]).await.unwrap(),
        vec![Value::from(42)]
    );
    assert_eq!(cluster.shards[0].generation.load(Ordering::Acquire), 0);
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}
