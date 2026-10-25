use super::*;
use crate::tests::{error_body, read_request, response, write_response};
use tokio::net::TcpListener;

async fn connected(sync: u64) -> (Pipeline, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let socket = TcpStream::connect(listener.local_addr().unwrap()).await.unwrap();
    let (server, _) = listener.accept().await.unwrap();
    let connection = Connection {
        socket: tokio::io::BufReader::new(socket),
        sync,
    };
    (Pipeline::start(connection, Arc::new(AtomicU64::new(0)), 0), server)
}

async fn pooled() -> (Cluster, TcpStream, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let socket = TcpStream::connect(&address).await.unwrap();
    let (server, _) = listener.accept().await.unwrap();
    let mut cluster = Cluster::new(vec![address.clone()], "unused".into(), "unused".into()).unwrap();
    cluster.pipeline = true;
    let pool = &mut cluster.shards[0];
    pool.leader = Mutex::new(Some(Leader {
        address,
        term: 1,
        incarnation: 1,
        generation: 0,
    }));
    pool.pipeline_slots = vec![Mutex::new(Some(Pipeline::start(
        Connection {
            socket: tokio::io::BufReader::new(socket),
            sync: 1,
        },
        pool.generation.clone(),
        0,
    )))];
    (cluster, server, listener)
}

async fn authenticate(server: &mut TcpStream) {
    let mut greeting = [b' '; 128];
    greeting[64..108].copy_from_slice(STANDARD.encode([1u8; 32]).as_bytes());
    server.write_all(&greeting).await.unwrap();
    let packet = read_packet(server).await.unwrap();
    let (header, body) = response_parts(&packet).unwrap();
    assert_eq!(response_field(&header, 0).unwrap(), 7);
    assert_eq!(field(&body, 0x23).and_then(Value::as_str), Some("unused"));
    write_response(
        server,
        &response(response_field(&header, 1).unwrap(), 0, Value::Map(vec![])),
    )
    .await;
}

fn enqueue(connection: &Pipeline, value: i64) -> oneshot::Receiver<Result<Vec<Value>>> {
    connection
        .enqueue("cairn.tx_mutate", vec![value.into()], Instant::now() + TIMEOUT)
        .unwrap()
}

fn success(sync: u64, value: i64) -> Vec<u8> {
    response(
        sync,
        0,
        Value::Map(vec![(0x30.into(), Value::Array(vec![value.into()]))]),
    )
}

async fn result(receiver: oneshot::Receiver<Result<Vec<Value>>>) -> Result<Vec<Value>> {
    tokio::time::timeout(Duration::from_secs(2), receiver)
        .await
        .unwrap()
        .unwrap()
}

async fn closed(server: &mut TcpStream) {
    let mut byte = [0];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), server.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn dropping_last_idle_owner_closes_peer_without_a_worker_cycle() {
    let (connection, mut server) = connected(0).await;
    let last_owner = connection.clone();
    drop(connection);
    assert!(last_owner.usable());
    // Reader, writer and watchdog are idle; their Shared Arcs must not retain
    // the last client owner or prevent its drop from closing both socket halves.
    drop(last_owner);
    closed(&mut server).await;
}

#[tokio::test]
async fn out_of_order_replies_have_exact_owners() {
    let (connection, mut server) = connected(1).await;
    let replies = [
        enqueue(&connection, 10),
        enqueue(&connection, 20),
        enqueue(&connection, 30),
    ];
    let mut requests = Vec::new();
    for expected in [10, 20, 30] {
        let (sync, body) = read_request(&mut server).await;
        assert_eq!(field(&body, 0x21).unwrap(), &Value::Array(vec![expected.into()]));
        requests.push(sync);
    }
    assert_eq!(requests, [2, 3, 4]); // Auth's SYNC is never reused.
    for index in (0..3).rev() {
        write_response(&mut server, &success(requests[index], (index as i64 + 1) * 10)).await;
    }
    for (reply, expected) in replies.into_iter().zip([10, 20, 30]) {
        assert_eq!(result(reply).await.unwrap(), vec![Value::from(expected)]);
    }
    assert!(connection.usable());
    assert_eq!(connection.inner.shared.state().bytes, 0);
}

#[tokio::test]
async fn cancelled_owner_and_queue_arrival_do_not_cancel_partial_reads() {
    for split in [1, 3, 5, 7] {
        let (connection, mut server) = connected(0).await;
        let abandoned = enqueue(&connection, 10);
        let (first, _) = read_request(&mut server).await;
        drop(abandoned);
        let body = success(first, 10);
        let mut packet = vec![0xce];
        packet.extend_from_slice(&(body.len() as u32).to_be_bytes());
        packet.extend_from_slice(&body);
        server.write_all(&packet[..split]).await.unwrap();
        tokio::task::yield_now().await;
        let second = enqueue(&connection, 20);
        let (second_sync, _) = read_request(&mut server).await;
        assert_eq!(connection.inner.shared.state().pending.len(), 2);
        server.write_all(&packet[split..]).await.unwrap();
        write_response(&mut server, &success(second_sync, 20)).await;
        assert_eq!(result(second).await.unwrap(), vec![Value::from(20)]);
        let third = enqueue(&connection, 30);
        let (third_sync, _) = read_request(&mut server).await;
        assert_eq!(third_sync, second_sync + 1);
        write_response(&mut server, &success(third_sync, 30)).await;
        assert_eq!(result(third).await.unwrap(), vec![Value::from(30)]);
        assert!(connection.usable());
    }
}

#[tokio::test]
async fn abandoned_deadline_retires_every_pending_request_without_replay() {
    let (connection, mut server) = connected(0).await;
    let abandoned = connection
        .enqueue("cairn.tx_mutate", vec![], Instant::now() + Duration::from_millis(200))
        .unwrap();
    let second = enqueue(&connection, 20);
    read_request(&mut server).await;
    read_request(&mut server).await;
    drop(abandoned);
    assert_eq!(result(second).await.unwrap_err().code, "08006");
    closed(&mut server).await;
    assert!(!connection.usable());
    assert_eq!(connection.inner.shared.generation.load(Ordering::Acquire), 1);
    assert_eq!(
        connection
            .enqueue("cairn.tx_mutate", vec![], Instant::now() + TIMEOUT)
            .unwrap_err()
            .code,
        "08006"
    );
}

#[tokio::test]
async fn eof_retires_all_pending_requests() {
    let (connection, mut server) = connected(0).await;
    let first = enqueue(&connection, 10);
    let second = enqueue(&connection, 20);
    read_request(&mut server).await;
    read_request(&mut server).await;
    drop(server);
    assert_eq!(result(first).await.unwrap_err().code, "08006");
    assert_eq!(result(second).await.unwrap_err().code, "08006");
    assert!(!connection.usable());
}

#[tokio::test]
async fn malformed_unknown_and_nonfinal_frames_retire_all_owners() {
    for case in 0..11 {
        let (connection, mut server) = connected(0).await;
        let first = enqueue(&connection, 10);
        let second = enqueue(&connection, 20);
        let (sync, _) = read_request(&mut server).await;
        read_request(&mut server).await;
        match case {
            0 => {
                write_response(
                    &mut server,
                    &response(sync + 100, 0x8001, error_body("[CAIRN:FENCED] foreign")),
                )
                .await
            }
            1 => write_response(&mut server, &response(sync, 0x80, error_body("[CAIRN:FENCED] push"))).await,
            2 => {
                let mut body = success(sync, 10);
                body.push(0xc0);
                write_response(&mut server, &body).await;
            }
            3 => {
                write_response(
                    &mut server,
                    &response(sync, 0x10000, error_body("[CAIRN:FENCED] unknown type")),
                )
                .await
            }
            4 => write_response(&mut server, &[0x82, 0, 0, 1, 1, 0x90]).await, // Non-map body.
            5 => write_response(&mut server, &[0x83, 0, 0, 1, 1, 1, 1, 0x80]).await, // Duplicate SYNC.
            6 => {
                server.write_all(&[0xce, 0, 0x80, 0, 1]).await.unwrap();
            }
            7 => {
                server.write_all(&[0xc0]).await.unwrap();
            }
            8 => {
                server.write_all(&[0xce, 0, 0, 0, 8, 0x82]).await.unwrap();
                server.shutdown().await.unwrap();
            }
            9 => {
                write_response(
                    &mut server,
                    &response(sync, 0, Value::Map(vec![(0x30.into(), true.into())])),
                )
                .await
            }
            _ => {
                server.write_all(&[0xce, 0]).await.unwrap();
                server.shutdown().await.unwrap();
            }
        }
        assert_eq!(result(first).await.unwrap_err().code, "08006", "case {case}");
        assert_eq!(result(second).await.unwrap_err().code, "08006", "case {case}");
        closed(&mut server).await;
        assert_eq!(connection.inner.shared.generation.load(Ordering::Acquire), 1);
    }
}

#[tokio::test]
async fn duplicate_completed_sync_cannot_complete_or_fence_another_owner() {
    let (connection, mut server) = connected(0).await;
    let first = enqueue(&connection, 10);
    let second = enqueue(&connection, 20);
    let (sync, _) = read_request(&mut server).await;
    read_request(&mut server).await;
    write_response(&mut server, &success(sync, 10)).await;
    assert_eq!(result(first).await.unwrap(), vec![Value::from(10)]);
    write_response(
        &mut server,
        &response(sync, 0x8001, error_body("[CAIRN:FENCED] duplicate")),
    )
    .await;
    assert_eq!(result(second).await.unwrap_err().code, "08006");
    assert_eq!(connection.inner.shared.generation.load(Ordering::Acquire), 1);
    closed(&mut server).await;
}

#[tokio::test]
async fn only_the_exact_matched_final_fence_can_be_retried() {
    for cancelled in [false, true] {
        let (connection, mut server) = connected(0).await;
        let first = enqueue(&connection, 10);
        let second = enqueue(&connection, 20);
        read_request(&mut server).await;
        let (sync, _) = read_request(&mut server).await;
        let second = if cancelled {
            drop(second);
            None
        } else {
            Some(second)
        };
        write_response(
            &mut server,
            &response(sync, 0x8001, error_body("[CAIRN:FENCED] old epoch")),
        )
        .await;
        assert_eq!(result(first).await.unwrap_err().code, "08006");
        if let Some(second) = second {
            assert_eq!(result(second).await.unwrap_err().code, "57P03");
        }
        assert_eq!(connection.inner.shared.generation.load(Ordering::Acquire), 1);
        assert!(!connection.usable());
        closed(&mut server).await;
    }
}

#[tokio::test]
async fn ordinary_terminal_errors_preserve_other_owners_and_stream_reuse() {
    let (connection, mut server) = connected(0).await;
    let first = enqueue(&connection, 10);
    let second = enqueue(&connection, 20);
    let (first_sync, _) = read_request(&mut server).await;
    let (second_sync, _) = read_request(&mut server).await;
    write_response(&mut server, &success(second_sync, 20)).await;
    write_response(
        &mut server,
        &response(first_sync, 0x8061, error_body("[CAIRN:CONFLICT] locked")),
    )
    .await;
    assert_eq!(result(first).await.unwrap_err().code, "40001");
    assert_eq!(result(second).await.unwrap(), vec![Value::from(20)]);
    let third = enqueue(&connection, 30);
    let (sync, _) = read_request(&mut server).await;
    write_response(&mut server, &success(sync, 30)).await;
    assert_eq!(result(third).await.unwrap(), vec![Value::from(30)]);
    assert_eq!(connection.inner.shared.generation.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn count_admission_retains_cancelled_tickets_until_drain() {
    let (connection, mut server) = connected(0).await;
    for _ in 0..MAX_REQUESTS {
        drop(enqueue(&connection, 10));
    }
    assert_eq!(connection.inner.shared.state().pending.len(), MAX_REQUESTS);
    assert_eq!(
        connection
            .enqueue("cairn.tx_mutate", vec![], Instant::now() + TIMEOUT)
            .unwrap_err()
            .code,
        "54000"
    );
    let (sync, _) = read_request(&mut server).await;
    write_response(&mut server, &success(sync, 10)).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while connection.inner.shared.state().pending.len() == MAX_REQUESTS {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(enqueue(&connection, 20));
    assert_eq!(connection.inner.shared.state().pending.len(), MAX_REQUESTS);
}

#[tokio::test]
async fn byte_admission_and_packet_limits_fail_before_send_without_retirement() {
    let (connection, _server) = connected(0).await;
    let arguments = || vec![Value::from("x".repeat(6 * 1024 * 1024))];
    drop(
        connection
            .enqueue("cairn.tx_mutate", arguments(), Instant::now() + TIMEOUT)
            .unwrap(),
    );
    drop(
        connection
            .enqueue("cairn.tx_mutate", arguments(), Instant::now() + TIMEOUT)
            .unwrap(),
    );
    let before = connection.inner.shared.state().bytes;
    assert_eq!(
        connection
            .enqueue("cairn.tx_mutate", arguments(), Instant::now() + TIMEOUT)
            .unwrap_err()
            .code,
        "54000"
    );
    assert_eq!(
        connection
            .enqueue(
                "cairn.tx_mutate",
                vec![Value::from("x".repeat(MAX_PACKET as usize))],
                Instant::now() + TIMEOUT
            )
            .unwrap_err()
            .code,
        "54000"
    );
    let state = connection.inner.shared.state();
    assert_eq!(state.pending.len(), 2);
    assert_eq!(state.bytes, before);
    assert!(state.bytes <= MAX_REQUEST_BYTES);
    assert!(!state.closed);
}

#[tokio::test]
async fn sync_exhaustion_and_old_generation_retirement_cannot_wrap_or_invalidate_new_work() {
    let (connection, mut server) = connected(u64::MAX - 1).await;
    let last = enqueue(&connection, 10);
    assert_eq!(
        connection
            .enqueue("cairn.tx_mutate", vec![], Instant::now() + TIMEOUT)
            .unwrap_err()
            .code,
        "08006"
    );
    assert_eq!(result(last).await.unwrap_err().code, "08006");
    closed(&mut server).await;
    let (old, _server) = connected(0).await;
    old.inner.shared.generation.store(3, Ordering::Release);
    assert!(!old.usable());
    assert_eq!(old.inner.shared.generation.load(Ordering::Acquire), 3);
}

#[tokio::test]
async fn cluster_never_replays_a_transport_uncertain_mutation() {
    let (cluster, mut server, listener) = pooled().await;
    let peer = tokio::spawn(async move {
        let (sync, body) = read_request(&mut server).await;
        assert_eq!(field(&body, 0x22).and_then(Value::as_str), Some("cairn.tx_mutate"));
        assert_eq!(field(&body, 0x21).unwrap(), &Value::Array(vec![10.into(), 1.into()]));
        // This text is not a pre-execution fence because the frame is a push.
        write_response(&mut server, &response(sync, 0x80, error_body("[CAIRN:FENCED] pushed"))).await;
        closed(&mut server).await;
        assert!(tokio::time::timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err());
    });
    assert_eq!(
        cluster
            .call(0, "cairn.tx_mutate", vec![10.into()])
            .await
            .unwrap_err()
            .code,
        "08006"
    );
    assert_eq!(cluster.shards[0].generation.load(Ordering::Acquire), 1);
    assert!(!cluster.shards[0].pipeline_slots[0]
        .lock()
        .await
        .as_ref()
        .unwrap()
        .usable());
    peer.await.unwrap();
}

#[tokio::test]
async fn cluster_retries_a_matched_preexecution_fence_once_at_the_new_epoch() {
    let (cluster, mut first, listener) = pooled().await;
    let peer = tokio::spawn(async move {
        let (sync, body) = read_request(&mut first).await;
        assert_eq!(field(&body, 0x21).unwrap(), &Value::Array(vec![10.into(), 1.into()]));
        write_response(
            &mut first,
            &response(sync, 0x8001, error_body("[CAIRN:FENCED] old epoch")),
        )
        .await;
        closed(&mut first).await;
        let (mut probe, _) = listener.accept().await.unwrap();
        authenticate(&mut probe).await;
        let (sync, body) = read_request(&mut probe).await;
        assert_eq!(field(&body, 0x22).and_then(Value::as_str), Some("cairn.ping"));
        let data = Value::Array(vec![
            0.into(),
            1.into(),
            2.into(),
            true.into(),
            true.into(),
            2.into(),
            "uuid".into(),
            1.into(),
        ]);
        write_response(&mut probe, &response(sync, 0, Value::Map(vec![(0x30.into(), data)]))).await;
        drop(probe);
        let (mut retried, _) = listener.accept().await.unwrap();
        authenticate(&mut retried).await;
        let (sync, body) = read_request(&mut retried).await;
        assert_eq!(field(&body, 0x22).and_then(Value::as_str), Some("cairn.tx_mutate"));
        assert_eq!(field(&body, 0x21).unwrap(), &Value::Array(vec![10.into(), 2.into()]));
        write_response(
            &mut retried,
            &response(sync, 0x8001, error_body("[CAIRN:FENCED] changed again")),
        )
        .await;
        closed(&mut retried).await;
        // A second explicit fence is returned; it cannot start a third attempt.
        assert!(tokio::time::timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err());
    });
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(2),
            cluster.call(0, "cairn.tx_mutate", vec![10.into()])
        )
        .await
        .unwrap()
        .unwrap_err()
        .code,
        "57P03"
    );
    assert_eq!(cluster.shards[0].generation.load(Ordering::Acquire), 2);
    peer.await.unwrap();
}
