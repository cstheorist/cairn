use cairn_iproto::Cluster;
use cairn_pgwire as pgwire;
use cairn_sql::Engine;
use std::{env, sync::Arc};
use tokio::{net::TcpListener, sync::Semaphore};

fn main() -> anyhow::Result<()> {
    let mut arguments = env::args().skip(1);
    if arguments.next().as_deref() == Some("--healthcheck") {
        let address = arguments.next().unwrap_or_else(|| "127.0.0.1:5432".into()).parse()?;
        std::net::TcpStream::connect_timeout(&address, std::time::Duration::from_secs(2))?;
        return Ok(());
    }
    run_server()
}

#[tokio::main]
async fn run_server() -> anyhow::Result<()> {
    let listen = env::var("CAIRN_LISTEN").unwrap_or_else(|_| "127.0.0.1:55432".into());
    let addresses = env::var("CAIRN_SHARDS").unwrap_or_else(|_| "127.0.0.1:3301,127.0.0.1:3302".into());
    let user = env::var("CAIRN_STORAGE_USER").unwrap_or_else(|_| "gateway".into());
    let storage_password = env::var("CAIRN_STORAGE_PASSWORD")?;
    let password = Arc::new(env::var("CAIRN_PASSWORD")?);
    let cluster = Cluster::new(
        addresses.split(',').map(str::to_owned).collect(),
        user,
        storage_password,
    )?;
    let startup_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        match cluster.check_topology().await {
            Ok(()) => break,
            Err(error) if matches!(error.code, "08006" | "57P03") && tokio::time::Instant::now() < startup_deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Err(error) => return Err(error.into()),
        }
    }
    let engine = Arc::new(Engine::new(cluster));
    if engine.cluster.profiling_enabled() {
        let profile = Arc::downgrade(&engine.cluster);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                let Some(cluster) = profile.upgrade() else {
                    break;
                };
                cluster.report_rpc_profile();
            }
        });
    }
    if env::var("CAIRN_RECOVERY").as_deref() != Ok("0") {
        let recovery = engine.clone();
        tokio::spawn(async move {
            let mut failed = false;
            loop {
                match recovery.cluster.recover_transactions().await {
                    Ok(_) => {
                        failed = false;
                    }
                    Err(error) if !failed => {
                        eprintln!("transaction recovery waiting: {error}");
                        failed = true;
                    }
                    Err(_) => {}
                }
                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
            }
        });
    }
    let listener = TcpListener::bind(&listen).await?;
    let sessions = Arc::new(Semaphore::new(256));
    eprintln!("cairn 0.1 listening on {listen}; static sharding; development protocol server");
    loop {
        let permit = sessions.clone().acquire_owned().await?;
        let (socket, _) = listener.accept().await?;
        socket.set_nodelay(true)?;
        let engine = engine.clone();
        let password = password.clone();
        tokio::spawn(async move {
            let _permit = permit;
            if let Err(error) = pgwire::serve(socket, engine, password).await {
                eprintln!("session ended: {error}");
            }
        });
    }
}
