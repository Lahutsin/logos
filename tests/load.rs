use std::env;
use std::time::Instant;

use logos::protocol::{Record, Response};
use logos::sdk::Client;
use rand::{rngs::StdRng, Rng, SeedableRng};

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn load_produce_fetch() -> anyhow::Result<()> {
    // Enable with RK_LOAD_ENABLE=1; defaults target localhost broker.
    if env::var("RK_LOAD_ENABLE").ok().as_deref() != Some("1") {
        eprintln!("skipping load test; set RK_LOAD_ENABLE=1 to run");
        return Ok(());
    }

    let addr = env::var("RK_LOAD_ADDR").unwrap_or_else(|_| "127.0.0.1:9092".to_string());
    let topic = env::var("RK_LOAD_TOPIC").unwrap_or_else(|_| "load".to_string());
    let partition: u32 = env::var("RK_LOAD_PARTITION")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let batch = env_usize("RK_LOAD_BATCH", 100);
    let payload_bytes = env_usize("RK_LOAD_PAYLOAD", 256);
    let workers = env_usize("RK_LOAD_WORKERS", 4);

    let mut rng = StdRng::seed_from_u64(42);
    let payload: Vec<u8> = (0..payload_bytes).map(|_| rng.gen()).collect();

    let start = Instant::now();
    let mut tasks = Vec::new();
    for w in 0..workers {
        let mut client = Client::connect(&addr).await?;
        let payload = payload.clone();
        let topic = topic.clone();
        tasks.push(tokio::spawn(async move {
            for i in 0..batch {
                let rec = Record {
                    key: format!("w{w}-k{i}").into_bytes(),
                    value: payload.clone(),
                    timestamp: 0,
                };
                match client.produce(&topic, partition, vec![rec], None).await? {
                    Response::Produced { .. } => {}
                    other => anyhow::bail!("unexpected response: {:?}", other),
                }
            }
            anyhow::Ok(())
        }));
    }

    for t in tasks {
        t.await??;
    }
    let elapsed = start.elapsed();

    // Fetch back a slice to ensure reads still work.
    let mut client = Client::connect(&addr).await?;
    let fetch_res = client
        .fetch(
            &topic,
            partition,
            0,
            (payload_bytes * batch * workers).min(2_000_000) as u32,
            None,
        )
        .await?;
    match fetch_res {
        Response::Fetched { records } => {
            assert!(!records.is_empty(), "no records fetched");
        }
        other => anyhow::bail!("unexpected fetch response: {:?}", other),
    }

    println!(
        "load test: workers={} batch={} payload={}B elapsed_ms={}",
        workers,
        batch,
        payload_bytes,
        elapsed.as_millis()
    );

    Ok(())
}
