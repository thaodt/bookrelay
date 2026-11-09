//! Bounded, public protocol observation. This is not collector/E3 acceptance.
use anyhow::{Context, ensure};
use engineering_challenge::{
    exchange::{ExchangeAdapter, polymarket::Polymarket},
    transport::{ConnectOptions, connect_public},
};
use futures_util::{SinkExt, StreamExt};
use std::{path::PathBuf, time::Duration};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_tungstenite::tungstenite::Message;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    ensure!(
        args.len() == 3,
        "usage: polymarket_wire_probe URL ASSET_ID OUTPUT_DIRECTORY"
    );
    let root = PathBuf::from(&args[2]);
    std::fs::create_dir(&root)?;
    let mut raw = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(root.join("wire.jsonl"))?;
    use std::io::Write;
    let mut connection = connect_public(&args[0], &ConnectOptions::default()).await?;
    let symbols = vec![args[1].clone()];
    let origin = Instant::now();
    let mut frames = 0;
    let mut bytes = 0;
    let mut events = 0;
    let mut snapshots = 0;
    let mut failures = 0;
    let mut phase = 0;
    let mut ping = tokio::time::interval(Duration::from_secs(5));
    ping.set_missed_tick_behavior(MissedTickBehavior::Skip);
    connection
        .socket
        .send(Message::Text(
            Polymarket.subscribe(1, &symbols)?.to_string().into(),
        ))
        .await?;
    loop {
        tokio::select! {
            _=ping.tick()=>connection.socket.send(Message::Text("PING".into())).await?,
            _=tokio::time::sleep_until(origin+Duration::from_secs(if phase==0 {8} else if phase==1 {10} else {18}))=>{
                if phase==2 {break;}
                let add=phase==1;
                let command=Polymarket.update_subscription(2+phase,0,&symbols,add)?;
                connection.socket.send(Message::Text(command.to_string().into())).await?;
                phase+=1;
            },
            incoming=connection.socket.next()=>{
                let received_at=chrono::Utc::now();
                let text=match incoming.context("websocket ended")?? {
                    Message::Text(t)=>t.to_string(),
                    Message::Ping(p)=>{connection.socket.send(Message::Pong(p)).await?;continue;},
                    Message::Pong(_)=>continue,
                    Message::Close(_)=>anyhow::bail!("peer closed probe"),
                    _=>continue,
                };
                frames+=1;bytes+=text.len();
                ensure!(bytes<=1<<20,"probe payload byte cap");
                writeln!(raw,"{}",serde_json::json!({"phase":phase,"received_at":received_at.to_rfc3339(),"elapsed_ms":origin.elapsed().as_millis(),"text":text}))?;
                match Polymarket.decode(&text,received_at) {
                    Ok(frame)=>{
                        snapshots+=frame.events.iter().filter(|e|matches!(e,engineering_challenge::model::BookEvent::Snapshot(_))).count();
                        events+=frame.events.len();
                    },
                    Err(e)=>{failures+=1;eprintln!("decode failure: {e:#}");},
                }
            }
        }
    }
    let close = engineering_challenge::transport::close(&mut connection.socket).await;
    let summary = serde_json::json!({"scope":"bounded public wire probe, not shared collector acceptance","asset_id":symbols[0],"peer":connection.peer,"local":connection.local,"frames":frames,"bytes":bytes,"events":events,"snapshots":snapshots,"decode_failures":failures,"clean_close":close.is_ok(),"elapsed_seconds":origin.elapsed().as_secs_f64()});
    raw.flush()?;
    std::fs::write(
        root.join("summary.json"),
        serde_json::to_vec_pretty(&summary)?,
    )?;
    println!("{summary}");
    ensure!(
        snapshots >= 2 && failures == 0 && close.is_ok(),
        "probe incomplete; preserve raw observations"
    );
    Ok(())
}
