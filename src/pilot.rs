//! Bounded read-only subscription capacity pilot. Rejections are observations,
//! not an invitation to flood the exchange looking for an unpublished limit.
use crate::{
    auth::Credentials,
    book::OrderBook,
    collector::stopped,
    exchange::{
        Control, ExchangeAdapter,
        kalshi::{Kalshi, SequenceTracker},
    },
    model::BookEvent,
    transport::{self, ConnectOptions},
};
use anyhow::{Context, ensure};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use serde_json::json;
use std::{collections::HashMap, time::Duration};
use tokio::{
    sync::watch,
    time::{Instant, MissedTickBehavior},
};
use tokio_tungstenite::tungstenite::Message;

pub struct Options {
    pub ws_url: String,
    pub symbols: Vec<String>,
    pub steps: Vec<usize>,
    pub dwell: Duration,
    pub step_timeout: Duration,
    pub command_gap: Duration,
    pub connect: ConnectOptions,
}
#[derive(Default, Serialize)]
pub struct Step {
    pub requested: usize,
    pub initialized: usize,
    pub acknowledged: bool,
    pub elapsed_ms: u128,
    pub frames: u64,
    pub updates: u64,
    pub bytes: u64,
    pub updates_per_second: f64,
    pub rss_bytes: Option<u64>,
}
#[derive(Default, Serialize)]
pub struct Report {
    pub started_utc: String,
    pub local: String,
    pub peer: String,
    pub steps: Vec<Step>,
    pub largest_verified: usize,
    pub failure: Option<String>,
    pub rejection_code: Option<u64>,
    pub clean_close: bool,
    pub minimum_command_gap_ms: u128,
    pub connection_limit_tested: usize,
}

pub fn rss_bytes() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()?
        .lines()
        .find_map(|l| {
            l.strip_prefix("VmRSS:")
                .and_then(|s| s.split_whitespace().next()?.parse::<u64>().ok())
                .map(|n| n * 1024)
        })
}

pub async fn run(
    options: Options,
    auth: &Credentials,
    mut stop: watch::Receiver<bool>,
) -> anyhow::Result<Report> {
    ensure!(
        !options.steps.is_empty()
            && options.steps.windows(2).all(|p| p[0] > 0 && p[0] < p[1])
            && options.steps[0] > 0
            && *options.steps.last().unwrap() <= options.symbols.len(),
        "invalid pilot steps/symbol inventory"
    );
    ensure!(
        !options.dwell.is_zero()
            && !options.step_timeout.is_zero()
            && options.command_gap >= Duration::from_millis(100),
        "invalid pilot time/rate caps"
    );
    let mut report = Report {
        started_utc: Utc::now().to_rfc3339(),
        minimum_command_gap_ms: options.command_gap.as_millis(),
        connection_limit_tested: 1,
        ..Report::default()
    };
    let mut connection = tokio::select! {_=stopped(&mut stop)=>return Ok(report),r=transport::connect(&options.ws_url,auth,&options.connect)=>r?};
    report.local = connection.local.to_string();
    report.peer = connection.peer.to_string();
    let mut books = HashMap::<String, OrderBook>::new();
    let mut tracker = SequenceTracker::default();
    let mut sid = None;
    let mut ping = tokio::time::interval(Duration::from_secs(10));
    ping.set_missed_tick_behavior(MissedTickBehavior::Skip);
    for (index, target) in options.steps.iter().copied().enumerate() {
        if *stop.borrow() {
            report.failure = Some("cancelled".into());
            break;
        }
        if index > 0 {
            tokio::select! {_=stopped(&mut stop)=>{report.failure=Some("cancelled".into());break;},_=tokio::time::sleep(options.command_gap)=>{}}
        }
        let previous = books.len();
        for s in &options.symbols[previous..target] {
            books.insert(s.clone(), OrderBook::new(s.clone()));
        }
        let id = index as u64 + 1;
        let command = if let Some(sid) = sid {
            json!({"id":id,"cmd":"update_subscription","params":{"sid":sid,"action":"add_markets","market_tickers":&options.symbols[previous..target]}})
        } else {
            Kalshi.subscribe(id, &options.symbols[..target])?
        };
        connection
            .socket
            .send(Message::Text(command.to_string().into()))
            .await?;
        let started = Instant::now();
        let mut ready_at = None;
        let mut step = Step {
            requested: target,
            ..Step::default()
        };
        loop {
            let end = started + options.step_timeout;
            let dwell_end = ready_at.map(|t: Instant| t + options.dwell).unwrap_or(end);
            tokio::select! {
                _=stopped(&mut stop)=>{report.failure=Some("cancelled".into());break;},
                _=tokio::time::sleep_until(dwell_end)=>{
                    if ready_at.is_none(){report.failure=Some("step ACK/initialization timeout".into());}
                    break;
                },
                _=ping.tick()=>connection.socket.send(Message::Ping(vec![].into())).await?,
                message=connection.socket.next()=>{
                    let receive=Utc::now();
                    let message=message.context("pilot websocket ended")??;
                    let text=match message {Message::Text(t)=>t.to_string(),Message::Ping(p)=>{connection.socket.send(Message::Pong(p)).await?;continue;},Message::Pong(_)=>continue,Message::Close(_)=>{report.failure=Some("peer closed pilot".into());break;},_=>continue};
                    step.frames+=1;step.bytes+=text.len() as u64;
                    let frame=Kalshi.decode(&text,receive)?;
                    if let Some((s,q))=frame.sequence && tracker.observe(s,q){report.failure=Some("sequence gap: pilot completeness invalid".into());break;}
                    match frame.control {
                        Control::Subscribed(s) if frame.request_id==Some(id)=>{sid=Some(s);step.acknowledged=true;},
                        Control::Ok if frame.request_id==Some(id)=>step.acknowledged=true,
                        Control::Error {code,message}=>{report.rejection_code=Some(code);report.failure=Some(format!("command rejected ({code}): {message}"));break;},_=>{},
                    }
                    for event in frame.events {
                        let book=books.get_mut(event.symbol()).context("unrequested pilot symbol")?;
                        match event {BookEvent::Snapshot(s)=>book.replace(s)?,BookEvent::Update(u)=>{book.apply(u)?;step.updates+=1;}}
                    }
                    step.initialized=books.values().filter(|b|b.is_valid()).count();
                    if step.acknowledged && step.initialized==target && ready_at.is_none(){ready_at=Some(Instant::now());}
                }
            }
        }
        step.elapsed_ms = started.elapsed().as_millis();
        step.updates_per_second = step.updates as f64 / started.elapsed().as_secs_f64();
        step.rss_bytes = rss_bytes();
        if step.acknowledged && step.initialized == target && report.failure.is_none() {
            report.largest_verified = target;
        }
        eprintln!("capacity step: {}", serde_json::to_string(&step)?);
        report.steps.push(step);
        if report.failure.is_some() {
            break;
        }
    }
    report.clean_close = crate::collector::close(&mut connection.socket)
        .await
        .is_ok();
    Ok(report)
}
