//! CLI selection reaches localhost REST/WS without consulting native credentials.
use engineering_challenge::replay;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::{
    process::{Command, Output, Stdio},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::Message;

const ASSET: &str = "340282366920938463463374607431768211456";

fn cli() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_engineering-challenge"));
    for name in [
        "KALSHI_KEY_ID",
        "KALSHI_KEY_ID_FILE",
        "KALSHI_PRIVATE_KEY_PATH",
    ] {
        command.env_remove(name);
    }
    command
}

async fn bounded_output(mut command: Command) -> Output {
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while child.try_wait().unwrap().is_none() {
        if tokio::time::Instant::now() >= deadline {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!(
                "CLI did not stop within 15s: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    child.wait_with_output().unwrap()
}

#[tokio::test]
async fn default_native_collection_still_requires_credentials_and_obeys_socket_budget() {
    let output = bounded_output(cli()).await;
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("set KALSHI_KEY_ID"));

    let temp = tempfile::tempdir().unwrap();
    let mut unreadable_id = cli();
    unreadable_id
        .env("KALSHI_KEY_ID_FILE", temp.path())
        .env("KALSHI_PRIVATE_KEY_PATH", temp.path());
    let output = bounded_output(unreadable_id).await;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("cannot read Key ID file"));

    let mut unreadable_key = cli();
    unreadable_key
        .env("KALSHI_KEY_ID", "test-id")
        .env("KALSHI_PRIVATE_KEY_PATH", temp.path());
    let output = bounded_output(unreadable_key).await;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("cannot read private-key file"));

    let mut over_budget = cli();
    over_budget.args(["collect", "--max-connections", "197"]);
    let output = bounded_output(over_budget).await;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("quota headroom at 196"));
}

#[tokio::test]
// The handshake callback's error response type is fixed by tungstenite's API.
#[allow(clippy::result_large_err)]
async fn public_collection_uses_endpoint_overrides_and_ignores_native_credential_files() {
    // A directory is guaranteed to fail read_to_string even when tests run as
    // root. Cover absent native variables and variables naming unusable files.
    for unusable_native_files in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let rest = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let rest_url = format!("http://{}", rest.local_addr().unwrap());
        let ws = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_url = format!("ws://{}/test-public", ws.local_addr().unwrap());
        let rest_server = tokio::spawn(async move {
            let (mut stream, _) = rest.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                assert!(request.len() < 16 << 10);
                request.push(stream.read_u8().await.unwrap());
            }
            let request = String::from_utf8(request).unwrap();
            assert!(request.starts_with("GET /markets/keyset?"), "{request}");
            assert!(request.contains("version=v1"), "{request}");
            assert!(request.contains("closed=any"), "{request}");
            assert!(
                request.contains(&format!("clob_token_ids={ASSET}")),
                "{request}"
            );
            assert!(!request.to_ascii_lowercase().contains("kalshi-access"));
            let body = json!({
                "markets": [{
                    "id": "market-1", "conditionId": "condition-1", "version": "v1",
                    "active": true, "closed": false, "acceptingOrders": true,
                    "enableOrderBook": true, "outcomes": "[\"Yes\",\"No\"]",
                    "clobTokenIds": format!("[\"{ASSET}\",\"456\"]")
                }],
                "next_cursor": null
            })
            .to_string();
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        });
        let ws_server = tokio::spawn(async move {
            let (stream, _) = ws.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_hdr_async(
                stream,
                |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                    assert_eq!(request.uri().path(), "/test-public");
                    assert!(
                        request
                            .headers()
                            .keys()
                            .all(|name| !name.as_str().starts_with("kalshi-"))
                    );
                    Ok(response)
                },
            )
            .await
            .unwrap();
            let subscription = loop {
                match ws.next().await.unwrap().unwrap() {
                    Message::Text(text) if text == "PING" => {
                        ws.send(Message::Text("PONG".into())).await.unwrap();
                    }
                    Message::Text(text) => break serde_json::from_str::<Value>(&text).unwrap(),
                    Message::Ping(payload) => ws.send(Message::Pong(payload)).await.unwrap(),
                    other => panic!("expected public subscription, got {other:?}"),
                }
            };
            assert_eq!(subscription["type"], "market");
            assert_eq!(subscription["assets_ids"], json!([ASSET]));
            assert_eq!(subscription["initial_dump"], true);
            assert!(subscription.get("id").is_none());
            for frame in [
                json!({"event_type":"book","asset_id":ASSET,"timestamp":"1700000000000",
                    "bids":[{"price":"0.4","size":"3"}],"asks":[{"price":"0.6","size":"2"}]}),
                json!({"event_type":"price_change","timestamp":"1700000000001",
                    "price_changes":[{"asset_id":ASSET,"price":"0.4","size":"4","side":"BUY"}]}),
            ] {
                ws.send(Message::Text(frame.to_string().into()))
                    .await
                    .unwrap();
            }
            while let Some(message) = ws.next().await {
                match message.unwrap() {
                    Message::Text(text) if text == "PING" => {
                        ws.send(Message::Text("PONG".into())).await.unwrap();
                    }
                    Message::Ping(payload) => ws.send(Message::Pong(payload)).await.unwrap(),
                    Message::Close(_) => {
                        ws.flush().await.unwrap();
                        return;
                    }
                    other => panic!("unexpected public command after initialization: {other:?}"),
                }
            }
        });
        let capture = temp.path().join("capture.sbe");
        let audit = temp.path().join("audit.jsonl");
        let summary = temp.path().join("summary.json");
        let mut command = cli();
        command
            .args([
                "collect",
                "--exchange",
                "polymarket",
                "--rest-url",
                &rest_url,
                "--ws-url",
                &ws_url,
                "--symbols",
                ASSET,
                "--replicas",
                "1",
                "--max-connections",
                "1",
                "--max-runtime",
                "1s",
                "--rotate-seconds",
                "0",
            ])
            .arg("--output")
            .arg(&capture)
            .arg("--audit")
            .arg(&audit)
            .arg("--summary")
            .arg(&summary);
        if unusable_native_files {
            command
                .env("KALSHI_KEY_ID_FILE", temp.path())
                .env("KALSHI_PRIVATE_KEY_PATH", temp.path());
        }
        let output = bounded_output(command).await;
        assert!(
            output.status.success(),
            "public CLI failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stdout.is_empty());
        tokio::time::timeout(Duration::from_secs(2), rest_server)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), ws_server)
            .await
            .unwrap()
            .unwrap();
        let report = replay::verify(&capture, Some(&audit), 10).unwrap();
        assert_eq!(report.symbols, 1);
        assert_eq!(report.updates, 1);
        let summary: Value =
            serde_json::from_reader(std::fs::File::open(summary).unwrap()).unwrap();
        assert_eq!(summary["desired"], 1);
        assert_eq!(summary["complete_at_cutoff"], true);
    }
}
