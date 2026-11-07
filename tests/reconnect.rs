use std::{
    collections::BTreeMap,
    time::Duration,
};

use anyhow::Context;
use convex::{
    ConvexClient,
    FunctionResult,
    Value,
};
use convex_sync_types::{
    ClientMessage,
    LogLinesMessage,
    ServerMessage,
    SessionId,
    StateVersion,
    Timestamp,
};
use futures::{
    SinkExt,
    StreamExt,
};
use tokio::{
    net::{
        TcpListener,
        TcpStream,
    },
    time::timeout,
};
use tokio_tungstenite::{
    accept_async,
    tungstenite::Message,
    WebSocketStream,
};
use uuid::Uuid;

async fn receive_message(socket: &mut WebSocketStream<TcpStream>) -> anyhow::Result<ClientMessage> {
    let message = socket.next().await.context("Client disconnected")??;
    let Message::Text(text) = message else {
        anyhow::bail!("Expected a text message, got {message:?}");
    };
    serde_json::from_str::<serde_json::Value>(&text)?.try_into()
}

async fn accept_connection(
    listener: &TcpListener,
) -> anyhow::Result<(WebSocketStream<TcpStream>, SessionId, u32)> {
    let (stream, _) = listener.accept().await?;
    let mut socket = accept_async(stream).await?;
    let ClientMessage::Connect {
        session_id,
        connection_count,
        ..
    } = receive_message(&mut socket).await?
    else {
        anyhow::bail!("Expected Connect as the first message");
    };
    Ok((socket, session_id, connection_count))
}

#[tokio::test]
async fn mutation_is_deduplicated_after_losing_responses() -> anyhow::Result<()> {
    timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let mut client = ConvexClient::new(&format!("http://{}", listener.local_addr()?)).await?;

        let server = async {
            let mut committed = BTreeMap::new();
            let mut counter = 0_i64;
            let mut sessions = Vec::new();
            let mut mutations = Vec::new();
            for attempt in 0..3 {
                let (mut socket, session_id, connection_count) =
                    accept_connection(&listener).await?;
                assert_eq!(connection_count, attempt);
                sessions.push(session_id);

                let mut version = StateVersion::initial();
                let request_id = loop {
                    let message = receive_message(&mut socket).await?;
                    match message {
                        ClientMessage::ModifyQuerySet { new_version, .. } => {
                            version.query_set = new_version;
                        },
                        ClientMessage::Mutation { request_id, .. } => {
                            mutations.push(message);
                            break request_id;
                        },
                        _ => anyhow::bail!("Unexpected client message: {message:?}"),
                    }
                };

                // Model the backend's committed-success lookup by (session_id, request_id).
                let result = *committed
                    .entry((Uuid::from(session_id), request_id))
                    .or_insert_with(|| {
                        counter += 1;
                        counter
                    });
                if attempt < 2 {
                    // The mutation committed, but the connection drops before its response.
                    drop(socket);
                    continue;
                }

                version.ts = Timestamp::try_from(result)?;
                let response = ServerMessage::MutationResponse {
                    request_id,
                    result: Ok(Value::Int64(result)),
                    ts: Some(version.ts),
                    log_lines: LogLinesMessage(vec![]),
                };
                socket
                    .send(Message::Text(
                        serde_json::Value::from(response).to_string().into(),
                    ))
                    .await?;
                let transition = ServerMessage::<Value>::Transition {
                    start_version: StateVersion::initial(),
                    end_version: version,
                    modifications: vec![],
                    client_clock_skew: None,
                    server_ts: None,
                };
                socket
                    .send(Message::Text(
                        serde_json::Value::from(transition).to_string().into(),
                    ))
                    .await?;
            }
            Ok::<_, anyhow::Error>((counter, sessions, mutations))
        };

        let (result, (counter, sessions, mutations)) =
            futures::try_join!(client.mutation("incrementCounter", BTreeMap::new()), server,)?;
        assert_eq!(result, FunctionResult::Value(Value::Int64(1)));
        assert_eq!(counter, 1, "The mutation must commit only once");
        assert!(sessions.windows(2).all(|pair| pair[0] == pair[1]));
        assert!(mutations.windows(2).all(|pair| pair[0] == pair[1]));
        Ok(())
    })
    .await?
}

#[tokio::test]
async fn independent_clients_have_distinct_sessions() -> anyhow::Result<()> {
    timeout(Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("http://{}", listener.local_addr()?);
        let _first_client = ConvexClient::new(&url).await?;
        let (_first_socket, first_session, first_count) = accept_connection(&listener).await?;
        let _second_client = ConvexClient::new(&url).await?;
        let (_second_socket, second_session, second_count) = accept_connection(&listener).await?;

        assert_ne!(first_session, second_session);
        assert_eq!(first_count, 0);
        assert_eq!(second_count, 0);
        Ok(())
    })
    .await?
}
