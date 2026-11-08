use std::{
    collections::BTreeMap,
    convert::Infallible,
    time::Duration,
};

use convex_sync_types::{
    backoff::Backoff,
    UdfPath,
};
use tokio::sync::{
    broadcast,
    mpsc,
    oneshot,
};
use tokio_stream::wrappers::BroadcastStream;

use crate::{
    base_client::{
        AuthTokenFetcher,
        BaseConvexClient,
        SubscriberId,
    },
    client::{
        QueryResults,
        QuerySubscription,
    },
    sync::{
        ProtocolResponse,
        ReconnectProtocolReason,
        ReconnectRequest,
        SyncProtocol,
    },
    value::Value,
    FunctionResult,
};

const INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const MAX_BACKOFF: Duration = Duration::from_secs(15);

pub enum ClientRequest {
    Mutation(
        MutationRequest,
        oneshot::Sender<oneshot::Receiver<FunctionResult>>,
    ),
    Action(
        ActionRequest,
        oneshot::Sender<oneshot::Receiver<FunctionResult>>,
    ),
    Subscribe(
        SubscribeRequest,
        oneshot::Sender<QuerySubscription>,
        mpsc::UnboundedSender<ClientRequest>,
    ),
    Unsubscribe(UnsubscribeRequest),
    Authenticate(Option<AuthTokenFetcher>),
}

pub struct MutationRequest {
    pub udf_path: UdfPath,
    pub args: BTreeMap<String, Value>,
}

pub struct ActionRequest {
    pub udf_path: UdfPath,
    pub args: BTreeMap<String, Value>,
}

pub struct SubscribeRequest {
    pub udf_path: UdfPath,
    pub args: BTreeMap<String, Value>,
}

#[derive(Debug)]
pub struct UnsubscribeRequest {
    pub subscriber_id: SubscriberId,
}

pub async fn worker<T: SyncProtocol>(
    mut protocol_response_receiver: mpsc::Receiver<ProtocolResponse>,
    mut client_request_receiver: mpsc::UnboundedReceiver<ClientRequest>,
    mut watch_sender: broadcast::Sender<QueryResults>,
    mut base_client: BaseConvexClient,
    mut protocol_manager: T,
) -> Infallible {
    let mut backoff = Backoff::new(INITIAL_BACKOFF, MAX_BACKOFF);
    loop {
        let e = loop {
            match _worker_once(
                &mut protocol_response_receiver,
                &mut client_request_receiver,
                &mut watch_sender,
                &mut base_client,
                &mut protocol_manager,
            )
            .await
            {
                Ok(()) => backoff.reset(),
                Err(e) => break e,
            }
        };

        let delay = backoff.fail(&mut rand::rng());
        tracing::error!(
            "Convex Client Worker failed: {e:?}. Backing off for {delay:?} and retrying."
        );
        tokio::time::sleep(delay).await;

        // Tell the sync protocol to reconnect followed by an immediate resend of
        // ongoing queries/mutations. It's important these happen together to
        // ensure mutation ordering. If an auth token fetcher is stored,
        // resend_ongoing_queries_mutations will refresh the token first.
        protocol_manager
            .reconnect(ReconnectRequest {
                reason: e,
                max_observed_timestamp: base_client.max_observed_timestamp(),
            })
            .await;
        // Until the protocol acknowledges the reconnect, responses still come
        // from the connection being replaced. Handle them against the state
        // they belong to, before it's resent below, and ignore their errors:
        // that connection is already being replaced. Otherwise its failure
        // (the server closes the socket right after an AuthError, say) would
        // reconnect a second time, and its transitions would hit the new state.
        while let Some(protocol_response) = protocol_response_receiver.recv().await {
            if let ProtocolResponse::ReconnectAcknowledged = protocol_response {
                break;
            }
            let _ =
                handle_protocol_response(&mut base_client, &mut watch_sender, protocol_response);
        }
        base_client.resend_ongoing_queries_mutations().await;
        // We'll flush messages from base_client inside the next call to
        // `_worker_once`.
    }
}

async fn _worker_once<T: SyncProtocol>(
    protocol_response_receiver: &mut mpsc::Receiver<ProtocolResponse>,
    client_request_receiver: &mut mpsc::UnboundedReceiver<ClientRequest>,
    watch_sender: &mut broadcast::Sender<QueryResults>,
    base_client: &mut BaseConvexClient,
    protocol_manager: &mut T,
) -> Result<(), ReconnectProtocolReason> {
    // If there are any outgoing messages to flush (e.g. from an outer reconnect),
    // do so first.
    communicate(
        base_client,
        protocol_response_receiver,
        watch_sender,
        protocol_manager,
    )
    .await?;

    tokio::select! {
        Some(protocol_response) = protocol_response_receiver.recv() => {
            handle_protocol_response(base_client, watch_sender, protocol_response)?;
        }
        Some(client_request) = client_request_receiver.recv() => {
            match client_request {
                ClientRequest::Subscribe(query, tx, request_sender) => {
                    let watch = watch_sender.subscribe();
                    let SubscribeRequest {
                        udf_path,
                        args,
                    } =  query;
                    let subscriber_id = base_client.subscribe(udf_path, args);
                    communicate(
                        base_client,
                        protocol_response_receiver,
                        watch_sender,
                        protocol_manager,
                    )
                    .await?;

                    let watch = BroadcastStream::new(watch);
                    let subscription = QuerySubscription {
                        subscriber_id,
                        request_sender,
                        watch,
                        initial: base_client.latest_results().get(&subscriber_id).cloned(),
                    };
                    let _ = tx.send(subscription);
                },
                ClientRequest::Mutation(mutation, tx) => {
                    let MutationRequest {
                        udf_path,
                        args,
                    } = mutation;
                    let result_receiver = base_client
                        .mutation(udf_path, args);
                        communicate(
                            base_client,
                            protocol_response_receiver,
                            watch_sender,
                            protocol_manager,
                        )
                        .await?;
                    let _ = tx.send(result_receiver);
                },
                ClientRequest::Action(action, tx) => {
                    let ActionRequest {
                        udf_path,
                        args,
                    } = action;
                    let result_receiver = base_client
                        .action(udf_path, args);
                        communicate(
                            base_client,
                            protocol_response_receiver,
                            watch_sender,
                            protocol_manager,
                        )
                        .await?;
                    let _ = tx.send(result_receiver);
                },
                ClientRequest::Unsubscribe(unsubscribe) => {
                    let UnsubscribeRequest {subscriber_id} = unsubscribe;
                    base_client.unsubscribe(subscriber_id);
                    communicate(
                        base_client,
                        protocol_response_receiver,
                        watch_sender,
                        protocol_manager,
                    )
                    .await?;
                },
                ClientRequest::Authenticate(fetcher) => {
                    base_client.set_auth_fetcher(fetcher).await;
                    communicate(
                        base_client,
                        protocol_response_receiver,
                        watch_sender,
                        protocol_manager,
                    )
                    .await?;
                },
            }
        },
        // TODO: this else branch will lead to an infinite loop if both channels
        // are closed
        else => (),
    }
    Ok(())
}

/// Flush all messages to the protocol while processing server mesages.
async fn communicate<P: SyncProtocol>(
    base_client: &mut BaseConvexClient,
    protocol_response_receiver: &mut mpsc::Receiver<ProtocolResponse>,
    watch_sender: &mut broadcast::Sender<QueryResults>,
    protocol: &mut P,
) -> Result<(), ReconnectProtocolReason> {
    while let Some(modification) = base_client.pop_next_message() {
        let mut send_future = protocol.send(modification);
        loop {
            tokio::select! {
               _ = &mut send_future => break,
               // Keep processing protocol responses while waiting so that we
               // don't deadlock with the websocket worker.
               Some(protocol_response) = protocol_response_receiver.recv() => {
                   handle_protocol_response(base_client, watch_sender, protocol_response)?;
               }
            }
        }
    }
    Ok(())
}

fn handle_protocol_response(
    base_client: &mut BaseConvexClient,
    watch_sender: &mut broadcast::Sender<QueryResults>,
    protocol_response: ProtocolResponse,
) -> Result<(), ReconnectProtocolReason> {
    match protocol_response {
        ProtocolResponse::ServerMessage(msg) => {
            if let Some(subscriber_id_to_latest_value) = base_client.receive_message(msg)? {
                // Notify watchers of the new consistent query results at new timestamp
                let _ = watch_sender.send(subscriber_id_to_latest_value);
            }
        },
        ProtocolResponse::Failure => {
            return Err("ProtocolFailure".into());
        },
        // Only expected after a reconnect request, where `worker` waits for it.
        ProtocolResponse::ReconnectAcknowledged => {},
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        sync::Arc,
        time::Duration,
    };

    use async_trait::async_trait;
    use convex_sync_types::ClientMessage;
    use parking_lot::Mutex;
    use tokio::sync::{
        broadcast,
        mpsc,
    };
    use url::Url;

    use super::worker;
    use crate::{
        base_client::BaseConvexClient,
        sync::{
            ProtocolResponse,
            ReconnectRequest,
            ServerMessage,
            SyncProtocol,
            WebSocketState,
        },
    };

    /// Records what the worker sends and the reconnects it asks for. Like
    /// the WebSocket worker, it can deliver responses from the old connection
    /// after a reconnect is requested, then acknowledges it.
    struct FakeProtocol {
        response_sender: mpsc::Sender<ProtocolResponse>,
        late_responses: Arc<Mutex<Vec<ProtocolResponse>>>,
        sent: Arc<Mutex<Vec<ClientMessage>>>,
        reconnects: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl SyncProtocol for FakeProtocol {
        async fn open(
            _ws_url: Url,
            _on_response: mpsc::Sender<ProtocolResponse>,
            _on_state_change: Option<mpsc::Sender<WebSocketState>>,
            _client_id: &str,
        ) -> anyhow::Result<Self> {
            unimplemented!()
        }

        async fn send(&mut self, message: ClientMessage) -> anyhow::Result<()> {
            self.sent.lock().push(message);
            Ok(())
        }

        async fn reconnect(&mut self, request: ReconnectRequest) {
            self.reconnects.lock().push(request.reason);
            let late_responses = std::mem::take(&mut *self.late_responses.lock());
            let response_sender = self.response_sender.clone();
            tokio::spawn(async move {
                for response in late_responses {
                    let _ = response_sender.send(response).await;
                }
                let _ = response_sender
                    .send(ProtocolResponse::ReconnectAcknowledged)
                    .await;
            });
        }
    }

    #[tokio::test]
    async fn test_auth_error_reconnects_once() -> anyhow::Result<()> {
        let (response_sender, response_receiver) = mpsc::channel(1);
        let (_request_sender, request_receiver) = mpsc::unbounded_channel();
        let (watch_sender, _watch_receiver) = broadcast::channel(1);
        let sent = Arc::new(Mutex::new(vec![]));
        let reconnects = Arc::new(Mutex::new(vec![]));
        // The server closes the socket right after rejecting the token, and
        // the WebSocket worker reports that after the reconnect is requested.
        let protocol = FakeProtocol {
            response_sender: response_sender.clone(),
            late_responses: Arc::new(Mutex::new(vec![ProtocolResponse::Failure])),
            sent: sent.clone(),
            reconnects: reconnects.clone(),
        };
        let worker = tokio::spawn(worker(
            response_receiver,
            request_receiver,
            watch_sender,
            BaseConvexClient::new(),
            protocol,
        ));

        response_sender
            .send(ProtocolResponse::ServerMessage(ServerMessage::AuthError {
                error_message: "Token expired".into(),
                base_version: None,
                auth_update_attempted: Some(false),
            }))
            .await?;

        // Wait for the state to be resent on the new connection, then for
        // longer than the backoff before a second reconnect.
        tokio::time::timeout(Duration::from_secs(2), async {
            while sent.lock().is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        tokio::time::sleep(Duration::from_secs(1)).await;
        worker.abort();
        let reconnects = reconnects.lock();
        assert_eq!(reconnects.len(), 1, "reconnected for {reconnects:?}");
        Ok(())
    }
}
