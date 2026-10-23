use std::{
    collections::BTreeMap,
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

/// Whether there is any point calling [`_worker_once`] again.
enum Progress {
    /// A message was handled (or there was nothing to flush). Go round again.
    Continue,
    /// Both channels are closed: the sync protocol has gone away and every
    /// [`crate::ConvexClient`] handle has been dropped, so nothing can ever
    /// arrive on either of them again. There is no work left and no way for
    /// any to appear, so the worker returns.
    Done,
}

pub async fn worker<T: SyncProtocol>(
    mut protocol_response_receiver: mpsc::Receiver<ProtocolResponse>,
    mut client_request_receiver: mpsc::UnboundedReceiver<ClientRequest>,
    mut watch_sender: broadcast::Sender<QueryResults>,
    mut base_client: BaseConvexClient,
    mut protocol_manager: T,
) {
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
                Ok(Progress::Continue) => backoff.reset(),
                Ok(Progress::Done) => return,
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
) -> Result<Progress, ReconnectProtocolReason> {
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
        // Neither channel can produce anything ever again: the protocol side
        // has gone away and no client handle is left to send a request. Every
        // branch of this `select!` is disabled, so going round the outer loop
        // would spin here with no await point in the loop at all — and a task
        // that spins with no await point cannot be aborted either, because an
        // abort only lands at one. So the worker is done instead.
        else => return Ok(Progress::Done),
    }
    Ok(Progress::Continue)
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
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::{
        broadcast,
        mpsc,
    };

    use super::worker;
    use crate::{
        base_client::BaseConvexClient,
        client::QueryResults,
        sync::{
            testing::TestProtocolManager,
            SyncProtocol,
        },
    };

    /// Both channels closed is the end of the worker's work, not a reason to
    /// go round again. Nothing can ever arrive on either of them, so the
    /// `select!` below has no branch left to wait on: with an `else` arm
    /// that merely returns `Ok(())` the outer loop re-enters it immediately
    /// and the task spins, in userspace, with no await point in it — which
    /// also makes it immune to `JoinHandle::abort`, since an abort only
    /// lands at an await point. The only recovery from that is killing the
    /// process.
    #[test]
    fn the_worker_ends_when_both_its_channels_are_closed() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("a runtime");
        let ended = runtime.block_on(async {
            let (response_sender, mut response_receiver) = mpsc::channel(1);
            let (request_sender, request_receiver) = mpsc::unbounded_channel();
            let (watch_sender, _watch_receiver) = broadcast::channel::<QueryResults>(1);
            let protocol = TestProtocolManager::open(
                "ws://test.com".parse().expect("a url"),
                response_sender,
                None,
                "rust-test",
            )
            .await
            .expect("the test protocol opens");

            // The two closures the real worker sees: the websocket worker has
            // gone away, so nothing can arrive from the protocol side, and the
            // last `ConvexClient` has been dropped, so no handle is left to
            // send a request. The response side is closed from the receiver
            // rather than by dropping the sender because the protocol manager
            // holds that sender and is itself moved into the worker; `recv`
            // answers `None` either way, which is all the `select!` sees.
            response_receiver.close();
            drop(request_sender);

            let handle = tokio::spawn(worker(
                response_receiver,
                request_receiver,
                watch_sender,
                BaseConvexClient::new(),
                protocol,
            ));
            tokio::time::timeout(Duration::from_secs(5), handle).await
        });
        // Give up on a worker that will not stop rather than blocking here
        // forever: a spinning task cannot be aborted, so the runtime would
        // never finish shutting down and the assertion below would never be
        // reached to say why.
        runtime.shutdown_timeout(Duration::from_secs(1));
        let joined = ended.expect(
            "the worker should end once both its channels are closed, rather than spin on a \
             select! with every branch disabled",
        );
        joined.expect("the worker task should not have panicked");
    }
}
