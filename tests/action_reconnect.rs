use std::{
    collections::BTreeMap,
    sync::atomic::{
        AtomicBool,
        Ordering,
    },
    task::Poll,
};

use convex::{
    base_client::BaseConvexClient,
    FunctionResult,
    Value,
};
use convex_sync_types::{
    AuthenticationToken,
    ClientMessage,
    ErrorPayload,
    LogLinesMessage,
    ServerMessage,
    StateVersion,
    Timestamp,
};
use tokio::sync::oneshot::error::TryRecvError;

#[tokio::test]
async fn reconnect_fails_in_flight_actions_but_replays_mutations_and_queries() -> anyhow::Result<()>
{
    let mut client = BaseConvexClient::new();
    client.subscribe("test:query".parse()?, BTreeMap::new());
    let mut action = client.action("test:action".parse()?, BTreeMap::new());
    let mut mutation = client.mutation("test:mutation".parse()?, BTreeMap::new());
    let original = std::iter::from_fn(|| client.pop_next_message()).collect::<Vec<_>>();
    let original_mutation = original
        .iter()
        .find(|message| matches!(message, ClientMessage::Mutation { .. }))
        .unwrap();

    client.resend_ongoing_queries_mutations().await;

    assert_eq!(
        action.try_recv()?,
        FunctionResult::ErrorMessage("Connection lost while action was in flight".into())
    );
    assert_eq!(mutation.try_recv(), Err(TryRecvError::Empty));
    let Some(ClientMessage::ModifyQuerySet {
        new_version,
        modifications,
        ..
    }) = client.pop_next_message()
    else {
        panic!("Expected restored query subscriptions");
    };
    assert_eq!(modifications.len(), 1);
    assert_eq!(client.pop_next_message().as_ref(), Some(original_mutation));
    assert!(client.pop_next_message().is_none());

    // A further reconnect must still replay the mutation and cannot resurrect
    // the failed action. Its original receiver can complete normally afterward.
    client.resend_ongoing_queries_mutations().await;
    assert!(matches!(
        client.pop_next_message(),
        Some(ClientMessage::ModifyQuerySet { .. })
    ));
    assert_eq!(client.pop_next_message().as_ref(), Some(original_mutation));
    assert!(client.pop_next_message().is_none());
    let ClientMessage::Mutation { request_id, .. } = original_mutation else {
        unreachable!();
    };
    let ts = Timestamp::try_from(1_i64)?;
    client
        .receive_message(ServerMessage::MutationResponse {
            request_id: *request_id,
            result: Ok(Value::Int64(1)),
            ts: Some(ts),
            log_lines: LogLinesMessage(vec![]),
        })
        .unwrap();
    let mut end_version = StateVersion::initial();
    end_version.query_set = new_version;
    end_version.ts = ts;
    client
        .receive_message(ServerMessage::Transition {
            start_version: StateVersion::initial(),
            end_version,
            modifications: vec![],
            client_clock_skew: None,
            server_ts: None,
        })
        .unwrap();
    assert_eq!(mutation.try_recv()?, FunctionResult::Value(Value::Int64(1)));
    Ok(())
}

#[tokio::test]
async fn queued_action_survives_reconnects_until_handed_to_transport() -> anyhow::Result<()> {
    let mut client = BaseConvexClient::new();
    let mut action = client.action("test:action".parse()?, BTreeMap::new());

    for _ in 0..3 {
        client.resend_ongoing_queries_mutations().await;
        // A connection failure interrupts the drain before the action is popped.
        assert!(matches!(
            client.pop_next_message(),
            Some(ClientMessage::ModifyQuerySet { .. })
        ));
        assert_eq!(action.try_recv(), Err(TryRecvError::Empty));
    }
    assert!(matches!(
        client.pop_next_message(),
        Some(ClientMessage::Action { request_id: 0, .. })
    ));
    assert!(client.pop_next_message().is_none());

    client.resend_ongoing_queries_mutations().await;
    assert_eq!(
        action.try_recv()?,
        FunctionResult::ErrorMessage("Connection lost while action was in flight".into())
    );
    assert!(std::iter::from_fn(|| client.pop_next_message())
        .all(|message| !matches!(message, ClientMessage::Action { .. })));
    Ok(())
}

#[tokio::test]
async fn cancelled_auth_refresh_preserves_queued_action() -> anyhow::Result<()> {
    let mut client = BaseConvexClient::new();
    let first_refresh = AtomicBool::new(true);
    client
        .set_auth_fetcher(Some(Box::new(move |force_refresh| {
            let wait = force_refresh && first_refresh.swap(false, Ordering::SeqCst);
            Box::pin(async move {
                if wait {
                    std::future::pending::<()>().await;
                }
                Ok(AuthenticationToken::User("test-token".into()))
            })
        })))
        .await;
    assert!(matches!(
        client.pop_next_message(),
        Some(ClientMessage::Authenticate { .. })
    ));
    let mut action = client.action("test:action".parse()?, BTreeMap::new());

    let mut restart = Box::pin(client.resend_ongoing_queries_mutations());
    assert!(matches!(futures::poll!(&mut restart), Poll::Pending));
    drop(restart);
    client.resend_ongoing_queries_mutations().await;

    assert_eq!(action.try_recv(), Err(TryRecvError::Empty));
    assert!(matches!(
        client.pop_next_message(),
        Some(ClientMessage::Authenticate { .. })
    ));
    assert!(matches!(
        client.pop_next_message(),
        Some(ClientMessage::ModifyQuerySet { .. })
    ));
    let Some(ClientMessage::Action { request_id: 0, .. }) = client.pop_next_message() else {
        panic!("Expected the action's first send after cancelled auth refresh");
    };
    assert!(client.pop_next_message().is_none());
    client
        .receive_message(ServerMessage::ActionResponse {
            request_id: 0,
            result: Ok(Value::Int64(9)),
            log_lines: LogLinesMessage(vec![]),
        })
        .unwrap();
    assert_eq!(action.try_recv()?, FunctionResult::Value(Value::Int64(9)));
    Ok(())
}

#[tokio::test]
async fn completed_action_keeps_its_result_across_reconnect() -> anyhow::Result<()> {
    for result in [
        Ok(Value::Int64(7)),
        Err(ErrorPayload::Message("Action failed".to_string())),
    ] {
        let mut client = BaseConvexClient::new();
        let mut action = client.action("test:action".parse()?, BTreeMap::new());
        let Some(ClientMessage::Action { request_id, .. }) = client.pop_next_message() else {
            panic!("Expected action request");
        };
        client
            .receive_message(ServerMessage::ActionResponse {
                request_id,
                result: result.clone(),
                log_lines: LogLinesMessage(vec![]),
            })
            .unwrap();

        client.resend_ongoing_queries_mutations().await;
        assert_eq!(action.try_recv()?, FunctionResult::from(result));
        assert!(std::iter::from_fn(|| client.pop_next_message())
            .all(|message| !matches!(message, ClientMessage::Action { .. })));
    }
    Ok(())
}
