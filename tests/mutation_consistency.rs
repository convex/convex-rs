use std::collections::BTreeMap;

use convex::{
    base_client::BaseConvexClient,
    FunctionResult,
    Value,
};
use convex_sync_types::{
    types::ErrorPayload,
    ClientMessage,
    LogLinesMessage,
    QueryId,
    QuerySetModification,
    ServerMessage,
    StateModification,
    StateVersion,
    Timestamp,
};
use tokio::sync::{
    oneshot,
    oneshot::error::TryRecvError,
};

#[test]
fn successful_mutation_waits_until_queries_reach_its_commit() {
    // Both reaching the commit and advancing beyond it make the write visible.
    for final_ts in [20, 25] {
        let mut client = BaseConvexClient::new();
        let subscriber = client.subscribe("counter:get".parse().unwrap(), BTreeMap::new());
        let Some(ClientMessage::ModifyQuerySet { modifications, .. }) = client.pop_next_message()
        else {
            panic!("expected subscription message");
        };
        let [QuerySetModification::Add(query)] = modifications.as_slice() else {
            panic!("expected one query");
        };
        let query_id = query.query_id;
        let mut version = StateVersion::initial();
        transition(&mut client, &mut version, 10, Some((query_id, 0)));

        let (request_id, mut result) = mutation(&mut client);
        respond(&mut client, request_id, 20);
        assert_eq!(result.try_recv(), Err(TryRecvError::Empty));

        // An unrelated write can update the query before this mutation is visible.
        transition(&mut client, &mut version, 15, Some((query_id, 1)));
        assert_eq!(
            client.latest_results().get(&subscriber),
            Some(&FunctionResult::Value(Value::Int64(1)))
        );
        assert_eq!(
            result.try_recv(),
            Err(TryRecvError::Empty),
            "mutation resolved while the subscribed query still preceded its commit"
        );

        transition(&mut client, &mut version, final_ts, Some((query_id, 2)));
        assert_eq!(
            result.try_recv(),
            Ok(FunctionResult::Value(Value::Int64(2)))
        );
        assert_eq!(
            client.latest_results().get(&subscriber),
            Some(&FunctionResult::Value(Value::Int64(2)))
        );
        assert_eq!(
            client.get_query(query_id),
            Some(FunctionResult::Value(Value::Int64(2)))
        );
    }
}

#[test]
fn each_mutation_waits_for_its_own_commit_timestamp() {
    let mut client = BaseConvexClient::new();
    let mut version = StateVersion::initial();
    let (first_id, mut first) = mutation(&mut client);
    let (second_id, mut second) = mutation(&mut client);
    let (pending_id, mut pending) = mutation(&mut client);
    respond(&mut client, first_id, 20);
    respond(&mut client, second_id, 30);

    transition(&mut client, &mut version, 20, None);
    assert_eq!(first.try_recv(), Ok(FunctionResult::Value(Value::Int64(2))));
    assert_eq!(second.try_recv(), Err(TryRecvError::Empty));
    assert_eq!(pending.try_recv(), Err(TryRecvError::Empty));

    transition(&mut client, &mut version, 30, None);
    assert_eq!(
        second.try_recv(),
        Ok(FunctionResult::Value(Value::Int64(2)))
    );
    assert_eq!(pending.try_recv(), Err(TryRecvError::Empty));

    respond(&mut client, pending_id, 40);
    transition(&mut client, &mut version, 40, None);
    assert_eq!(
        pending.try_recv(),
        Ok(FunctionResult::Value(Value::Int64(2)))
    );
}

#[test]
fn mutation_errors_complete_without_a_transition() {
    let mut client = BaseConvexClient::new();
    for error in [
        ErrorPayload::Message("rejected".into()),
        ErrorPayload::ErrorData {
            message: "rejected".into(),
            data: Value::Int64(42),
        },
    ] {
        let expected = FunctionResult::from(Err(error.clone()));
        let (request_id, mut result) = mutation(&mut client);
        client
            .receive_message(ServerMessage::MutationResponse {
                request_id,
                result: Err(error),
                ts: None,
                log_lines: LogLinesMessage(vec![]),
            })
            .unwrap();
        assert_eq!(result.try_recv(), Ok(expected));
    }
}

#[test]
fn actions_complete_without_a_transition() {
    let mut client = BaseConvexClient::new();
    let mut action = client.action("counter:action".parse().unwrap(), BTreeMap::new());
    let Some(ClientMessage::Action { request_id, .. }) = client.pop_next_message() else {
        panic!("expected action message");
    };
    client
        .receive_message(ServerMessage::ActionResponse {
            request_id,
            result: Ok(Value::Null),
            log_lines: LogLinesMessage(vec![]),
        })
        .unwrap();
    assert_eq!(action.try_recv(), Ok(FunctionResult::Value(Value::Null)));
}

// --- Protocol fixture helpers ---

fn mutation(client: &mut BaseConvexClient) -> (u32, oneshot::Receiver<FunctionResult>) {
    let result = client.mutation("counter:increment".parse().unwrap(), BTreeMap::new());
    let Some(ClientMessage::Mutation { request_id, .. }) = client.pop_next_message() else {
        panic!("expected mutation message");
    };
    (request_id, result)
}

fn respond(client: &mut BaseConvexClient, request_id: u32, commit_ts: i64) {
    client
        .receive_message(ServerMessage::MutationResponse {
            request_id,
            result: Ok(Value::Int64(2)),
            ts: Some(Timestamp::try_from(commit_ts).unwrap()),
            log_lines: LogLinesMessage(vec![]),
        })
        .unwrap();
}

fn transition(
    client: &mut BaseConvexClient,
    version: &mut StateVersion,
    ts: i64,
    update: Option<(QueryId, i64)>,
) {
    let end_version = StateVersion {
        ts: Timestamp::try_from(ts).unwrap(),
        query_set: if update.is_some() {
            1.into()
        } else {
            version.query_set
        },
        ..*version
    };
    let modifications = update
        .map(|(query_id, value)| StateModification::QueryUpdated {
            query_id,
            value: Value::Int64(value),
            log_lines: LogLinesMessage(vec![]),
            journal: None,
        })
        .into_iter()
        .collect();
    client
        .receive_message(ServerMessage::Transition {
            start_version: *version,
            end_version,
            modifications,
            client_clock_skew: None,
            server_ts: None,
        })
        .unwrap();
    *version = end_version;
}

// --- End protocol fixture helpers ---
