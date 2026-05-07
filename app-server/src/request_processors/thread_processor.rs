use std::path::PathBuf;

use crate::thread_state::ThreadState;
use crate::thread_state::ThreadStore;
use claude_app_server_protocol::ForkedThread;
use claude_app_server_protocol::INVALID_PARAMS_ERROR_CODE;
use claude_app_server_protocol::JSONRPCError;
use claude_app_server_protocol::THREAD_NOT_FOUND_ERROR_CODE;
use claude_app_server_protocol::ThreadForkResponse;
use claude_app_server_protocol::ThreadResumeResponse;
use claude_app_server_protocol::ThreadStartResponse;
use claude_app_server_protocol::ThreadStartThread;

pub async fn thread_start(
    store: &ThreadStore,
    params: Option<serde_json::Value>,
) -> Result<ThreadStartResponse, JSONRPCError> {
    let raw = params.unwrap_or_default();
    let cwd = raw
        .get("cwd")
        .and_then(serde_json::Value::as_str)
        .map(expand_home)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    let permission_mode = raw
        .get("permissionMode")
        .or_else(|| raw.get("permission_mode"))
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|err| JSONRPCError::new(INVALID_PARAMS_ERROR_CODE, err.to_string()))?
        .unwrap_or_default();
    let thread = ThreadState::new(cwd, permission_mode);
    let response = ThreadStartResponse {
        thread: ThreadStartThread {
            id: thread.id.clone(),
            created_at: thread.created_at,
        },
    };
    store.insert(thread).await;
    Ok(response)
}

pub async fn thread_resume(
    store: &ThreadStore,
    params: Option<serde_json::Value>,
) -> Result<ThreadResumeResponse, JSONRPCError> {
    let thread_id = get_thread_id(params.as_ref())?;
    let thread = store.get_snapshot(&thread_id).await.ok_or_else(|| {
        JSONRPCError::new(
            THREAD_NOT_FOUND_ERROR_CODE,
            format!("Thread not found: {thread_id}"),
        )
    })?;
    Ok(ThreadResumeResponse { thread })
}

pub async fn thread_fork(
    store: &ThreadStore,
    params: Option<serde_json::Value>,
) -> Result<ThreadForkResponse, JSONRPCError> {
    let source_id = get_thread_id(params.as_ref())?;
    let forked = store
        .with_thread_mut(&source_id, |source| ThreadState::fork_from(source))
        .await
        .ok_or_else(|| {
            JSONRPCError::new(
                THREAD_NOT_FOUND_ERROR_CODE,
                format!("Thread not found: {source_id}"),
            )
        })?
        .ok_or_else(|| {
            JSONRPCError::new(
                INVALID_PARAMS_ERROR_CODE,
                "Cannot fork a thread that has no turns yet.",
            )
        })?;
    let response = ThreadForkResponse {
        thread: ForkedThread {
            id: forked.id.clone(),
            forked_from: source_id,
            created_at: forked.created_at,
        },
    };
    store.insert(forked).await;
    Ok(response)
}

pub fn get_thread_id(params: Option<&serde_json::Value>) -> Result<String, JSONRPCError> {
    params
        .and_then(|params| {
            params
                .get("threadId")
                .or_else(|| params.get("thread_id"))
                .and_then(serde_json::Value::as_str)
        })
        .map(ToString::to_string)
        .ok_or_else(|| {
            JSONRPCError::new(
                INVALID_PARAMS_ERROR_CODE,
                "thread_id or threadId is required",
            )
        })
}

fn expand_home(cwd: &str) -> PathBuf {
    if cwd == "~" {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(cwd))
    } else if let Some(rest) = cwd.strip_prefix("~/") {
        std::env::var_os("HOME")
            .map(|home| PathBuf::from(home).join(rest))
            .unwrap_or_else(|| PathBuf::from(cwd))
    } else {
        PathBuf::from(cwd)
    }
}
