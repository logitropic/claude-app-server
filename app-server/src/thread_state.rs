use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use claude_app_server_protocol::Item;
use claude_app_server_protocol::PermissionMode;
use claude_app_server_protocol::StoredItem;
use claude_app_server_protocol::Thread;
use claude_app_server_protocol::Turn;
use claude_app_server_protocol::TurnStatus;
use tokio::process::Child;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[derive(Clone, Default)]
pub struct ThreadStore {
    inner: Arc<Mutex<HashMap<String, ThreadState>>>,
}

pub struct ThreadState {
    pub id: String,
    pub created_at: u128,
    pub turns: Vec<TurnState>,
    pub cwd: PathBuf,
    pub permission_mode: PermissionMode,
    pub active_turn_id: Option<String>,
    pub cli_session_id: Option<String>,
    pub fork_from: Option<ForkFrom>,
}

pub struct TurnState {
    pub id: String,
    pub thread_id: String,
    pub status: TurnStatus,
    pub user_content: String,
    pub steer_queue: Vec<String>,
    pub items: Vec<StoredItem>,
    pub process: Option<Arc<Mutex<Child>>>,
    pub cancel: CancellationToken,
    pub created_at: u128,
    pub completed_at: Option<u128>,
    pub error: Option<String>,
    pub usage: Option<serde_json::Value>,
    pub cost_usd: Option<f64>,
}

#[derive(Clone)]
pub struct ForkFrom {
    pub cli_session_id: String,
}

impl ThreadStore {
    pub async fn insert(&self, thread: ThreadState) {
        self.inner.lock().await.insert(thread.id.clone(), thread);
    }

    pub async fn with_thread_mut<R>(
        &self,
        id: &str,
        f: impl FnOnce(&mut ThreadState) -> R,
    ) -> Option<R> {
        let mut threads = self.inner.lock().await;
        let thread = threads.get_mut(id)?;
        Some(f(thread))
    }

    pub async fn get_snapshot(&self, id: &str) -> Option<Thread> {
        self.inner.lock().await.get(id).map(ThreadState::snapshot)
    }
}

impl ThreadState {
    pub fn new(cwd: PathBuf, permission_mode: PermissionMode) -> Self {
        Self {
            id: Uuid::now_v7().to_string(),
            created_at: now_millis(),
            turns: Vec::new(),
            cwd,
            permission_mode,
            active_turn_id: None,
            cli_session_id: None,
            fork_from: None,
        }
    }

    pub fn fork_from(source: &ThreadState) -> Option<Self> {
        let cli_session_id = source.cli_session_id.clone()?;
        let mut forked = Self::new(source.cwd.clone(), source.permission_mode);
        forked.fork_from = Some(ForkFrom { cli_session_id });
        Some(forked)
    }

    pub fn snapshot(&self) -> Thread {
        Thread {
            id: self.id.clone(),
            created_at: self.created_at,
            cwd: Some(self.cwd.to_string_lossy().to_string()),
            permission_mode: Some(self.permission_mode),
            cli_session_id: self.cli_session_id.clone(),
            turns: self.turns.iter().map(TurnState::snapshot).collect(),
        }
    }

    pub fn active_turn_mut(&mut self) -> Option<&mut TurnState> {
        let active_turn_id = self.active_turn_id.as_deref()?;
        self.turns.iter_mut().find(|turn| turn.id == active_turn_id)
    }
}

impl TurnState {
    pub fn new(thread_id: String, user_content: String) -> Self {
        Self {
            id: Uuid::now_v7().to_string(),
            thread_id,
            status: TurnStatus::Active,
            user_content,
            steer_queue: Vec::new(),
            items: Vec::new(),
            process: None,
            cancel: CancellationToken::new(),
            created_at: now_millis(),
            completed_at: None,
            error: None,
            usage: None,
            cost_usd: None,
        }
    }

    pub fn snapshot(&self) -> Turn {
        Turn {
            id: self.id.clone(),
            thread_id: self.thread_id.clone(),
            status: self.status.clone(),
            user_content: Some(self.user_content.clone()),
            items: self.items.clone(),
            created_at: self.created_at,
            completed_at: self.completed_at,
            error: self.error.clone(),
        }
    }

    pub fn push_item(&mut self, item: Item) -> StoredItem {
        let stored = StoredItem {
            id: Uuid::now_v7().to_string(),
            created_at: now_millis(),
            item,
        };
        self.items.push(stored.clone());
        stored
    }
}

pub fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stores_thread_snapshots() {
        let store = ThreadStore::default();
        let thread = ThreadState::new(PathBuf::from("/tmp"), PermissionMode::Default);
        let id = thread.id.clone();
        store.insert(thread).await;
        let snapshot = store.get_snapshot(&id).await.unwrap();
        assert_eq!(snapshot.id, id);
    }
}
