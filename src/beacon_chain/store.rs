use async_trait::async_trait;
use sqlx::PgPool;

use super::states::{self, BeaconState};

#[async_trait]
pub trait BeaconStore {
    async fn last_stored_state(&self) -> Option<BeaconState>;
}

pub struct BeaconStorePostgres {
    db_pool: PgPool,
}

impl BeaconStorePostgres {
    pub fn new(db_pool: PgPool) -> Self {
        Self { db_pool }
    }
}

#[async_trait]
impl BeaconStore for BeaconStorePostgres {
    async fn last_stored_state(&self) -> Option<BeaconState> {
        states::last_stored_state(&self.db_pool).await.unwrap()
    }
}

#[cfg(test)]
mod tests {
    use test_context::test_context;

    use crate::{beacon_chain::Slot, db::tests::TestDb};

    use super::*;

    #[test_context(TestDb)]
    #[tokio::test]
    async fn last_stored_state_test(test_db: &TestDb) {
        let beacon_store = BeaconStorePostgres::new(test_db.pool.clone());

        let state = beacon_store.last_stored_state().await;
        assert!(state.is_none());

        let test_state = BeaconState {
            slot: Slot(0),
            state_root: "0xstate_root".to_string(),
        };

        states::store_state(&test_db.pool, &test_state.state_root, test_state.slot).await;

        let state = beacon_store.last_stored_state().await;
        assert_eq!(Some(test_state), state);
    }
}
