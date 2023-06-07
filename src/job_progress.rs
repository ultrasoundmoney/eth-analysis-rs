//! Job Progress
//! ============
//! Small module to help with tracking the progress of long running jobs. Uses the DB key value
//! store and a progress value.

use serde::{de::DeserializeOwned, Serialize};

use crate::key_value_store::KeyValueStore;

pub struct JobProgress<'a, A: Serialize + DeserializeOwned> {
    key_value_store: &'a dyn KeyValueStore,
    key: &'static str,
    phantom: std::marker::PhantomData<A>,
}

impl<A: Serialize + DeserializeOwned> JobProgress<'_, A> {
    pub fn new<'a>(
        key: &'static str,
        key_value_store: &'a impl KeyValueStore,
    ) -> JobProgress<'a, A> {
        JobProgress {
            key,
            key_value_store,
            phantom: std::marker::PhantomData,
        }
    }

    pub async fn get(&self) -> Option<A> {
        self.key_value_store
            .get_value(self.key)
            .await
            .map(|value| serde_json::from_value(value).unwrap())
    }

    pub async fn set(&self, value: &A) {
        self.key_value_store
            .set_value(self.key, &serde_json::to_value(value).unwrap())
            .await
    }
}
