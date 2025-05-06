pub mod doc;
pub mod errors;

use crate::doc::ForStore;
use crate::errors::StoreError;
use async_trait::async_trait;
use futures_util::stream::BoxStream;
use futures_util::{StreamExt};
use yrs::{Doc, Transact};

#[async_trait]
pub trait Store: Send + Sync {
    async fn start(&self) -> Result<(), StoreError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), StoreError> {
        Ok(())
    }
    async fn delete(&self) -> Result<(), StoreError>;
    async fn write(&self, update: &Vec<u8>) -> Result<(), StoreError>;
    async fn read(&self) -> Result<BoxStream<Result<(Vec<u8>, i64), StoreError>>, StoreError>;
    async fn read_payloads(&self) -> Result<BoxStream<Result<Vec<u8>, StoreError>>, StoreError>;
    async fn squash(&self) -> Result<(), StoreError>;
    /// save a YDoc updates
    /// # Arguments
    /// * `doc`: y doc
    async fn save(&self, doc: Doc) -> Result<(), StoreError> {
        let update = doc.get_update();
        self.write(&update).await
    }
    /// load and apply updates for doc
    /// # Arguments:
    ///* `doc`: apply updates YDoc
    async fn load(&self, doc: &Doc) -> Result<(), StoreError> {
        let mut txn = doc.transact_mut();
        let mut streams = self.read_payloads().await?;
        while let Some(result) = streams.next().await {
            let update = result?;
            doc.apply_update(&mut txn, &update)
                .map_err(StoreError::UpdateError)?;
        }
        Ok(())
    }
}