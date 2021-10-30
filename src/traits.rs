use crate::error::SumkinResult;
use async_trait::async_trait;
use derive_getters::Getters;
use crate::Revision;
use sqlx::FromRow;

#[derive(Debug, Getters, FromRow, Clone)]
pub struct KeyValue {
    #[sqlx(rename = "name")]
    key: String,
    create_revision: Revision,
    #[sqlx(rename = "theid")]
    mod_revision: Revision,
    #[sqlx(default)]
    value: Option<Vec<u8>>,
    #[sqlx(default)]
    lease: Option<i64>

}

#[async_trait]
pub trait Backend {
    async fn size(&self) -> SumkinResult<u64>;
    async fn current_revision(&self) -> SumkinResult<Revision>;
    async fn count(&self, prefix: &str) -> SumkinResult<u64>;
    async fn put(&self, name: &str, value: &[u8]) -> SumkinResult<Revision>;
    async fn get(&self, name: &str, revision: Option<Revision>) -> SumkinResult<Option<KeyValue>> {
        if let Some(_r) = revision {
            unimplemented!();
        } else {
            let kv = self.list_current(name, 1, false).await?;
            Ok(kv.into_iter().next())
        }
    }
    async fn list_current(&self, prefix: &str, limit: i64, include_deleted: bool) -> SumkinResult<Vec<KeyValue>>;

    async fn delete(&self, name: &str) -> SumkinResult<Revision>;
    //async fn get_revision(&self, revision: i64) -> SumkinResult<()>;
    //async fn get(key: &str, revision: i64) -> SumkinResult<KeyValue>;
    //async fn create(key: &str, value: Vec<u8>, lease: i64) -> SumkinResult<i64>;
}
