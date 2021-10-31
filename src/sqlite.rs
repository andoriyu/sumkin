use crate::error::SumkinResult;
use sqlx::{sqlite::{SqlitePoolOptions,SqliteConnectOptions, SqliteJournalMode}, SqlitePool, Executor};
use tracing::{info, debug};
use std::path::Path;
use std::fs::OpenOptions;
use crate::traits::{Backend,KeyValue};
use sqlx::{Row, Transaction, Sqlite};
use async_trait::async_trait;
use crate::Revision;

mod sql {
    pub static COLUMNS: &str = "kv.id AS theid, kv.name, kv.created, kv.deleted, kv.create_revision, kv.prev_revision, kv.lease, kv.value, kv.old_value";
    pub static SIZE_SQL: &str = "SELECT SUM(pgsize) FROM dbstat";
    pub static CURRENT_REVISION_SQL: &str = "SELECT MAX(rkv.id) AS id FROM sumkin AS rkv";
    pub static COMPACT_REV_SQL: &str = "SELECT MAX(crkv.prev_revision) AS prev_revision
		FROM sumkin AS crkv
		WHERE crkv.name = 'compact_rev_key'";
    pub static INSERT: &str = "INSERT INTO sumkin(name, created, deleted, create_revision, prev_revision, lease, value, old_value) values(?, ?, ?, ?, ?, ?, ?, ?)";
    lazy_static! {
        static ref GET_REVISION_SQL: String = format!("SELECT
			0, 0, %s
			FROM sumkin AS kv
			WHERE kv.id = ? {}", COLUMNS);
        pub static ref LIST_SQL: String = format!("SELECT ({}), ({}), {}
            FROM sumkin AS kv
            JOIN (
                SELECT MAX(mkv.id) AS id
                FROM sumkin AS mkv
                WHERE
                    mkv.name LIKE ?
                    {{}}
                GROUP BY mkv.name) maxkv
            ON maxkv.id = kv.id
            WHERE
                  (kv.deleted = 0 OR ?)
            ORDER BY kv.id ASC", CURRENT_REVISION_SQL, COMPACT_REV_SQL, COLUMNS);
        pub static ref COUNT_SQL: String = format!("SELECT ({}), COUNT(c.theid) as count FROM ({}) c", CURRENT_REVISION_SQL, LIST_SQL.replace("{}", ""));
        pub static ref GET_CURRENT_SQL: String = LIST_SQL.replace("{}", "");
    }

}

static SCHEMA:  &'static [&'static str] = &[
    r###"
        CREATE TABLE IF NOT EXISTS sumkin
			(
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				name INTEGER,
				created INTEGER,
				deleted INTEGER,
				create_revision INTEGER,
				prev_revision INTEGER,
				lease INTEGER,
				value BLOB,
				old_value BLOB
			)
    "###,
    "CREATE INDEX IF NOT EXISTS sumkin_name_index ON sumkin (name)",
    "CREATE INDEX IF NOT EXISTS sumkin_name_id_index ON sumkin (name,id)",
    "CREATE INDEX IF NOT EXISTS sumkin_id_deleted_index ON sumkin (id,deleted)",
    "CREATE INDEX IF NOT EXISTS sumkin_prev_revision_index ON sumkin (prev_revision)",
    "CREATE UNIQUE INDEX IF NOT EXISTS sumkin_name_prev_revision_uindex ON sumkin (name, prev_revision)",
];

fn create_file(path: &Path) -> SumkinResult<()> {
    OpenOptions::new().write(true)
                             .create_new(true)
                             .open(path)?;
    Ok(())
}

#[derive(Clone, Debug)]
pub struct SqliteBackend {
    pool: SqlitePool
}

impl SqliteBackend {
    pub async fn new(filepath: &Path, pool_options: SqlitePoolOptions) -> SumkinResult<Self> {
        info!("Connecting to datasource: {}", &filepath.display());

        create_file(filepath)?;

        let options = SqliteConnectOptions::new()
            .filename(filepath)
            .journal_mode(SqliteJournalMode::Wal)
            .shared_cache(true);
        let pool = pool_options.connect_with(options).await?;

        debug!("Connecting to datasource: {}", &filepath.display());
        Self::with_pool(pool).await
    }

    pub async fn with_pool(pool: SqlitePool) -> SumkinResult<Self> {
        info!("Configuring database table schema and indexes, this may take a moment...");

        for migration in SCHEMA {
            debug!("Running migration : {}", migration);
            pool.execute(*migration).await?;
        }
        info!("Backend setup complete.");
        Ok(Self {
            pool
        })

    }

    async fn get_with_tx(tx: &mut Transaction<'_, Sqlite>, name: &str, revision: Option<Revision>) -> SumkinResult<Option<KeyValue>> {
        if let Some(_r) = revision {
            unimplemented!();
        } else {
            let kv = Self::list_current_with_tx(tx, name, 1, false).await?;
            Ok(kv.into_iter().next())
        }

    }

    async fn list_current_with_tx(tx: &mut Transaction<'_, Sqlite>, prefix: &str, limit: i64, include_deleted: bool) -> SumkinResult<Vec<KeyValue>> {
        let sql = if limit > 0 {
            format!("{} LIMIT {}", sql::GET_CURRENT_SQL.as_str(), limit)
        } else {
            sql::GET_CURRENT_SQL.clone()
        };

        debug!("LIST SQL: {}", &sql);

        let rows = if prefix.ends_with('/') {
            let prefix = format!("{}%", prefix);
            sqlx::query_as::<_, KeyValue>(&sql)
                .bind(&prefix)
                .bind(include_deleted)
                .fetch_all(tx).await?
        } else {
            sqlx::query_as::<_, KeyValue>(&sql)
                .bind(prefix)
                .bind(include_deleted)
                .fetch_all(tx).await?
        };
        Ok(rows)
    }

    async fn insert_with_tx(tx: &mut Transaction<'_, Sqlite>, name: &str, created: bool, deleted: bool, create_revision: Revision, prev_revision: Option<Revision>, lease: Option<i64>, value: Option<&[u8]>, old_value: Option<Vec<u8>>) -> SumkinResult<Revision> {
        debug!("INSERT SQL: {}", sql::INSERT);
        let row = sqlx::query(sql::INSERT)
            .bind(name)
            .bind(created)
            .bind(deleted)
            .bind(create_revision)
            .bind(prev_revision)
            .bind(lease)
            .bind(value)
            .bind(old_value)
            .execute(tx).await?;
        Ok(row.last_insert_rowid())
    }


    async fn current_revision_with_tx(tx: &mut Transaction<'_, Sqlite>) -> SumkinResult<Revision> {
        debug!("CURRENT REVISION SQL: {}", sql::CURRENT_REVISION_SQL);
        let size: i64 = sqlx::query(sql::CURRENT_REVISION_SQL).fetch_one(tx).await?.try_get("id")?;
        Ok(size)
    }
}

#[async_trait]
impl Backend for SqliteBackend {
    async fn size(&self) -> SumkinResult<u64> {
        debug!("SIZE SQL: {}", sql::SIZE_SQL);
        let size: i64 = sqlx::query(sql::SIZE_SQL).fetch_one(&self.pool).await?.try_get(0)?;
        Ok(size as u64)
    }

    async fn current_revision(&self) -> SumkinResult<Revision> {
        debug!("CURRENT REVISION SQL: {}", sql::CURRENT_REVISION_SQL);
        let size: i64 = sqlx::query(sql::CURRENT_REVISION_SQL).fetch_one(&self.pool).await?.try_get("id")?;
        Ok(size)
    }

    async fn count(&self, prefix: &str) -> SumkinResult<u64> {
        debug!("COUNT SQL: {}", sql::COUNT_SQL.as_str());
        let row = if prefix.ends_with('/') {
            let prefix = format!("{}%", prefix);
            sqlx::query(sql::COUNT_SQL.as_str()).bind(&prefix).fetch_one(&self.pool).await?
        } else {

            sqlx::query(sql::COUNT_SQL.as_str()).bind(prefix).fetch_one(&self.pool).await?
        };
        let count: i64 = row.try_get("count")?;
        Ok(count as u64)
    }

    async fn put(&self, name: &str, value: &[u8]) -> SumkinResult<Revision> {
        let mut tx = self.pool.begin().await?;
        let next_revision = Self::current_revision_with_tx(&mut tx).await? + 1;
        let revision = if let Some(kv) = Self::get_with_tx(&mut tx, name, None).await? {
            debug!("Updating existing key: {}", name);
            Self::insert_with_tx(&mut tx, name, false, false, *kv.create_revision(),  None, None, Some(value), kv.value().clone()).await?
        } else {
            debug!("Creating new key: {}", name);
            Self::insert_with_tx(&mut tx, name, true, false, next_revision, None, None, Some(value), None).await?
        };
        tx.commit().await?;
        Ok(revision)
    }

    async fn list_current(&self, prefix: &str, limit: i64, include_deleted: bool) -> SumkinResult<Vec<KeyValue>> {
        let mut tx = self.pool.begin().await?;
        let kvs = Self::list_current_with_tx(&mut tx, prefix, limit, include_deleted).await?;
        tx.commit().await?;

        Ok(kvs)

    }

    async fn delete(&self, name: &str) -> SumkinResult<Revision> {
        let mut tx = self.pool.begin().await?;
        if let Some(kv) = Self::get_with_tx(&mut tx, name, None).await? {
            let revision = Self::insert_with_tx(&mut tx, name, false, true, 0,  None, None, None, kv.value().clone()).await?;
            tx.commit().await?;
            Ok(revision)
        } else {
            self.current_revision().await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tracing_test::traced_test;
    use tempfile::TempDir;

    fn get_random_datasource(dir: &tempfile::TempDir) -> String {
        let path = dir.path().join("state.db");

        path.to_string_lossy().to_owned().to_string()

    }

    #[tokio::test]
    #[traced_test]
    async fn super_basic() {
        let temp_dir = TempDir::new_in(".").expect("Failed to create temp dir");


        let datasource = get_random_datasource(&temp_dir);
        let pool_opts = SqlitePoolOptions::default();

        let backend = SqliteBackend::new(Path::new(datasource.as_str()), pool_opts).await.unwrap();

        let _og_size = backend.size().await.unwrap();

        let revision = backend.current_revision().await.unwrap();
        assert_eq!(0, revision);


        let count = backend.count("/").await.unwrap();
        assert_eq!(0, count);

        let list = backend.list_current("/", -1, false).await.unwrap();
        assert_eq!(0, list.len());
    }


    #[tokio::test]
    #[traced_test]
    async fn solo_key_crud() {
        let temp_dir = TempDir::new_in(".").expect("Failed to create temp dir");

        let datasource = get_random_datasource(&temp_dir);
        let pool_opts = SqlitePoolOptions::default();

        let backend = SqliteBackend::new(Path::new(datasource.as_str()), pool_opts).await.unwrap();

        let key = "/root/health";
        let value = b"OK";
        let null = backend.get(key, None).await.unwrap();
        assert!(null.is_none());

        let expected_revision = backend.put(key, value).await.unwrap();
        assert_eq!(1, expected_revision);


        let count = backend.count(key).await.unwrap();
        assert_eq!(1, count);

        let kv = backend.get(key, None).await.unwrap().unwrap();
        assert_eq!(kv.value().as_ref().unwrap(), value);
        assert_eq!(1, *kv.create_revision());
        assert_eq!(1, *kv.mod_revision());

        let new_value = b"NOT OKAY";

        let expected_revision = backend.put(key, new_value).await.unwrap();
        assert_eq!(2, expected_revision);

        let count = backend.count(key).await.unwrap();
        assert_eq!(1, count);

        let kv = backend.get(key, None).await.unwrap().unwrap();
        assert_eq!(kv.value().as_ref().unwrap(), new_value);
        assert_eq!(1, *kv.create_revision());
        assert_eq!(2, *kv.mod_revision());

        let revision = backend.delete(key).await.unwrap();
        assert_eq!(3, revision);

        let kv = backend.get(key, None).await.unwrap();
        assert!(kv.is_none());

        let revision = backend.delete(key).await.unwrap();
        assert_eq!(3, revision);
        let count = backend.count(key).await.unwrap();
        assert_eq!(0, count);
    }


    #[tokio::test]
    #[traced_test]
    async fn list_two() {
        let temp_dir = TempDir::new_in(".").expect("Failed to create temp dir");

        let datasource = get_random_datasource(&temp_dir);
        let pool_opts = SqlitePoolOptions::default();

        let backend = SqliteBackend::new(Path::new(datasource.as_str()), pool_opts).await.unwrap();

        let key_1 = "/root/health";
        let key_2 = "/root/status";
        let value = b"OK";

        let expected_revision = backend.put(key_1, value).await.unwrap();
        assert_eq!(1, expected_revision);


        let expected_revision = backend.put(key_2, value).await.unwrap();
        assert_eq!(2, expected_revision);

        let count = backend.count("/root/").await.unwrap();
        assert_eq!(2, count);

        let kvs = backend.list_current("/root/", -1, false).await.unwrap();
        assert_eq!(2, kvs.len());


        let kvs = backend.list_current("/root/", 1, false).await.unwrap();
        assert_eq!(1, kvs.len());


        let expected_revision = backend.put(key_1, value).await.unwrap();
        assert_eq!(3, expected_revision);


        let expected_revision = backend.put(key_2, value).await.unwrap();
        assert_eq!(4, expected_revision);

        let count = backend.count("/root/").await.unwrap();
        assert_eq!(2, count);

        let revision = backend.delete("/root").await.unwrap();
        assert_eq!(4, revision);

        let revision = backend.delete(key_1).await.unwrap();
        assert_eq!(5, revision);
        let revision = backend.delete(key_1).await.unwrap();
        assert_eq!(5, revision);
        let count = backend.count("/root/").await.unwrap();
        assert_eq!(1, count);
        let kvs = backend.list_current("/root/", -1, false).await.unwrap();
        assert_eq!(1, kvs.len());



        let revision = backend.delete(key_2).await.unwrap();
        assert_eq!(6, revision);
        let revision = backend.delete(key_2).await.unwrap();
        assert_eq!(6, revision);
        let count = backend.count("/root/").await.unwrap();
        assert_eq!(0, count);
        let kvs = backend.list_current("/root/", -1, false).await.unwrap();
        assert_eq!(0, kvs.len());
    }
}
