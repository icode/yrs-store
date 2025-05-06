use async_stream;
use async_trait::async_trait;
use chrono::NaiveDateTime;
use futures_util::stream::BoxStream;
use futures_util::TryStreamExt;
use sqlx::{PgPool, Row};
use yrs::Doc;
use yrs_store::doc::ForStore;
use yrs_store::errors::StoreError;
use yrs_store::Store;

pub struct PostgresStorage {
    document_id: i64,
    table_name: String,
    pool: PgPool,
    run_vacuum: bool,
}

impl PostgresStorage {
    pub async fn new(
        document_id: i64,
        table_name: String,
        pool: PgPool,
        run_vacuum: bool,
    ) -> Result<Self, sqlx::Error> {
        Ok(PostgresStorage {
            document_id,
            table_name,
            pool,
            run_vacuum,
        })
    }
}

fn map_sqlx_err(error: sqlx::Error) -> StoreError {
    StoreError::StorageError(Box::new(error))
}

#[async_trait]
impl Store for PostgresStorage {
    async fn delete(&self) -> Result<(), StoreError> {
        sqlx::query(&format!(
            "DELETE FROM {} WHERE document_id = $1",
            self.table_name
        ))
        .bind(self.document_id)
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(map_sqlx_err)
    }

    async fn write(&self, update: &Vec<u8>) -> Result<(), StoreError> {
        let document_id = self.document_id.clone();
        let now = chrono::Utc::now().naive_utc();
        let query_result = sqlx::query(&format!(
            "INSERT INTO {} (document_id, payload, timestamp) VALUES ($1, $2, $3)",
            self.table_name
        ))
        .bind(document_id)
        .bind(update)
        .bind(now)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;

        let rows_affected = query_result.rows_affected();

        // 检查影响行数是否等于 1
        if rows_affected != 1 {
            return Err(StoreError::WriteError(format!(
                "Expected 1 row affected for insert, but got {}",
                rows_affected
            )));
        }

        Ok(())
    }

    async fn read(&self) -> Result<BoxStream<Result<(Vec<u8>, i64), StoreError>>, StoreError> {
        let document_id = self.document_id;
        let table_name = self.table_name.clone();
        let pool = self.pool.clone();

        let stream = async_stream::stream! {
            let sql = format!(
                "SELECT payload, timestamp FROM {} WHERE document_id = $1 ORDER BY timestamp",
                table_name
            );

            let mut rows = sqlx::query(&sql)
                .bind(document_id)
                .fetch(&pool);

            while let Some(row) = rows.try_next().await.map_err(map_sqlx_err)? {
                let payload: Vec<u8> = row.get("payload");
                let timestamp_ndt: NaiveDateTime = row.get("timestamp");
                let timestamp_ms = timestamp_ndt.and_utc().timestamp_millis();
                yield Ok((payload, timestamp_ms));
            }
        };

        Ok(Box::pin(stream))
    }

    async fn read_payloads(&self) -> Result<BoxStream<Result<Vec<u8>, StoreError>>, StoreError> {
        let document_id = self.document_id;
        let table_name = self.table_name.clone();
        let pool = self.pool.clone();

        let stream = async_stream::stream! {
            let sql = format!(
                "SELECT payload FROM {} WHERE document_id = $1 ORDER BY timestamp",
                table_name
            );

            let mut rows = sqlx::query(&sql)
                .bind(document_id)
                .fetch(&pool);

            while let Some(row) = rows.try_next().await.map_err(map_sqlx_err)? {
                let payload: Vec<u8> = row.get("payload");
                yield Ok(payload);
            }
        };

        Ok(Box::pin(stream))
    }

    async fn squash(&self) -> Result<(), StoreError> {
        let doc = Doc::new();
        self.load(&doc).await?;
        let tx = self.pool.begin().await.map_err(map_sqlx_err)?;
        sqlx::query(&format!(
            "DELETE FROM {} WHERE document_id = $1",
            self.table_name
        ))
        .bind(self.document_id)
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;

        let squashed_update = doc.get_update();
        self.write(&squashed_update).await?;

        // 如果在事务超出范围之前都未调用，rollback则自动调用
        tx.commit().await.map_err(map_sqlx_err)?;

        if self.run_vacuum {
            // 回收死行占据的存储空间
            sqlx::query(&format!("VACUUM {}", self.table_name))
                .execute(&self.pool)
                .await
                .map_err(map_sqlx_err)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use std::env;
    use std::str::FromStr;
    use yrs::Any::{BigInt, Bool, Number};
    use yrs::{GetString, Map, Out, ReadTxn, Text, Transact, WriteTxn};

    // 验证数据库中的记录数量
    async fn assert_record_count(
        pool: &PgPool,
        expected_count: i64,
        message: &str,
        document_id: i64,
    ) -> Result<(), sqlx::Error> {
        let count = sqlx::query!(
            "SELECT COUNT(*) as count FROM document_updates WHERE document_id = $1",
            document_id
        )
        .fetch_one(pool)
        .await?
        .count
        .unwrap_or(0);
        assert_eq!(count, expected_count, "{}", message);
        Ok(())
    }

    // 清理测试数据
    async fn cleanup_test_data(pool: &PgPool, document_id: i64) -> Result<(), sqlx::Error> {
        sqlx::query!(
            "DELETE FROM document_updates WHERE document_id = $1",
            document_id
        )
        .execute(pool)
        .await
        .map(|_| ())
    }

    async fn create_test_pg_poll(document_id: i64) -> Result<PgPool, sqlx::Error> {
        let url = env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/test".to_string());
        // 连接到测试数据库
        let connect_options = PgConnectOptions::from_str(&url).expect("无法解析数据库连接字符串");

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect_with(connect_options)
            .await?;

        // 确保测试表存在
        sqlx::query(
            "
            CREATE TABLE IF NOT EXISTS document_updates (
                document_id BIGINT NOT NULL,
                payload BYTEA NOT NULL,
                timestamp TIMESTAMP NOT NULL
            )
        ",
        )
        .execute(&pool)
        .await?;

        // 清理可能存在的测试数据
        cleanup_test_data(&pool, document_id).await?;

        Ok(pool)
    }

    // 创建一个测试用的PostgresStore
    async fn create_test_store(document_id: i64) -> Result<(PostgresStorage, PgPool), sqlx::Error> {
        let pool = create_test_pg_poll(document_id).await?;

        // 创建测试用的PostgresStore
        let store = PostgresStorage::new(
            document_id,
            "document_updates".to_string(),
            pool.clone(),
            false,
        )
        .await?;

        Ok((store, pool))
    }

    // 创建文档并添加文本内容
    async fn create_doc_with_text(
        store: &PostgresStorage,
        text_content: &str,
    ) -> Result<Doc, StoreError> {
        let doc = Doc::new();
        {
            let mut txn = doc.transact_mut();
            let text = txn.get_or_insert_text("text");
            text.insert(&mut txn, 0, text_content);
        }
        let update = doc.get_update();
        store.write(&update).await?;
        Ok(doc)
    }

    // 向文档添加更新
    async fn update_doc_text(
        doc: &Doc,
        store: &PostgresStorage,
        position: u32,
        text_content: &str,
    ) -> Result<(), StoreError> {
        {
            let mut txn = doc.transact_mut();
            let text = txn.get_text("text").unwrap();
            text.insert(&mut txn, position, text_content);
        }
        let update = doc.get_update();
        store.write(&update).await
    }

    // 从文档中删除文本
    async fn remove_doc_text(
        doc: &Doc,
        store: &PostgresStorage,
        position: u32,
        length: u32,
    ) -> Result<(), StoreError> {
        {
            let mut txn = doc.transact_mut();
            let text = txn.get_text("text").unwrap();
            text.remove_range(&mut txn, position, length);
        }
        let update = doc.get_update();
        store.write(&update).await
    }

    // 验证文档文本内容
    fn assert_doc_text(doc: &Doc, expected_text: &str, message: &str) {
        let txn = doc.transact();
        let text = txn.get_text("text").unwrap();
        let content = text.get_string(&txn);
        assert_eq!(content, expected_text, "{}", message);
    }

    // 验证文档Map内容
    fn assert_doc_map(
        doc: &Doc,
        map_name: &str,
        expected_entries: &[(&str, serde_json::Value)],
        message: &str,
    ) {
        let txn = doc.transact();
        let map = txn.get_map(map_name).unwrap();

        for (key, expected_value) in expected_entries {
            match expected_value {
                serde_json::Value::String(expected_str) => {
                    let value = map.get(&txn, *key).unwrap().to_string(&txn);
                    assert_eq!(value, *expected_str, "{} - key: {}", message, key);
                }
                serde_json::Value::Number(expected_num) => {
                    if let Some(expected_i64) = expected_num.as_i64() {
                        let i64_value = match map.get(&txn, *key).unwrap() {
                            Out::Any(BigInt(v)) => v,
                            Out::Any(Number(v)) => v as i64,
                            _ => panic!("Expected Out::Any(BigInt)"),
                        };
                        assert_eq!(i64_value, expected_i64, "{} - key: {}", message, key);
                    } else if let Some(expected_f64) = expected_num.as_f64() {
                        // 将值转换为字符串并解析为f64进行比较
                        let f64_value = match map.get(&txn, *key).unwrap() {
                            Out::Any(BigInt(v)) => v as f64,
                            Out::Any(Number(v)) => v,
                            _ => panic!("Expected Out::Any(Number)"),
                        };
                        assert!(
                            (f64_value - expected_f64).abs() < f64::EPSILON,
                            "{} - key: {}",
                            message,
                            key
                        );
                    }
                }
                serde_json::Value::Bool(expected_bool) => {
                    // 将值转换为字符串并解析为布尔值进行比较
                    let bool_value = match map.get(&txn, *key).unwrap() {
                        Out::Any(Bool(v)) => v,
                        _ => panic!("Expected Out::Any(Bool)"),
                    };
                    assert_eq!(bool_value, *expected_bool, "{} - key: {}", message, key);
                }
                _ => {} // 忽略其他类型
            }
        }
    }

    // 创建新文档并应用更新
    async fn create_and_apply_doc(store: &PostgresStorage) -> Result<Doc, StoreError> {
        let doc = Doc::new();
        store.load(&doc).await?;
        Ok(doc)
    }

    // 向文档添加Map
    async fn add_map_to_doc(
        doc: &Doc,
        store: &PostgresStorage,
        map_name: &str,
        entries: &[(&str, serde_json::Value)],
    ) -> Result<(), StoreError> {
        {
            let mut txn = doc.transact_mut();
            let map = txn.get_or_insert_map(map_name);

            for (key, value) in entries {
                match value {
                    serde_json::Value::String(s) => {
                        map.insert(&mut txn, *key, s.as_str());
                    }
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            map.insert(&mut txn, *key, i);
                        } else if let Some(f) = n.as_f64() {
                            map.insert(&mut txn, *key, f);
                        }
                    }
                    serde_json::Value::Bool(b) => {
                        map.insert(&mut txn, *key, *b);
                    }
                    _ => {} // 忽略其他类型
                }
            }
        }
        let update = doc.get_update();
        store.write(&update).await
    }

    #[tokio::test]
    async fn test_squash_preserves_history() -> Result<(), Box<dyn std::error::Error>> {
        let document_id = 1;

        // 创建测试存储
        let (store, pool) = create_test_store(document_id).await?;

        // 创建一个YDoc并添加初始文本
        let doc = create_doc_with_text(&store, "Hello").await?;

        // 添加更多文本
        update_doc_text(&doc, &store, 5, ", World").await?;

        // 验证数据库中有两条记录
        assert_record_count(&pool, 2, "数据库中应该有两条更新记录", document_id).await?;

        // 执行squash操作
        store.squash().await?;

        // 验证数据库中现在只有一条记录
        assert_record_count(&pool, 1, "squash后数据库中应该只有一条记录", document_id).await?;

        // 创建一个新的YDoc并应用squash后的更新
        let new_doc = create_and_apply_doc(&store).await?;

        // 验证新文档包含所有历史更改
        assert_doc_text(
            &new_doc,
            "Hello, World",
            "squash后的文档应该包含所有历史更改",
        );

        // 清理测试数据
        cleanup_test_data(&pool, document_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_squash_with_multiple_updates() -> Result<(), Box<dyn std::error::Error>> {
        let document_id = 2;
        // 创建测试存储
        let (store, pool) = create_test_store(document_id).await?;

        // 创建一个YDoc并添加多次更新
        let doc = Doc::new();

        // 进行多次更新
        for i in 0..5 {
            {
                let mut txn = doc.transact_mut();
                let text = if i == 0 {
                    txn.get_or_insert_text("text")
                } else {
                    txn.get_text("text").unwrap()
                };

                let len = text.len(&txn);
                text.insert(&mut txn, len, &format!("Part {}", i));
            }
            // 存储更新
            let update = doc.get_update();
            store.write(&update).await?;
        }

        // 验证数据库中有5条记录
        assert_record_count(&pool, 5, "数据库中应该有5条更新记录", document_id).await?;

        // 执行squash操作
        store.squash().await?;

        // 验证数据库中现在只有一条记录
        assert_record_count(&pool, 1, "squash后数据库中应该只有一条记录", document_id).await?;

        // 创建一个新的YDoc并应用squash后的更新
        let new_doc = create_and_apply_doc(&store).await?;

        // 验证新文档包含所有历史更改
        assert_doc_text(
            &new_doc,
            "Part 0Part 1Part 2Part 3Part 4",
            "squash后的文档应该包含所有历史更改",
        );

        // 清理测试数据
        cleanup_test_data(&pool, document_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_squash_with_complex_operations() -> Result<(), Box<dyn std::error::Error>> {
        let document_id = 3;
        // 创建测试存储
        let (store, pool) = create_test_store(document_id).await?;

        // 创建一个YDoc并添加初始文本
        let doc = create_doc_with_text(&store, "Initial content").await?;

        // 添加更多文本
        update_doc_text(&doc, &store, 15, " with more text").await?;

        // 第三次更新：删除部分内容
        remove_doc_text(&doc, &store, 8, 8).await?; // 删除"content "

        // 执行squash操作
        store.squash().await?;

        // 创建一个新的YDoc并应用squash后的更新
        let new_doc = create_and_apply_doc(&store).await?;

        // 验证新文档包含所有历史更改，包括删除操作
        assert_doc_text(
            &new_doc,
            "Initial with more text",
            "squash后的文档应该正确应用所有操作，包括删除",
        );

        // 清理测试数据
        cleanup_test_data(&pool, document_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_apply_updates() -> Result<(), Box<dyn std::error::Error>> {
        let document_id = 4;
        // 创建测试存储
        let (store, pool) = create_test_store(document_id).await?;

        // 创建源文档并添加文本内容
        let source_doc = create_doc_with_text(&store, "Hello, World!").await?;

        let map_name = "map".to_string();
        // 添加 Map
        let entries = [
            ("key1", serde_json::json!("value1")),
            ("key2", serde_json::json!(42)),
        ];
        add_map_to_doc(&source_doc, &store, &map_name, &entries).await?;

        // 创建目标文档并应用更新
        let target_doc = create_and_apply_doc(&store).await?;

        // 验证目标文档包含所有更新

        // 验证文本
        assert_doc_text(&target_doc, "Hello, World!", "目标文档应该包含文本内容");

        // 验证 Map
        assert_doc_map(&target_doc, &map_name, &entries, "目标文档应该包含Map的值");

        // 清理测试数据
        cleanup_test_data(&pool, document_id).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_encode_state_as_update() -> Result<(), Box<dyn std::error::Error>> {
        let document_id = 5;
        // 创建测试存储
        let (store, pool) = create_test_store(document_id).await?;

        // 创建文档并添加内容
        let doc = Doc::new();

        // 添加内容
        {
            let mut txn = doc.transact_mut();
            let text = txn.get_or_insert_text("text");
            text.insert(&mut txn, 0, "测试文本内容");

            let map = txn.get_or_insert_map("map");
            map.insert(&mut txn, "key", "value");
        }

        // 使用encode_state_as_update存储状态
        store.save(doc.clone()).await?;

        // 验证数据已写入数据库
        assert_record_count(&pool, 1, "数据库中应该有一条记录", document_id).await?;

        // 创建新文档并应用更新
        let new_doc = Doc::new();
        store.load(&new_doc).await?;

        // 验证新文档包含所有内容
        let txn = new_doc.transact();

        // 验证文本
        let text = txn.get_text("text").unwrap();
        let content = text.get_string(&txn);
        assert_eq!(content, "测试文本内容", "新文档应该包含文本内容");

        // 验证map
        let map = txn.get_map("map").unwrap();
        let key_value = map.get(&txn, "key").unwrap().to_string(&txn);
        assert_eq!(key_value, "value", "新文档应该包含Map的key值");

        // 清理测试数据
        cleanup_test_data(&pool, document_id).await?;

        Ok(())
    }
}
