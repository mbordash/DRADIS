//! Durable identities for requests whose acceptance is not yet known.
use anyhow::{Context, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::{sqlite::SqliteConnectOptions, Row, SqlitePool};
use std::path::Path;
#[cfg(test)]
use std::str::FromStr;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JournalOrder {
    pub order_hash: String,
    pub order_id: Option<String>,
    pub funder: String,
    pub token_id: String,
    pub side: u8,
    pub signed_quantity: Decimal,
    pub maker_amount: String,
    pub taker_amount: String,
}

/// Explicit owner supplied by the strategy that submitted a working order.
/// This record is sufficient to restore its guard after a process restart.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TrackedOrder {
    pub order_id: String,
    pub squadron: String,
    pub strategy: String,
    pub token_id: String,
    pub side: u8,
    pub side_label: String,
    pub quantity: Decimal,
    pub limit_price: Decimal,
    pub entry_price: Decimal,
    pub entry_fee: Decimal,
    pub exit_fee: Decimal,
    pub market_name: String,
    pub pair_token: Option<String>,
    pub close_time: Option<chrono::DateTime<chrono::Utc>>,
    pub booked_cumulative: Decimal,
    pub initial_filled: Decimal,
    pub initial_notional: Decimal,
}

pub struct OrderJournal {
    pool: SqlitePool,
}

impl OrderJournal {
    pub async fn open(state_dir: &Path) -> Result<Self> {
        tokio::fs::create_dir_all(state_dir)
            .await
            .context("create CLOB journal directory")?;
        let options = SqliteConnectOptions::new()
            .filename(state_dir.join("clob-order-journal.sqlite"))
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .synchronous(sqlx::sqlite::SqliteSynchronous::Full)
            .busy_timeout(std::time::Duration::from_secs(5));
        Self::connect(options).await
    }

    #[cfg(test)]
    pub async fn memory() -> Result<Self> {
        Self::connect(SqliteConnectOptions::from_str("sqlite::memory:")?).await
    }

    async fn connect(options: SqliteConnectOptions) -> Result<Self> {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS clob_order_journal (order_hash TEXT PRIMARY KEY, token_id TEXT NOT NULL, funder TEXT NOT NULL, state TEXT NOT NULL, record TEXT NOT NULL)")
            .execute(&pool).await?;
        sqlx::query("CREATE UNIQUE INDEX IF NOT EXISTS clob_token_claim ON clob_order_journal(funder,token_id) WHERE state IN ('prepared','uncertain','recovering','accepted')")
            .execute(&pool).await?;
        sqlx::query("CREATE TABLE IF NOT EXISTS clob_tracked_orders(funder TEXT NOT NULL,order_id TEXT NOT NULL,owner TEXT NOT NULL,PRIMARY KEY(funder,order_id))").execute(&pool).await?;
        Ok(Self { pool })
    }

    pub async fn track(&self, funder: &str, owner: &TrackedOrder) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let exists = sqlx::query("SELECT record FROM clob_order_journal WHERE funder=?")
            .bind(funder)
            .fetch_all(&mut *tx)
            .await?;
        let mut matched = false;
        for row in exists {
            let record: JournalOrder = serde_json::from_str(row.try_get::<&str, _>("record")?)?;
            if record.order_id.as_deref() == Some(owner.order_id.as_str())
                && record.token_id == owner.token_id
                && record.side == owner.side
            {
                matched = true;
                break;
            }
        }
        anyhow::ensure!(
            matched,
            "tracked order is not a recorded order of this wallet"
        );
        if let Some(previous) = sqlx::query_scalar::<_, String>(
            "SELECT owner FROM clob_tracked_orders WHERE funder=? AND order_id=?",
        )
        .bind(funder)
        .bind(&owner.order_id)
        .fetch_optional(&mut *tx)
        .await?
        {
            let previous: TrackedOrder = serde_json::from_str(&previous)?;
            anyhow::ensure!(
                previous.squadron == owner.squadron
                    && previous.strategy == owner.strategy
                    && previous.token_id == owner.token_id,
                "tracked order ownership cannot change"
            );
        }
        sqlx::query("INSERT INTO clob_tracked_orders(funder,order_id,owner) VALUES(?,?,?) ON CONFLICT(funder,order_id) DO UPDATE SET owner=excluded.owner")
            .bind(funder).bind(&owner.order_id).bind(serde_json::to_string(owner)?).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn tracked(&self, funder: &str, squadron: &str) -> Result<Vec<TrackedOrder>> {
        let rows =
            sqlx::query_scalar::<_, String>("SELECT owner FROM clob_tracked_orders WHERE funder=?")
                .bind(funder)
                .fetch_all(&self.pool)
                .await?;
        rows.into_iter()
            .map(|value| serde_json::from_str::<TrackedOrder>(&value))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map(|owners| {
                owners
                    .into_iter()
                    .filter(|owner| owner.squadron == squadron)
                    .collect()
            })
            .map_err(Into::into)
    }
    pub async fn finish_tracked(&self, funder: &str, id: &str) -> Result<()> {
        sqlx::query("DELETE FROM clob_tracked_orders WHERE funder=? AND order_id=?")
            .bind(funder)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// One transaction claims every token before a batch can reach the network.
    pub async fn prepare(&self, records: &[JournalOrder]) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        for record in records {
            sqlx::query("INSERT INTO clob_order_journal(order_hash,token_id,funder,state,record) VALUES (?,?,?,'prepared',?)")
                .bind(&record.order_hash).bind(&record.token_id).bind(&record.funder).bind(serde_json::to_string(record)?)
                .execute(&mut *tx).await.context("CLOB exposure is unresolved for this token; reconcile the recorded order before retrying")?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn outcome(&self, record: &JournalOrder, state: &str) -> Result<()> {
        anyhow::ensure!(
            matches!(
                state,
                "accepted" | "rejected" | "uncertain" | "recovering" | "resolved"
            ),
            "invalid journal transition"
        );
        let updated =
            sqlx::query("UPDATE clob_order_journal SET state=?,record=? WHERE order_hash=?")
                .bind(state)
                .bind(serde_json::to_string(record)?)
                .bind(&record.order_hash)
                .execute(&self.pool)
                .await?;
        anyhow::ensure!(
            updated.rows_affected() == 1,
            "CLOB journal order is missing"
        );
        Ok(())
    }

    pub async fn unresolved(&self, funder: &str) -> Result<Vec<JournalOrder>> {
        let rows = sqlx::query("SELECT record FROM clob_order_journal WHERE funder=? AND state IN ('prepared','uncertain','recovering','accepted') ORDER BY rowid")
            .bind(funder).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|row| Ok(serde_json::from_str(row.try_get::<&str, _>("record")?)?))
            .collect()
    }

    /// Call only after a known-id query was durably adopted by its consumer.
    pub async fn acknowledge_placement(&self, id: &str, funder: &str) -> Result<()> {
        let rows = sqlx::query(
            "SELECT order_hash,record FROM clob_order_journal WHERE funder=? AND state IN ('accepted','acknowledged','recovering')",
        )
        .bind(funder)
        .fetch_all(&self.pool)
        .await?;
        for row in rows {
            let record: JournalOrder = serde_json::from_str(row.try_get::<&str, _>("record")?)?;
            if record.order_id.as_deref() == Some(id) {
                sqlx::query("UPDATE clob_order_journal SET state='acknowledged' WHERE order_hash=? AND funder=? AND state IN ('accepted','recovering')")
                    .bind(&record.order_hash).bind(funder).execute(&self.pool).await?;
                return Ok(());
            }
        }
        anyhow::bail!("accepted order is missing from this wallet's journal")
    }

    pub async fn acknowledge_recovered(&self, hash: &str, funder: &str) -> Result<()> {
        let result=sqlx::query("UPDATE clob_order_journal SET state='acknowledged' WHERE order_hash=? AND funder=? AND state='recovering'")
            .bind(hash).bind(funder).execute(&self.pool).await?;
        anyhow::ensure!(
            result.rows_affected() == 1,
            "order has not been recovered by a known-id query"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn record(hash: &str, token: &str) -> JournalOrder {
        JournalOrder {
            order_hash: hash.into(),
            order_id: None,
            funder: "synthetic-wallet".into(),
            token_id: token.into(),
            side: 0,
            signed_quantity: Decimal::ONE,
            maker_amount: "500000".into(),
            taker_amount: "1000000".into(),
        }
    }
    #[tokio::test]
    async fn concurrent_exposure_claims_have_one_winner_and_batch_claims_are_atomic() -> Result<()>
    {
        let journal = OrderJournal::memory().await?;
        let a = record("a", "1");
        let b = record("b", "1");
        let a_records = [a.clone()];
        let b_records = [b.clone()];
        let (a_result, b_result) =
            tokio::join!(journal.prepare(&a_records), journal.prepare(&b_records));
        assert_ne!(a_result.is_ok(), b_result.is_ok());
        assert_eq!(journal.unresolved("synthetic-wallet").await?.len(), 1);
        assert!(journal
            .prepare(&[record("c", "2"), record("d", "1")])
            .await
            .is_err());
        assert!(journal.prepare(&[record("e", "2")]).await.is_ok());
        Ok(())
    }
    #[tokio::test]
    async fn prepared_and_uncertain_orders_survive_reopening() -> Result<()> {
        let dir = std::env::temp_dir().join(format!(
            "dradis-clob-journal-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        ));
        let journal = OrderJournal::open(&dir).await?;
        let order = record("a", "1");
        journal.prepare(&[order.clone()]).await?;
        journal.outcome(&order, "uncertain").await?;
        drop(journal);
        let reopened = OrderJournal::open(&dir).await?;
        assert_eq!(
            reopened.unresolved("synthetic-wallet").await?[0].order_hash,
            "a"
        );
        assert!(reopened.prepare(&[record("b", "1")]).await.is_err());
        assert!(reopened
            .acknowledge_recovered("a", "synthetic-wallet")
            .await
            .is_err());
        reopened.outcome(&order, "recovering").await?;
        reopened
            .acknowledge_recovered("a", "synthetic-wallet")
            .await?;
        reopened.prepare(&[record("b", "1")]).await?;
        Ok(())
    }
    #[tokio::test]
    async fn funder_claims_and_recovery_are_isolated() -> Result<()> {
        let journal = OrderJournal::memory().await?;
        let first = record("first", "1");
        let mut second = record("second", "1");
        second.funder = "different-wallet".into();
        journal.prepare(&[first]).await?;
        journal.prepare(&[second]).await?;
        assert_eq!(journal.unresolved("synthetic-wallet").await?.len(), 1);
        assert_eq!(journal.unresolved("different-wallet").await?.len(), 1);
        assert!(journal.unresolved("unrelated-wallet").await?.is_empty());
        Ok(())
    }
    #[tokio::test]
    async fn tracked_buy_and_sell_owners_survive_restart_without_changing_strategy() -> Result<()> {
        let dir = std::env::temp_dir().join(format!(
            "dradis-tracked-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        ));
        let journal = OrderJournal::open(&dir).await?;
        for (hash, side) in [("buy", 0), ("sell", 1)] {
            let mut entry = record(hash, hash);
            entry.side = side;
            entry.order_id = Some(hash.into());
            journal.prepare(&[entry.clone()]).await?;
            journal.outcome(&entry, "accepted").await?;
            let owner = TrackedOrder {
                order_id: hash.into(),
                squadron: "synthetic-squadron".into(),
                strategy: "synthetic-owner".into(),
                token_id: hash.into(),
                side,
                side_label: "YES".into(),
                quantity: Decimal::ONE,
                limit_price: Decimal::new(5, 1),
                entry_price: Decimal::new(4, 1),
                entry_fee: Decimal::ZERO,
                exit_fee: Decimal::ZERO,
                market_name: "Synthetic market".into(),
                pair_token: None,
                close_time: None,
                booked_cumulative: Decimal::ZERO,
                initial_filled: Decimal::ZERO,
                initial_notional: Decimal::ZERO,
            };
            journal.track("synthetic-wallet", &owner).await?;
            let mut wrong = owner.clone();
            wrong.strategy = "different-owner".into();
            assert!(journal.track("synthetic-wallet", &wrong).await.is_err());
            journal
                .acknowledge_placement(hash, "synthetic-wallet")
                .await?;
        }
        drop(journal);
        let restored = OrderJournal::open(&dir).await?;
        let owners = restored
            .tracked("synthetic-wallet", "synthetic-squadron")
            .await?;
        assert_eq!(owners.len(), 2);
        assert!(owners
            .iter()
            .all(|owner| owner.strategy == "synthetic-owner"));
        assert!(restored
            .tracked("different-wallet", "synthetic-squadron")
            .await?
            .is_empty());
        assert!(restored
            .tracked("synthetic-wallet", "different-squadron")
            .await?
            .is_empty());
        Ok(())
    }
}
