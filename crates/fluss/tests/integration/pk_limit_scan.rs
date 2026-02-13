/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use fluss::client::FlussConnection;
use fluss::config::Config;
use fluss::metadata::{TableBucket, TablePath};
use std::env;

fn required_env(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| panic!("Missing required env var: {key}"))
}

#[tokio::test]
#[ignore = "Manual test: requires external Fluss cluster and target PK table"]
async fn pk_limit_scan_against_external_cluster() {
    let bootstrap_servers = required_env("FLUSS_BOOTSTRAP_SERVERS");
    let database = required_env("FLUSS_DATABASE");
    let table_name = required_env("FLUSS_PK_TABLE");
    let bucket_id: i32 = env::var("FLUSS_BUCKET_ID")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let partition_id: Option<i64> = env::var("FLUSS_PARTITION_ID")
        .ok()
        .and_then(|v| v.parse().ok());
    let limit: i32 = env::var("FLUSS_SCAN_LIMIT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);
    let max_batches: usize = env::var("FLUSS_MAX_BATCHES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);

    let config = Config {
        bootstrap_servers,
        ..Default::default()
    };
    let conn = FlussConnection::new(config)
        .await
        .expect("Failed to create FlussConnection");

    let table_path = TablePath::new(database, table_name);
    let table = conn
        .get_table(&table_path)
        .await
        .expect("Failed to fetch target table");
    assert!(
        table.has_primary_key(),
        "Target table must be a PK table: {}",
        table_path
    );

    let table_info = table.get_table_info();
    let num_buckets = table_info.get_num_buckets();
    eprintln!(
        "[pk_limit_scan] table={} table_id={} num_buckets={} partitioned={} partition_id={:?}",
        table_path,
        table_info.get_table_id(),
        num_buckets,
        table_info.is_partitioned(),
        partition_id
    );
    assert!(
        bucket_id >= 0 && bucket_id < num_buckets,
        "FLUSS_BUCKET_ID {} out of range [0, {})",
        bucket_id,
        num_buckets
    );

    let table_bucket =
        TableBucket::new_with_partition(table_info.get_table_id(), partition_id, bucket_id);
    let mut scanner = table
        .new_scan()
        .limit(limit)
        .expect("invalid limit")
        .create_kv_batch_scanner(table_bucket)
        .expect("Failed to create kv batch scanner");

    let mut total_rows = 0usize;
    for batch_index in 0..max_batches {
        let batch = scanner
            .poll_batch()
            .await
            .expect("poll_batch failed against cluster");
        let rows = batch.len();
        total_rows += rows;
        eprintln!(
            "[pk_limit_scan] batch={} rows={} has_more_results={}",
            batch_index,
            rows,
            batch.has_more_results()
        );
        if !batch.has_more_results() {
            break;
        }
    }

    scanner.close().await.expect("close scanner failed");
    eprintln!(
        "[pk_limit_scan] done total_rows={} table={} bucket={} partition_id={:?} limit={}",
        total_rows, table_path, bucket_id, partition_id, limit
    );
}
