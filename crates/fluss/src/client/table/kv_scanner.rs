// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::client::metadata::Metadata;
use crate::error::{ApiError, Error, Result};
use crate::metadata::{KvFormat, RowType, Schema, TableBucket, TableInfo, TablePath};
use crate::record::kv::SchemaGetter;
use crate::record::{DefaultValueRecordBatch, ValueRecordReadContext};
use crate::row::compacted::CompactedRow;
use crate::rpc::RpcClient;
use crate::rpc::message::LimitScanRequest;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

const KV_SCAN_RPC_TIMEOUT: Duration = Duration::from_secs(30);

/// One batch returned by PK table limit scan.
pub struct KvScanBatch {
    rows: Vec<Vec<u8>>,
    row_type: Arc<RowType>,
    has_more_results: bool,
}

impl KvScanBatch {
    fn new(rows: Vec<Vec<u8>>, row_type: Arc<RowType>, has_more_results: bool) -> Self {
        Self {
            rows,
            row_type,
            has_more_results,
        }
    }

    fn empty(row_type: Arc<RowType>) -> Self {
        Self {
            rows: Vec::new(),
            row_type,
            has_more_results: false,
        }
    }

    pub fn has_more_results(&self) -> bool {
        self.has_more_results
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn get_rows(&self) -> Vec<CompactedRow<'_>> {
        self.rows
            .iter()
            .map(|bytes| CompactedRow::from_bytes(&self.row_type, bytes))
            .collect()
    }
}

struct StaticSchemaGetter {
    schema: Arc<Schema>,
}

impl SchemaGetter for StaticSchemaGetter {
    fn get_schema(&self, _schema_id: i16) -> Result<Arc<Schema>> {
        Ok(Arc::clone(&self.schema))
    }
}

/// PK table limit scanner based on LimitScan RPC.
pub struct KvBatchScanner {
    rpc_client: Arc<RpcClient>,
    table_info: TableInfo,
    table_path: Arc<TablePath>,
    metadata: Arc<Metadata>,
    table_bucket: TableBucket,
    row_type: Arc<RowType>,
    read_context: ValueRecordReadContext,
    limit: i32,
    closed: bool,
}

impl KvBatchScanner {
    pub(super) fn new(
        rpc_client: Arc<RpcClient>,
        table_info: TableInfo,
        metadata: Arc<Metadata>,
        table_bucket: TableBucket,
        limit: i32,
    ) -> Self {
        let kv_format = table_info
            .get_table_config()
            .get_kv_format()
            .unwrap_or(KvFormat::COMPACTED);
        let schema_getter = Arc::new(StaticSchemaGetter {
            schema: Arc::new(table_info.get_schema().clone()),
        });
        let read_context = ValueRecordReadContext::new(kv_format, schema_getter);

        Self {
            rpc_client,
            table_path: Arc::new(table_info.table_path.clone()),
            row_type: Arc::new(table_info.row_type().clone()),
            table_info,
            metadata,
            table_bucket,
            read_context,
            limit,
            closed: false,
        }
    }

    pub async fn poll_batch(&mut self) -> Result<KvScanBatch> {
        if self.closed {
            return Ok(KvScanBatch::empty(Arc::clone(&self.row_type)));
        }

        let connection = self.connection_for_bucket().await?;
        let request = LimitScanRequest::new(
            self.table_bucket.table_id(),
            self.table_bucket.partition_id(),
            self.table_bucket.bucket_id(),
            self.limit,
        );
        let response = timeout(KV_SCAN_RPC_TIMEOUT, connection.request(request))
            .await
            .map_err(|_| Error::UnexpectedError {
                message: format!(
                    "LimitScan request timed out after {:?} for {}",
                    KV_SCAN_RPC_TIMEOUT, self.table_bucket
                ),
                source: None,
            })??;

        if let Some(error_code) = response.error_code {
            if error_code != 0 {
                return Err(Error::FlussAPIError {
                    api_error: ApiError {
                        code: error_code,
                        message: response.error_message.unwrap_or_default(),
                    },
                });
            }
        }

        if response.is_log_table.unwrap_or(false) {
            return Err(Error::UnsupportedOperation {
                message: format!(
                    "LimitScan returned log records for {}, expected KV records for PK table",
                    self.table_bucket
                ),
            });
        }

        let Some(records_bytes) = response.records else {
            self.closed = true;
            return Ok(KvScanBatch::empty(Arc::clone(&self.row_type)));
        };

        let rows = self.decode_limit_scan_records(records_bytes)?;

        self.closed = true;
        Ok(KvScanBatch::new(rows, Arc::clone(&self.row_type), false))
    }

    pub async fn keep_alive(&mut self) -> Result<()> {
        Ok(())
    }

    pub async fn close(&mut self) -> Result<()> {
        self.closed = true;
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn connection_for_bucket(&self) -> Result<crate::rpc::ServerConnection> {
        let cluster = self.metadata.get_cluster();
        let leader = self
            .metadata
            .leader_for(self.table_path.as_ref(), &self.table_bucket)
            .await?
            .ok_or_else(|| {
                Error::leader_not_available(format!(
                    "No leader found for table bucket: {}",
                    self.table_bucket
                ))
            })?;

        let tablet_server = cluster.get_tablet_server(leader.id()).ok_or_else(|| {
            Error::leader_not_available(format!(
                "Tablet server {} is not found in metadata cache",
                leader.id()
            ))
        })?;

        Ok(self.rpc_client.get_connection(tablet_server).await?)
    }

    fn decode_limit_scan_records(&self, records_bytes: Vec<u8>) -> Result<Vec<Vec<u8>>> {
        let batch = DefaultValueRecordBatch::new(bytes::Bytes::from(records_bytes), 0);
        let records = batch.records(&self.read_context)?;
        let mut rows = Vec::with_capacity(records.len());
        for record in records {
            let decoder = self.read_context.get_row_decoder(record.schema_id())?;
            let row = record.row(decoder.as_ref());
            rows.push(row.as_bytes().to_vec());
        }
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DataField, DataTypes, KvFormat};
    use crate::row::encode::{RowEncoder, RowEncoderFactory};
    use crate::row::{Datum, InternalRow};

    fn build_row_type() -> Arc<RowType> {
        Arc::new(RowType::new(vec![
            DataField::new("id", DataTypes::int(), None),
            DataField::new("name", DataTypes::string(), None),
        ]))
    }

    fn encode_compacted_row(row_type: &RowType, id: i32, name: &str) -> Vec<u8> {
        let mut encoder = RowEncoderFactory::create(KvFormat::COMPACTED, row_type.clone())
            .expect("create encoder");
        encoder.start_new_row().expect("start row");
        encoder
            .encode_field(0, Datum::Int32(id))
            .expect("encode id field");
        encoder
            .encode_field(1, Datum::String(name.into()))
            .expect("encode name field");
        encoder.finish_row().expect("finish row").to_vec()
    }

    #[test]
    fn kv_scan_batch_rows_are_decodable() {
        let row_type = build_row_type();
        let rows = vec![encode_compacted_row(&row_type, 1, "alice")];
        let batch = KvScanBatch::new(rows, Arc::clone(&row_type), false);

        assert_eq!(batch.len(), 1);
        assert!(!batch.is_empty());
        assert!(!batch.has_more_results());

        let decoded = batch.get_rows();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].get_int(0), 1);
        assert_eq!(decoded[0].get_string(1), "alice");
    }
}
