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

//! Value record batch implementation for LimitScan PK responses.
//!
//! Layout (DefaultValueRecordBatch):
//! - Length => Int32
//! - Magic => Int8
//! - RecordCount => Int32
//! - Records => [ValueRecord]
//!
//! ValueRecord layout:
//! - Length => Int32 (size without length field)
//! - SchemaId => Int16
//! - Value => BinaryRow bytes

use bytes::Bytes;
use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};

use crate::error::Result;
use crate::metadata::KvFormat;
use crate::record::kv::SchemaGetter;
use crate::row::{CompactedRow, RowDecoder, RowDecoderFactory};

const LENGTH_OFFSET: usize = 0;
const LENGTH_LENGTH: usize = 4;
const MAGIC_OFFSET: usize = LENGTH_OFFSET + LENGTH_LENGTH;
const MAGIC_LENGTH: usize = 1;
const RECORDS_COUNT_OFFSET: usize = MAGIC_OFFSET + MAGIC_LENGTH;
const RECORD_COUNT_LENGTH: usize = 4;
const RECORDS_OFFSET: usize = LENGTH_LENGTH + MAGIC_LENGTH + RECORD_COUNT_LENGTH;
const RECORD_BATCH_HEADER_SIZE: usize = RECORDS_OFFSET;
const BATCH_OVERHEAD: usize = LENGTH_OFFSET + LENGTH_LENGTH;

const VALUE_RECORD_LENGTH_OFFSET: usize = 0;
const VALUE_RECORD_LENGTH_LENGTH: usize = 4;
const VALUE_RECORD_SCHEMA_ID_OFFSET: usize = VALUE_RECORD_LENGTH_LENGTH;
const VALUE_RECORD_SCHEMA_ID_LENGTH: usize = 2;
const VALUE_RECORD_VALUE_OFFSET: usize =
    VALUE_RECORD_SCHEMA_ID_OFFSET + VALUE_RECORD_SCHEMA_ID_LENGTH;

#[derive(Debug, Clone)]
pub struct ValueRecord {
    schema_id: i16,
    value_bytes: Bytes,
    size_in_bytes: usize,
}

impl ValueRecord {
    pub fn schema_id(&self) -> i16 {
        self.schema_id
    }

    pub fn size_in_bytes(&self) -> usize {
        self.size_in_bytes
    }

    pub fn row<'a>(&'a self, decoder: &dyn RowDecoder) -> CompactedRow<'a> {
        decoder.decode(self.value_bytes.as_ref())
    }
}

pub struct DefaultValueRecordBatch {
    data: Bytes,
    position: usize,
}

impl DefaultValueRecordBatch {
    pub fn new(data: Bytes, position: usize) -> Self {
        Self { data, position }
    }

    pub fn size_in_bytes(&self) -> io::Result<usize> {
        if self.data.len() < self.position.saturating_add(LENGTH_LENGTH) {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes to read batch length",
            ));
        }
        let length_i32 = i32::from_le_bytes([
            self.data[self.position],
            self.data[self.position + 1],
            self.data[self.position + 2],
            self.data[self.position + 3],
        ]);
        if length_i32 < 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid batch length: {length_i32}"),
            ));
        }
        Ok((length_i32 as usize).saturating_add(BATCH_OVERHEAD))
    }

    pub fn magic(&self) -> io::Result<u8> {
        if self.data.len() < self.position.saturating_add(MAGIC_OFFSET + 1) {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes to read magic",
            ));
        }
        Ok(self.data[self.position + MAGIC_OFFSET])
    }

    pub fn record_count(&self) -> io::Result<i32> {
        if self.data.len() < self.position.saturating_add(RECORDS_COUNT_OFFSET + 4) {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "Not enough bytes to read record count",
            ));
        }
        Ok(i32::from_le_bytes([
            self.data[self.position + RECORDS_COUNT_OFFSET],
            self.data[self.position + RECORDS_COUNT_OFFSET + 1],
            self.data[self.position + RECORDS_COUNT_OFFSET + 2],
            self.data[self.position + RECORDS_COUNT_OFFSET + 3],
        ]))
    }

    pub fn records(&self, read_context: &ValueRecordReadContext) -> Result<Vec<ValueRecord>> {
        let record_count = self.record_count()?;
        if record_count <= 0 {
            return Ok(Vec::new());
        }

        let mut records = Vec::with_capacity(record_count as usize);
        let mut position = self.position + RECORD_BATCH_HEADER_SIZE;
        let size_in_bytes = self.size_in_bytes()?;
        let end = self.position.saturating_add(size_in_bytes);

        for _ in 0..record_count {
            if position.saturating_add(VALUE_RECORD_LENGTH_LENGTH) > end {
                return Err(crate::error::Error::IoUnexpectedError {
                    message: "Not enough bytes to read value record length".to_string(),
                    source: io::Error::new(io::ErrorKind::UnexpectedEof, "value record length"),
                });
            }
            let size_without_length = i32::from_le_bytes([
                self.data[position + VALUE_RECORD_LENGTH_OFFSET],
                self.data[position + VALUE_RECORD_LENGTH_OFFSET + 1],
                self.data[position + VALUE_RECORD_LENGTH_OFFSET + 2],
                self.data[position + VALUE_RECORD_LENGTH_OFFSET + 3],
            ]);
            if size_without_length < VALUE_RECORD_SCHEMA_ID_LENGTH as i32 {
                return Err(crate::error::Error::IoUnexpectedError {
                    message: format!("Invalid value record length: {size_without_length}"),
                    source: io::Error::new(io::ErrorKind::InvalidData, "value record length"),
                });
            }
            let size_without_length = size_without_length as usize;
            let record_size = size_without_length + VALUE_RECORD_LENGTH_LENGTH;
            let record_end = position.saturating_add(record_size);
            if record_end > end {
                return Err(crate::error::Error::IoUnexpectedError {
                    message: "Not enough bytes to read value record".to_string(),
                    source: io::Error::new(io::ErrorKind::UnexpectedEof, "value record"),
                });
            }

            let schema_id = i16::from_le_bytes([
                self.data[position + VALUE_RECORD_SCHEMA_ID_OFFSET],
                self.data[position + VALUE_RECORD_SCHEMA_ID_OFFSET + 1],
            ]);
            let value_len = size_without_length - VALUE_RECORD_SCHEMA_ID_LENGTH;
            let value_start = position + VALUE_RECORD_VALUE_OFFSET;
            let value_end = value_start + value_len;
            let value_bytes = self.data.slice(value_start..value_end);

            // Validate decoder exists (side-effect: cache)
            let _ = read_context.get_row_decoder(schema_id)?;

            records.push(ValueRecord {
                schema_id,
                value_bytes,
                size_in_bytes: record_size,
            });
            position = record_end;
        }

        Ok(records)
    }
}

pub struct ValueRecordReadContext {
    kv_format: KvFormat,
    schema_getter: Arc<dyn SchemaGetter>,
    row_decoder_cache: Mutex<HashMap<i16, Arc<dyn RowDecoder>>>,
}

impl ValueRecordReadContext {
    pub fn new(kv_format: KvFormat, schema_getter: Arc<dyn SchemaGetter>) -> Self {
        Self {
            kv_format,
            schema_getter,
            row_decoder_cache: Mutex::new(HashMap::new()),
        }
    }

    pub fn get_row_decoder(&self, schema_id: i16) -> Result<Arc<dyn RowDecoder>> {
        {
            let cache = self
                .row_decoder_cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(decoder) = cache.get(&schema_id) {
                return Ok(Arc::clone(decoder));
            }
        }

        let schema = self.schema_getter.get_schema(schema_id)?;
        let row_type = schema.row_type().clone();
        let decoder = RowDecoderFactory::create(self.kv_format.clone(), row_type)?;

        {
            let mut cache = self
                .row_decoder_cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(existing) = cache.get(&schema_id) {
                return Ok(Arc::clone(existing));
            }
            cache.insert(schema_id, Arc::clone(&decoder));
        }

        Ok(decoder)
    }
}
