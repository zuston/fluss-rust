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

//! Key decoders (binary primary key → row).
//!
//! Mirrors `org.apache.fluss.row.decode` for lake-aware key formats.

mod iceberg_key_decoder;

use crate::error::{Error, Result};
use crate::metadata::{DataLakeFormat, RowType};
use crate::row::GenericRow;
pub use iceberg_key_decoder::IcebergKeyDecoder;

/// Decode primary key bytes to a one-column [`GenericRow`] holding the key fields.
///
/// Reference: `org.apache.fluss.row.decode.KeyDecoder`
pub trait KeyDecoder: Send + Sync {
    fn decode_key(&self, key_bytes: &[u8]) -> Result<GenericRow<'static>>;
}

/// Factory for [`KeyDecoder`] implementations aligned with [`crate::row::encode::KeyEncoderFactory`].
///
/// Reference: `org.apache.fluss.row.decode.KeyDecoder.ofPrimaryKeyDecoder` (subset implemented in Rust).
pub struct KeyDecoderFactory;

impl KeyDecoderFactory {
    /// Create a decoder for the given primary key field names and lake format.
    pub fn of(
        row_type: &RowType,
        key_fields: &[String],
        data_lake_format: &Option<DataLakeFormat>,
    ) -> Result<Box<dyn KeyDecoder>> {
        match data_lake_format {
            Some(DataLakeFormat::Iceberg) => Ok(Box::new(IcebergKeyDecoder::new(
                row_type, key_fields,
            )?)),
            Some(DataLakeFormat::Paimon) => Err(Error::UnsupportedOperation {
                message: "KeyDecoder for Paimon format is not yet implemented".to_string(),
            }),
            Some(DataLakeFormat::Lance) | None => Err(Error::UnsupportedOperation {
                message: "KeyDecoder for non-Iceberg formats is not yet implemented".to_string(),
            }),
        }
    }
}
