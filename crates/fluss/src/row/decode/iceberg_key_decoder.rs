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

//! Decoder for Iceberg-encoded primary key bytes (single key column).
//!
//! Reference: `org.apache.fluss.row.decode.iceberg.IcebergKeyDecoder`

use std::borrow::Cow;

use crate::error::Error::IllegalArgument;
use crate::error::Result;
use crate::metadata::DataType;
use crate::metadata::RowType;
use crate::row::datum::{Time, TimestampNtz};
use crate::row::decode::KeyDecoder;
use crate::row::{Datum, Decimal, GenericRow};

/// Decodes key bytes produced by [`crate::row::encode::IcebergKeyEncoder`].
pub struct IcebergKeyDecoder {
    reader: IcebergKeyFieldReader,
}

enum IcebergKeyFieldReader {
    IntOrDate,
    Time,
    BigInt,
    TimestampNtz,
    Decimal { precision: u32, scale: u32 },
    Utf8String,
    RawBytes,
}

impl IcebergKeyDecoder {
    pub fn new(row_type: &RowType, keys: &[String]) -> Result<Self> {
        if keys.len() != 1 {
            return Err(IllegalArgument {
                message: format!(
                    "Key fields must have exactly one field for iceberg format, but got: {keys:?}"
                ),
            });
        }

        let key_name = &keys[0];
        let key_index = match row_type.get_field_index(key_name) {
            Some(idx) => idx,
            None => {
                return Err(IllegalArgument {
                    message: format!("Field {key_name:?} not found in input row type {row_type:?}"),
                });
            }
        };

        let data_type = row_type.fields().get(key_index).unwrap().data_type();
        let reader = create_field_reader(data_type)?;
        Ok(IcebergKeyDecoder { reader })
    }

    fn decode_inner(&self, key_bytes: &[u8]) -> Result<Datum<'static>> {
        self.reader.read(key_bytes)
    }
}

impl KeyDecoder for IcebergKeyDecoder {
    fn decode_key(&self, key_bytes: &[u8]) -> Result<GenericRow<'static>> {
        let value = self.decode_inner(key_bytes)?;
        Ok(GenericRow::from_data(vec![value]))
    }
}

fn read_i64_le(key_bytes: &[u8]) -> Result<i64> {
    let a: [u8; 8] = key_bytes
        .get(..8)
        .ok_or_else(|| IllegalArgument {
            message: format!(
                "Iceberg key payload too short: need 8 bytes for fixed-width type, got {}",
                key_bytes.len()
            ),
        })?
        .try_into()
        .map_err(|_| IllegalArgument {
            message: "Iceberg key: invalid slice for i64".to_string(),
        })?;
    Ok(i64::from_le_bytes(a))
}

fn create_field_reader(field_type: &DataType) -> Result<IcebergKeyFieldReader> {
    match field_type {
        DataType::Int(_) | DataType::Date(_) => Ok(IcebergKeyFieldReader::IntOrDate),
        DataType::Time(_) => Ok(IcebergKeyFieldReader::Time),
        DataType::BigInt(_) => Ok(IcebergKeyFieldReader::BigInt),
        DataType::Timestamp(_) => Ok(IcebergKeyFieldReader::TimestampNtz),
        DataType::Decimal(d) => Ok(IcebergKeyFieldReader::Decimal {
            precision: d.precision(),
            scale: d.scale(),
        }),
        DataType::String(_) | DataType::Char(_) => Ok(IcebergKeyFieldReader::Utf8String),
        DataType::Binary(_) | DataType::Bytes(_) => Ok(IcebergKeyFieldReader::RawBytes),
        _ => Err(IllegalArgument {
            message: format!("Unsupported type for Iceberg key decoder: {field_type:?}"),
        }),
    }
}

impl IcebergKeyFieldReader {
    fn read(&self, key_bytes: &[u8]) -> Result<Datum<'static>> {
        match self {
            IcebergKeyFieldReader::IntOrDate => {
                let v = read_i64_le(key_bytes)?;
                Ok(Datum::Int32(v as i32))
            }
            IcebergKeyFieldReader::Time => {
                let micros = read_i64_le(key_bytes)?;
                let millis = (micros / 1000) as i32;
                Ok(Datum::Time(Time::new(millis)))
            }
            IcebergKeyFieldReader::BigInt => Ok(Datum::Int64(read_i64_le(key_bytes)?)),
            IcebergKeyFieldReader::TimestampNtz => {
                let micros = read_i64_le(key_bytes)?;
                let millis = micros / 1000;
                let nano_of_millis = ((micros % 1000) * 1000) as i32;
                let ts = TimestampNtz::from_millis_nanos(millis, nano_of_millis)?;
                Ok(Datum::TimestampNtz(ts))
            }
            IcebergKeyFieldReader::Decimal { precision, scale } => {
                let d = Decimal::from_unscaled_bytes(key_bytes, *precision, *scale)?;
                Ok(Datum::Decimal(d))
            }
            IcebergKeyFieldReader::Utf8String => {
                let s = String::from_utf8(key_bytes.to_vec()).map_err(|e| IllegalArgument {
                    message: format!("Iceberg key string is not valid UTF-8: {e}"),
                })?;
                Ok(Datum::String(Cow::Owned(s)))
            }
            IcebergKeyFieldReader::RawBytes => Ok(Datum::Blob(Cow::Owned(key_bytes.to_vec()))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::{DataType, DataTypes, TimestampType};
    use crate::row::datum::TimestampNtz;
    use crate::row::encode::{IcebergKeyEncoder, KeyEncoder};
    use crate::row::{Datum, Decimal, GenericRow, InternalRow};
    use bigdecimal::BigDecimal;

    #[test]
    fn single_key_field_requirement() {
        let row_type = RowType::with_data_types_and_field_names(
            vec![DataTypes::int(), DataTypes::string()],
            vec!["id", "name"],
        );

        assert!(IcebergKeyDecoder::new(&row_type, &["id".to_string()]).is_ok());

        let err = IcebergKeyDecoder::new(&row_type, &["id".to_string(), "name".to_string()])
            .err()
            .expect("expected error for multiple keys");
        assert!(err.to_string().contains("exactly one field"), "{err}");
    }

    #[test]
    fn round_trip_int() {
        let row_type = RowType::with_data_types(vec![DataTypes::int()]);
        let keys = vec!["f0".to_string()];
        let mut enc = IcebergKeyEncoder::create_key_encoder(&row_type, &keys).unwrap();
        let dec = IcebergKeyDecoder::new(&row_type, &keys).unwrap();

        for v in [0i32, -42, 42, i32::MAX, i32::MIN] {
            let row = GenericRow::from_data(vec![Datum::from(v)]);
            let bytes = enc.encode_key(&row).unwrap();
            let out = dec.decode_key(bytes.as_ref()).unwrap();
            assert_eq!(out.get_int(0).unwrap(), v);
        }
    }

    #[test]
    fn round_trip_long() {
        let row_type = RowType::with_data_types(vec![DataTypes::bigint()]);
        let keys = vec!["f0".to_string()];
        let mut enc = IcebergKeyEncoder::create_key_encoder(&row_type, &keys).unwrap();
        let dec = IcebergKeyDecoder::new(&row_type, &keys).unwrap();

        for v in [0i64, -999i64, 1234567890123456789i64, i64::MAX] {
            let row = GenericRow::from_data(vec![Datum::from(v)]);
            let bytes = enc.encode_key(&row).unwrap();
            let out = dec.decode_key(bytes.as_ref()).unwrap();
            assert_eq!(out.get_long(0).unwrap(), v);
        }
    }

    #[test]
    fn round_trip_string() {
        let row_type = RowType::with_data_types(vec![DataTypes::string()]);
        let keys = vec!["f0".to_string()];
        let mut enc = IcebergKeyEncoder::create_key_encoder(&row_type, &keys).unwrap();
        let dec = IcebergKeyDecoder::new(&row_type, &keys).unwrap();

        for s in ["", "a", "Hello", "Hello Iceberg!", "UTF-8: 你好世界"] {
            let row = GenericRow::from_data(vec![Datum::from(s)]);
            let bytes = enc.encode_key(&row).unwrap();
            let out = dec.decode_key(bytes.as_ref()).unwrap();
            assert_eq!(out.get_string(0).unwrap(), s);
        }
    }

    #[test]
    fn round_trip_timestamp() {
        let row_type = RowType::with_data_types(vec![DataType::Timestamp(
            TimestampType::with_nullable(false, 6).unwrap(),
        )]);
        let keys = vec!["f0".to_string()];
        let mut enc = IcebergKeyEncoder::create_key_encoder(&row_type, &keys).unwrap();
        let dec = IcebergKeyDecoder::new(&row_type, &keys).unwrap();

        let millis_values = [0i64, 1000i64, 1698235273182i64];
        let nanos_values = [0i32, 0i32, 123000i32];

        for i in 0..millis_values.len() {
            let ts = TimestampNtz::from_millis_nanos(millis_values[i], nanos_values[i]).unwrap();
            let row = GenericRow::from_data(vec![Datum::TimestampNtz(ts)]);
            let bytes = enc.encode_key(&row).unwrap();
            let out = dec.decode_key(bytes.as_ref()).unwrap();
            let got = out.get_timestamp_ntz(0, 6).unwrap();
            assert_eq!(got.get_millisecond(), millis_values[i]);
            assert_eq!(got.get_nano_of_millisecond(), nanos_values[i]);
        }
    }

    #[test]
    fn round_trip_decimal_high_precision() {
        let row_type = RowType::with_data_types(vec![DataTypes::decimal(38, 10)]);
        let keys = vec!["f0".to_string()];
        let mut enc = IcebergKeyEncoder::create_key_encoder(&row_type, &keys).unwrap();
        let dec = IcebergKeyDecoder::new(&row_type, &keys).unwrap();

        let bd: BigDecimal = "1234567890123456789012345678.1234567890".parse().unwrap();
        let d = Decimal::from_big_decimal(bd.clone(), 38, 10).unwrap();
        let row = GenericRow::from_data(vec![Datum::Decimal(d)]);
        let bytes = enc.encode_key(&row).unwrap();
        let out = dec.decode_key(bytes.as_ref()).unwrap();
        assert_eq!(out.get_decimal(0, 38, 10).unwrap().to_big_decimal(), bd);
    }
}
