/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Executable storage and scoring contracts for Hudi's proposed native text
//! index. This is intentionally a small prototype, not a production index.

use std::error::Error;
use std::fmt::{Display, Formatter};

pub const SEGMENT_MAGIC: [u8; 8] = *b"HUDIFTS1";
pub const FORMAT_VERSION_V1: u16 = 1;
pub const FEATURE_POSITIONS: u16 = 1 << 0;
pub const MAX_HEADER_LENGTH: u32 = 16 * 1024 * 1024;
pub const FIXED_HEADER_LENGTH: usize = 20;

const KNOWN_V1_FEATURES: u16 = FEATURE_POSITIONS;

/// Stable logical address used to materialize a matching Hudi record.
///
/// `row_position_hint` is only an optimization and is valid solely while the
/// source-slice fingerprint in the segment descriptor is an exact match.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DocumentAddress {
    pub source_ordinal: u32,
    pub partition_path: Vec<u8>,
    pub file_id: Vec<u8>,
    pub record_key: Vec<u8>,
    pub row_position_hint: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentStatistics {
    pub document_count: u64,
    pub total_token_count: u64,
    pub source_count: u32,
}

impl SegmentStatistics {
    pub fn validate(self) -> Result<(), TextIndexError> {
        if self.document_count == 0 && self.total_token_count != 0 {
            return Err(TextIndexError::InvalidStatistics);
        }
        if self.document_count > 0 && self.source_count == 0 {
            return Err(TextIndexError::InvalidStatistics);
        }
        Ok(())
    }

    pub fn average_document_length(self) -> f32 {
        if self.document_count == 0 {
            0.0
        } else {
            self.total_token_count as f32 / self.document_count as f32
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Bm25Parameters {
    pub k1: f32,
    pub b: f32,
}

impl Default for Bm25Parameters {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

impl Bm25Parameters {
    pub fn validate(self) -> Result<(), TextIndexError> {
        if !self.k1.is_finite()
            || self.k1 < 0.0
            || !self.b.is_finite()
            || !(0.0..=1.0).contains(&self.b)
        {
            return Err(TextIndexError::InvalidBm25Parameters);
        }
        Ok(())
    }

    pub fn inverse_document_frequency(document_count: u64, document_frequency: u64) -> f32 {
        if document_count == 0 {
            return 0.0;
        }
        let n = document_count as f64;
        let df = document_frequency.min(document_count) as f64;
        (((n - df + 0.5) / (df + 0.5)) + 1.0).ln() as f32
    }

    pub fn score_term(
        self,
        term_frequency: u32,
        document_length: u32,
        average_document_length: f32,
        inverse_document_frequency: f32,
    ) -> f32 {
        if term_frequency == 0 || average_document_length <= 0.0 {
            return 0.0;
        }
        let tf = term_frequency as f32;
        let normalization =
            1.0 - self.b + self.b * document_length as f32 / average_document_length;
        inverse_document_frequency * (tf * (self.k1 + 1.0)) / (tf + self.k1 * normalization)
    }

    /// A conservative upper bound suitable for block-max pruning when the
    /// block records its maximum term frequency and minimum document length.
    pub fn score_upper_bound(
        self,
        maximum_term_frequency: u32,
        minimum_document_length: u32,
        average_document_length: f32,
        inverse_document_frequency: f32,
    ) -> f32 {
        self.score_term(
            maximum_term_frequency,
            minimum_document_length,
            average_document_length,
            inverse_document_frequency,
        )
    }
}

/// Fixed portion of every native index file. Multi-byte integers are little
/// endian. The variable header immediately follows these 20 bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentEnvelope {
    pub format_version: u16,
    pub features: u16,
    pub variable_header_length: u32,
    pub variable_header_checksum: u32,
}

impl SegmentEnvelope {
    pub fn new(features: u16, variable_header_length: u32, variable_header_checksum: u32) -> Self {
        Self {
            format_version: FORMAT_VERSION_V1,
            features,
            variable_header_length,
            variable_header_checksum,
        }
    }

    pub fn validate(self) -> Result<(), TextIndexError> {
        if self.format_version != FORMAT_VERSION_V1 {
            return Err(TextIndexError::UnsupportedFormatVersion(
                self.format_version,
            ));
        }
        let unknown = self.features & !KNOWN_V1_FEATURES;
        if unknown != 0 {
            return Err(TextIndexError::UnsupportedFeatures(unknown));
        }
        if self.variable_header_length > MAX_HEADER_LENGTH {
            return Err(TextIndexError::HeaderTooLarge(self.variable_header_length));
        }
        Ok(())
    }

    pub fn encode(self) -> Result<[u8; FIXED_HEADER_LENGTH], TextIndexError> {
        self.validate()?;
        let mut bytes = [0; FIXED_HEADER_LENGTH];
        bytes[0..8].copy_from_slice(&SEGMENT_MAGIC);
        bytes[8..10].copy_from_slice(&self.format_version.to_le_bytes());
        bytes[10..12].copy_from_slice(&self.features.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.variable_header_length.to_le_bytes());
        bytes[16..20].copy_from_slice(&self.variable_header_checksum.to_le_bytes());
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, TextIndexError> {
        if bytes.len() < FIXED_HEADER_LENGTH {
            return Err(TextIndexError::TruncatedEnvelope(bytes.len()));
        }
        if bytes[0..8] != SEGMENT_MAGIC[..] {
            return Err(TextIndexError::InvalidMagic);
        }
        let envelope = Self {
            format_version: u16::from_le_bytes([bytes[8], bytes[9]]),
            features: u16::from_le_bytes([bytes[10], bytes[11]]),
            variable_header_length: u32::from_le_bytes([
                bytes[12], bytes[13], bytes[14], bytes[15],
            ]),
            variable_header_checksum: u32::from_le_bytes([
                bytes[16], bytes[17], bytes[18], bytes[19],
            ]),
        };
        envelope.validate()?;
        Ok(envelope)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TextIndexError {
    InvalidMagic,
    TruncatedEnvelope(usize),
    UnsupportedFormatVersion(u16),
    UnsupportedFeatures(u16),
    HeaderTooLarge(u32),
    InvalidStatistics,
    InvalidBm25Parameters,
}

impl Display for TextIndexError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidMagic => write!(formatter, "invalid Hudi text-index magic"),
            Self::TruncatedEnvelope(length) => {
                write!(formatter, "truncated envelope: {length} bytes")
            }
            Self::UnsupportedFormatVersion(version) => {
                write!(formatter, "unsupported format version: {version}")
            }
            Self::UnsupportedFeatures(features) => {
                write!(formatter, "unsupported feature bits: {features:#06x}")
            }
            Self::HeaderTooLarge(length) => {
                write!(formatter, "variable header is too large: {length} bytes")
            }
            Self::InvalidStatistics => write!(formatter, "invalid segment statistics"),
            Self::InvalidBm25Parameters => write!(formatter, "invalid BM25 parameters"),
        }
    }
}

impl Error for TextIndexError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_round_trips() {
        let expected = SegmentEnvelope::new(FEATURE_POSITIONS, 4096, 0xfeed_beef);
        assert_eq!(
            SegmentEnvelope::decode(&expected.encode().unwrap()).unwrap(),
            expected
        );
    }

    #[test]
    fn envelope_rejects_unknown_versions_and_features() {
        let mut bytes = SegmentEnvelope::new(0, 0, 0).encode().unwrap();
        bytes[8..10].copy_from_slice(&2_u16.to_le_bytes());
        assert_eq!(
            SegmentEnvelope::decode(&bytes),
            Err(TextIndexError::UnsupportedFormatVersion(2))
        );

        bytes[8..10].copy_from_slice(&FORMAT_VERSION_V1.to_le_bytes());
        bytes[10..12].copy_from_slice(&0x8000_u16.to_le_bytes());
        assert_eq!(
            SegmentEnvelope::decode(&bytes),
            Err(TextIndexError::UnsupportedFeatures(0x8000))
        );
    }

    #[test]
    fn envelope_rejects_bad_input() {
        assert_eq!(
            SegmentEnvelope::decode(&[0; 4]),
            Err(TextIndexError::TruncatedEnvelope(4))
        );
        let mut bytes = SegmentEnvelope::new(0, 0, 0).encode().unwrap();
        bytes[0] = b'X';
        assert_eq!(
            SegmentEnvelope::decode(&bytes),
            Err(TextIndexError::InvalidMagic)
        );
        assert_eq!(
            SegmentEnvelope::new(0, MAX_HEADER_LENGTH + 1, 0).encode(),
            Err(TextIndexError::HeaderTooLarge(MAX_HEADER_LENGTH + 1))
        );
    }

    #[test]
    fn statistics_are_consistent() {
        let statistics = SegmentStatistics {
            document_count: 4,
            total_token_count: 20,
            source_count: 2,
        };
        assert_eq!(statistics.average_document_length(), 5.0);
        assert_eq!(statistics.validate(), Ok(()));
        assert_eq!(
            SegmentStatistics {
                document_count: 1,
                total_token_count: 0,
                source_count: 1,
            }
            .validate(),
            Ok(())
        );
        assert_eq!(
            SegmentStatistics {
                document_count: 0,
                total_token_count: 1,
                source_count: 1,
            }
            .validate(),
            Err(TextIndexError::InvalidStatistics)
        );
    }

    #[test]
    fn bm25_matches_the_declared_formula() {
        let parameters = Bm25Parameters::default();
        let idf = Bm25Parameters::inverse_document_frequency(100, 10);
        let expected = idf * (3.0 * 2.2) / (3.0 + 1.2 * (0.25 + 0.75 * 80.0 / 50.0));
        assert!((parameters.score_term(3, 80, 50.0, idf) - expected).abs() < 1e-6);
        assert_eq!(parameters.score_term(0, 80, 50.0, idf), 0.0);
    }

    #[test]
    fn block_bound_is_conservative() {
        let parameters = Bm25Parameters::default();
        let idf = Bm25Parameters::inverse_document_frequency(1_000, 10);
        let bound = parameters.score_upper_bound(10, 20, 100.0, idf);
        assert!(parameters.score_term(8, 25, 100.0, idf) <= bound);
    }

    #[test]
    fn bm25_parameters_are_validated() {
        assert_eq!(Bm25Parameters::default().validate(), Ok(()));
        assert_eq!(
            Bm25Parameters { k1: -1.0, b: 0.75 }.validate(),
            Err(TextIndexError::InvalidBm25Parameters)
        );
        assert_eq!(
            Bm25Parameters { k1: 1.2, b: 1.1 }.validate(),
            Err(TextIndexError::InvalidBm25Parameters)
        );
    }
}
