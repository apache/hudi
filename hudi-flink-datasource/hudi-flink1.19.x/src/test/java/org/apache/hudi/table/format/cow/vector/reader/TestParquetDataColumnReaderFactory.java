/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.format.cow.vector.reader;

import org.apache.flink.table.data.TimestampData;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for the {@link ParquetDataColumnReaderFactory} INT64 timestamp dispatch added when
 * vendoring Flink 2.1's nested-Parquet reader (FLINK-35702).
 *
 * <p>The factory is exercised end-to-end by integration tests through
 * {@link NestedPrimitiveColumnReader}; this unit test focuses on the small, deterministic piece
 * that was added by this PR — selecting the right {@code ParquetDataColumnReader} for each
 * supported INT64 TIMESTAMP encoding (modern {@link LogicalTypeAnnotation.TimestampLogicalTypeAnnotation}
 * MILLIS / MICROS / NANOS plus the legacy {@link OriginalType} encodings) and decoding values
 * using both the values-reader and dictionary code paths.
 */
class TestParquetDataColumnReaderFactory {

  // -----------------------------------------------------------------------------------------------
  // Type dispatch
  // -----------------------------------------------------------------------------------------------

  @Test
  void valuesReaderDispatchInt96TimestampUsesInt96Reader() {
    PrimitiveType type = Types.required(PrimitiveType.PrimitiveTypeName.INT96).named("ts");
    ParquetDataColumnReader reader =
        ParquetDataColumnReaderFactory.getDataColumnReaderByType(type, new StubValuesReader(), true);
    assertInstanceOf(ParquetDataColumnReaderFactory.TypesFromInt96PageReader.class, reader);
  }

  @Test
  void valuesReaderDispatchInt64WithoutAnnotationUsesDefaultReader() {
    PrimitiveType type = Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("plainLong");
    ParquetDataColumnReader reader =
        ParquetDataColumnReaderFactory.getDataColumnReaderByType(type, new StubValuesReader(), true);
    assertInstanceOf(ParquetDataColumnReaderFactory.DefaultParquetDataColumnReader.class, reader);
  }

  @Test
  void valuesReaderDispatchInt64TimestampMillisLogicalUsesInt64Reader() {
    PrimitiveType type =
        Types.required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("ts");
    ParquetDataColumnReader reader =
        ParquetDataColumnReaderFactory.getDataColumnReaderByType(type, new StubValuesReader(), true);
    assertInstanceOf(ParquetDataColumnReaderFactory.TypesFromInt64PageReader.class, reader);
  }

  @Test
  void valuesReaderDispatchInt64TimestampMicrosLogicalUsesInt64Reader() {
    PrimitiveType type =
        Types.required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("ts");
    ParquetDataColumnReader reader =
        ParquetDataColumnReaderFactory.getDataColumnReaderByType(type, new StubValuesReader(), true);
    assertInstanceOf(ParquetDataColumnReaderFactory.TypesFromInt64PageReader.class, reader);
  }

  @Test
  void valuesReaderDispatchInt64TimestampNanosLogicalUsesInt64Reader() {
    PrimitiveType type =
        Types.required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
            .named("ts");
    ParquetDataColumnReader reader =
        ParquetDataColumnReaderFactory.getDataColumnReaderByType(type, new StubValuesReader(), true);
    assertInstanceOf(ParquetDataColumnReaderFactory.TypesFromInt64PageReader.class, reader);
  }

  @Test
  void valuesReaderDispatchInt64LegacyTimestampMillisOriginalUsesInt64Reader() {
    PrimitiveType type =
        Types.required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(OriginalType.TIMESTAMP_MILLIS)
            .named("ts");
    ParquetDataColumnReader reader =
        ParquetDataColumnReaderFactory.getDataColumnReaderByType(type, new StubValuesReader(), true);
    assertInstanceOf(ParquetDataColumnReaderFactory.TypesFromInt64PageReader.class, reader);
  }

  @Test
  void valuesReaderDispatchInt64LegacyTimestampMicrosOriginalUsesInt64Reader() {
    PrimitiveType type =
        Types.required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(OriginalType.TIMESTAMP_MICROS)
            .named("ts");
    ParquetDataColumnReader reader =
        ParquetDataColumnReaderFactory.getDataColumnReaderByType(type, new StubValuesReader(), true);
    assertInstanceOf(ParquetDataColumnReaderFactory.TypesFromInt64PageReader.class, reader);
  }

  @Test
  void valuesReaderDispatchInt32DoesNotUseTimestampReader() {
    PrimitiveType type = Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("i");
    ParquetDataColumnReader reader =
        ParquetDataColumnReaderFactory.getDataColumnReaderByType(type, new StubValuesReader(), true);
    assertInstanceOf(ParquetDataColumnReaderFactory.DefaultParquetDataColumnReader.class, reader);
  }

  @Test
  void dictionaryReaderDispatchInt64TimestampMillisLogicalUsesInt64Reader() {
    PrimitiveType type =
        Types.required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("ts");
    ParquetDataColumnReader reader =
        ParquetDataColumnReaderFactory.getDataColumnReaderByTypeOnDictionary(
            type, new StubDictionary(), true);
    assertInstanceOf(ParquetDataColumnReaderFactory.TypesFromInt64PageReader.class, reader);
  }

  // -----------------------------------------------------------------------------------------------
  // INT64 → TimestampData decoding (per ChronoUnit, both UTC and local-time-zone branches)
  // -----------------------------------------------------------------------------------------------

  @Test
  void int64ReaderReadsTimestampMillisFromValuesReaderInUtc() {
    PrimitiveType type =
        Types.required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("ts");
    long epochMillis = 1_700_000_000_123L;
    ParquetDataColumnReader reader =
        ParquetDataColumnReaderFactory.getDataColumnReaderByType(
            type, new StubValuesReader(epochMillis), true);

    TimestampData ts = reader.readTimestamp();
    assertNotNull(ts);
    assertEquals(epochMillis, ts.getMillisecond());
    assertEquals(0, ts.getNanoOfMillisecond());
  }

  @Test
  void int64ReaderReadsTimestampMicrosFromValuesReaderInUtc() {
    PrimitiveType type =
        Types.required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
            .named("ts");
    long epochMicros = 1_700_000_000_123_456L;
    ParquetDataColumnReader reader =
        ParquetDataColumnReaderFactory.getDataColumnReaderByType(
            type, new StubValuesReader(epochMicros), true);

    TimestampData ts = reader.readTimestamp();
    assertNotNull(ts);
    assertEquals(epochMicros / 1_000L, ts.getMillisecond());
    // 456 microseconds remain → 456_000 nanoseconds within the millisecond
    assertEquals(456_000, ts.getNanoOfMillisecond());
  }

  @Test
  void int64ReaderReadsTimestampNanosFromValuesReaderInUtc() {
    PrimitiveType type =
        Types.required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
            .named("ts");
    long epochNanos = 1_700_000_000_123_456_789L;
    ParquetDataColumnReader reader =
        ParquetDataColumnReaderFactory.getDataColumnReaderByType(
            type, new StubValuesReader(epochNanos), true);

    TimestampData ts = reader.readTimestamp();
    assertNotNull(ts);
    assertEquals(epochNanos / 1_000_000L, ts.getMillisecond());
    assertEquals(456_789, ts.getNanoOfMillisecond());
  }

  @Test
  void int64ReaderReadsTimestampMillisFromDictionaryInUtc() {
    PrimitiveType type =
        Types.required(PrimitiveType.PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("ts");
    long epochMillis = 1_700_000_000_456L;
    ParquetDataColumnReader reader =
        ParquetDataColumnReaderFactory.getDataColumnReaderByTypeOnDictionary(
            type, new StubDictionary(epochMillis), true);

    TimestampData ts = reader.readTimestamp(0);
    assertNotNull(ts);
    assertEquals(epochMillis, ts.getMillisecond());
  }

  // -----------------------------------------------------------------------------------------------
  // Stubs (only the methods exercised by the dispatch + decoding tests above)
  // -----------------------------------------------------------------------------------------------

  /** Minimal {@link ValuesReader} returning a fixed long; other methods throw. */
  private static final class StubValuesReader extends ValuesReader {
    private final long fixedLong;

    StubValuesReader() {
      this(0L);
    }

    StubValuesReader(long fixedLong) {
      this.fixedLong = fixedLong;
    }

    @Override
    public long readLong() {
      return fixedLong;
    }

    @Override
    public void skip() {
      // unused
    }
  }

  /** Minimal {@link Dictionary} returning a fixed long for any id; other methods throw. */
  private static final class StubDictionary extends Dictionary {
    private final long fixedLong;

    StubDictionary() {
      this(0L);
    }

    StubDictionary(long fixedLong) {
      super(null);
      this.fixedLong = fixedLong;
    }

    @Override
    public Binary decodeToBinary(int id) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long decodeToLong(int id) {
      return fixedLong;
    }

    @Override
    public int getMaxId() {
      return 0;
    }
  }
}
