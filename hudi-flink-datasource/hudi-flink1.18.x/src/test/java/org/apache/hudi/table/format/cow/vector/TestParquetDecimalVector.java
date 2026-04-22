/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.format.cow.vector;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.columnar.vector.BytesColumnVector;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapBytesVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapIntVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapLongVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapShortVector;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ParquetDecimalVector}.
 *
 * <p>Primary target is the {@link ParquetDecimalVector#getDecimal} dispatch fixed in GH-18491:
 * small-precision decimals are physically encoded in Parquet as INT32 or INT64, not as bytes.
 * The pre-fix implementation unconditionally cast the child vector to
 * {@link org.apache.flink.table.data.columnar.vector.BytesColumnVector} and threw
 * {@link ClassCastException} at read time.
 */
class TestParquetDecimalVector {

  // ---------------------------------------------------------------------------------------------
  // getDecimal dispatch: INT32 / INT64 / BYTES
  // ---------------------------------------------------------------------------------------------

  @Test
  void getDecimalFromInt32VectorDecodesUnscaledLong() {
    // precision <= 9 ⇒ ParquetSchemaConverter#is32BitDecimal(precision) == true
    HeapIntVector intVector = new HeapIntVector(1);
    intVector.vector[0] = 12345;
    ParquetDecimalVector wrapped = new ParquetDecimalVector(intVector);

    DecimalData decoded = wrapped.getDecimal(0, 5, 2);

    assertEquals(new BigDecimal("123.45"), decoded.toBigDecimal());
  }

  @Test
  void getDecimalFromInt64VectorDecodesUnscaledLong() {
    // 9 < precision <= 18 ⇒ ParquetSchemaConverter#is64BitDecimal(precision) == true
    HeapLongVector longVector = new HeapLongVector(1);
    longVector.vector[0] = 1234567890123456L;
    ParquetDecimalVector wrapped = new ParquetDecimalVector(longVector);

    DecimalData decoded = wrapped.getDecimal(0, 18, 4);

    assertEquals(new BigDecimal("123456789012.3456"), decoded.toBigDecimal());
  }

  @Test
  void getDecimalFromBytesVectorDecodesUnscaledBytes() {
    // precision > 18 ⇒ BINARY / FIXED_LEN_BYTE_ARRAY path
    BigDecimal original = new BigDecimal("12345678901234567890.1234567890");
    byte[] unscaled = original.unscaledValue().toByteArray();
    HeapBytesVector bytesVector = new HeapBytesVector(1);
    bytesVector.appendBytes(0, unscaled, 0, unscaled.length);
    ParquetDecimalVector wrapped = new ParquetDecimalVector(bytesVector);

    DecimalData decoded = wrapped.getDecimal(0, 30, 10);

    assertEquals(original, decoded.toBigDecimal());
  }

  @Test
  void getDecimalFallsBackToBytesWhenChildIsBytesEvenAtSmallPrecision() {
    // A Parquet file can legally encode a small-precision decimal as BINARY. In that case the
    // dispatch must fall through to the bytes branch rather than require an IntColumnVector.
    BigDecimal original = new BigDecimal("123.45");
    byte[] unscaled = original.unscaledValue().toByteArray();
    HeapBytesVector bytesVector = new HeapBytesVector(1);
    bytesVector.appendBytes(0, unscaled, 0, unscaled.length);
    ParquetDecimalVector wrapped = new ParquetDecimalVector(bytesVector);

    DecimalData decoded = wrapped.getDecimal(0, 5, 2);

    assertEquals(original, decoded.toBigDecimal());
  }

  @Test
  void getDecimalOnUnsupportedVectorTypeThrows() {
    // A large-precision request must have a bytes-backed child; any other writable child is an
    // illegal combination and must be surfaced via Preconditions#checkArgument.
    ColumnVector unsupported = new HeapShortVector(1);
    ParquetDecimalVector wrapped = new ParquetDecimalVector(unsupported);

    assertThrows(IllegalArgumentException.class, () -> wrapped.getDecimal(0, 30, 10));
  }

  // ---------------------------------------------------------------------------------------------
  // Null handling delegates to the child
  // ---------------------------------------------------------------------------------------------

  @Test
  void isNullAtDelegatesToChild() {
    HeapIntVector intVector = new HeapIntVector(2);
    intVector.vector[0] = 1;
    intVector.setNullAt(1);
    ParquetDecimalVector wrapped = new ParquetDecimalVector(intVector);

    assertFalse(wrapped.isNullAt(0));
    assertTrue(wrapped.isNullAt(1));
  }

  // ---------------------------------------------------------------------------------------------
  // Writable delegation: the new WritableInt / WritableLong / WritableBytes contracts
  // ---------------------------------------------------------------------------------------------

  @Test
  void writableIntPathRoundTripsThroughWrapper() {
    HeapIntVector intVector = new HeapIntVector(1);
    ParquetDecimalVector wrapped = new ParquetDecimalVector(intVector);

    wrapped.setInt(0, 42);

    assertEquals(42, wrapped.getInt(0));
    assertEquals(42, intVector.vector[0]);
  }

  @Test
  void writableLongPathRoundTripsThroughWrapper() {
    HeapLongVector longVector = new HeapLongVector(1);
    ParquetDecimalVector wrapped = new ParquetDecimalVector(longVector);

    wrapped.setLong(0, 9876543210L);

    assertEquals(9876543210L, wrapped.getLong(0));
    assertEquals(9876543210L, longVector.vector[0]);
  }

  @Test
  void writableBytesPathRoundTripsThroughWrapper() {
    HeapBytesVector bytesVector = new HeapBytesVector(1);
    ParquetDecimalVector wrapped = new ParquetDecimalVector(bytesVector);
    byte[] payload = new byte[] {0x01, 0x02, 0x03};

    wrapped.appendBytes(0, payload, 0, payload.length);

    BytesColumnVector.Bytes out = wrapped.getBytes(0);
    assertEquals(payload.length, out.len);
    assertEquals(0x01, out.data[out.offset]);
    assertEquals(0x02, out.data[out.offset + 1]);
    assertEquals(0x03, out.data[out.offset + 2]);
  }

  @Test
  void resetDelegatesToChild() {
    HeapIntVector intVector = new HeapIntVector(1);
    intVector.setNullAt(0);
    ParquetDecimalVector wrapped = new ParquetDecimalVector(intVector);
    assertTrue(wrapped.isNullAt(0));

    wrapped.reset();

    assertFalse(wrapped.isNullAt(0));
  }

  @Test
  void fillWithNullsDelegatesToChild() {
    HeapIntVector intVector = new HeapIntVector(2);
    ParquetDecimalVector wrapped = new ParquetDecimalVector(intVector);

    wrapped.fillWithNulls();

    assertTrue(wrapped.isNullAt(0));
    assertTrue(wrapped.isNullAt(1));
  }

  @Test
  void setNullAtDelegatesToChild() {
    HeapIntVector intVector = new HeapIntVector(2);
    ParquetDecimalVector wrapped = new ParquetDecimalVector(intVector);

    wrapped.setNullAt(0);
    wrapped.setNulls(1, 1);

    assertTrue(wrapped.isNullAt(0));
    assertTrue(wrapped.isNullAt(1));
  }
}
