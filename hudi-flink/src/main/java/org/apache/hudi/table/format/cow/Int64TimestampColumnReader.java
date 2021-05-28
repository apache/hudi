/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.format.cow;

import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.vector.writable.WritableIntVector;
import org.apache.flink.table.data.vector.writable.WritableTimestampVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;

/**
 * Timestamp {@link org.apache.flink.formats.parquet.vector.reader.ColumnReader} that supports INT64 8 bytes,
 * TIMESTAMP_MILLIS is the deprecated ConvertedType counterpart of a TIMESTAMP logical type
 * that is UTC normalized and has MILLIS precision.
 *
 * <p>See https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#timestamp
 * TIMESTAMP_MILLIS and TIMESTAMP_MICROS are the deprecated ConvertedType.
 */
public class Int64TimestampColumnReader extends AbstractColumnReader<WritableTimestampVector> {

  private final boolean utcTimestamp;

  public Int64TimestampColumnReader(
      boolean utcTimestamp,
      ColumnDescriptor descriptor,
      PageReader pageReader) throws IOException {
    super(descriptor, pageReader);
    this.utcTimestamp = utcTimestamp;
    checkTypeName(PrimitiveType.PrimitiveTypeName.INT64);
  }

  @Override
  protected boolean supportLazyDecode() {
    return false;
  }

  @Override
  protected void readBatch(int rowId, int num, WritableTimestampVector column) {
    for (int i = 0; i < num; i++) {
      if (runLenDecoder.readInteger() == maxDefLevel) {
        ByteBuffer buffer = readDataBuffer(8);
        column.setTimestamp(rowId + i, int64ToTimestamp(utcTimestamp, buffer.getLong()));
      } else {
        column.setNullAt(rowId + i);
      }
    }
  }

  @Override
  protected void readBatchFromDictionaryIds(
      int rowId,
      int num,
      WritableTimestampVector column,
      WritableIntVector dictionaryIds) {
    for (int i = rowId; i < rowId + num; ++i) {
      if (!column.isNullAt(i)) {
        column.setTimestamp(i, decodeInt64ToTimestamp(
            utcTimestamp, dictionary, dictionaryIds.getInt(i)));
      }
    }
  }

  public static TimestampData decodeInt64ToTimestamp(
      boolean utcTimestamp,
      org.apache.parquet.column.Dictionary dictionary,
      int id) {
    long value = dictionary.decodeToLong(id);
    return int64ToTimestamp(utcTimestamp, value);
  }

  private static TimestampData int64ToTimestamp(boolean utcTimestamp, long millionsOfDay) {
    if (utcTimestamp) {
      return TimestampData.fromEpochMillis(millionsOfDay, 0);
    } else {
      Timestamp timestamp = new Timestamp(millionsOfDay);
      return TimestampData.fromTimestamp(timestamp);
    }
  }
}
