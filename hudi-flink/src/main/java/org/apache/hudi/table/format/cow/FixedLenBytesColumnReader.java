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

import org.apache.flink.table.data.vector.writable.WritableBytesVector;
import org.apache.flink.table.data.vector.writable.WritableColumnVector;
import org.apache.flink.table.data.vector.writable.WritableIntVector;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Fixed length bytes {@code ColumnReader}, just for decimal.
 *
 * <p>Note: Reference Flink release 1.13.2
 * {@code org.apache.flink.formats.parquet.vector.reader.FixedLenBytesColumnReader}
 * to always write as legacy decimal format.
 */
public class FixedLenBytesColumnReader<V extends WritableColumnVector>
    extends AbstractColumnReader<V> {

  public FixedLenBytesColumnReader(
      ColumnDescriptor descriptor, PageReader pageReader, int precision) throws IOException {
    super(descriptor, pageReader);
    checkTypeName(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY);
  }

  @Override
  protected void readBatch(int rowId, int num, V column) {
    int bytesLen = descriptor.getPrimitiveType().getTypeLength();
    WritableBytesVector bytesVector = (WritableBytesVector) column;
    for (int i = 0; i < num; i++) {
      if (runLenDecoder.readInteger() == maxDefLevel) {
        byte[] bytes = readDataBinary(bytesLen).getBytes();
        bytesVector.appendBytes(rowId + i, bytes, 0, bytes.length);
      } else {
        bytesVector.setNullAt(rowId + i);
      }
    }
  }

  @Override
  protected void readBatchFromDictionaryIds(
      int rowId, int num, V column, WritableIntVector dictionaryIds) {
    WritableBytesVector bytesVector = (WritableBytesVector) column;
    for (int i = rowId; i < rowId + num; ++i) {
      if (!bytesVector.isNullAt(i)) {
        byte[] v = dictionary.decodeToBinary(dictionaryIds.getInt(i)).getBytes();
        bytesVector.appendBytes(i, v, 0, v.length);
      }
    }
  }

  private Binary readDataBinary(int len) {
    ByteBuffer buffer = readDataBuffer(len);
    if (buffer.hasArray()) {
      return Binary.fromConstantByteArray(
          buffer.array(), buffer.arrayOffset() + buffer.position(), len);
    } else {
      byte[] bytes = new byte[len];
      buffer.get(bytes);
      return Binary.fromConstantByteArray(bytes);
    }
  }
}
