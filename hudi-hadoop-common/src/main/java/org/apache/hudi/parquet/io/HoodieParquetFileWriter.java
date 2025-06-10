/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.parquet.io;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndexBuilder;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

/**
 *  wrap ParquetFileWriter to solve the compatibility issues of lower versions of Parquet (< 0.13.1)
 */
public class HoodieParquetFileWriter extends ParquetFileWriter {

  private final Method copyMethod;
  private final Field currentBloomFiltersField;
  private final Field currentColumnIndexesField;
  private final Field currentOffsetIndexesField;
  private final Field currentBlockField;

  public HoodieParquetFileWriter(OutputFile file, MessageType schema, Mode mode, long rowGroupSize, int maxPaddingSize, int columnIndexTruncateLength,
                                 int statisticsTruncateLength, boolean pageWriteChecksumEnabled) throws IOException, NoSuchMethodException, NoSuchFieldException {
    super(file, schema, mode, rowGroupSize, maxPaddingSize, columnIndexTruncateLength, statisticsTruncateLength, pageWriteChecksumEnabled);

    Class clazz = ParquetFileWriter.class;
    copyMethod = clazz.getDeclaredMethod("copy", SeekableInputStream.class, PositionOutputStream.class, long.class, long.class);
    copyMethod.setAccessible(true);

    currentBloomFiltersField = clazz.getDeclaredField("currentBloomFilters");
    currentBloomFiltersField.setAccessible(true);

    currentColumnIndexesField = clazz.getDeclaredField("currentColumnIndexes");
    currentColumnIndexesField.setAccessible(true);

    currentOffsetIndexesField = clazz.getDeclaredField("currentOffsetIndexes");
    currentOffsetIndexesField.setAccessible(true);

    currentBlockField = clazz.getDeclaredField("currentBlock");
    currentBlockField.setAccessible(true);
  }


  /**
   * Copy from super, and add null check of offsetIndex
   * @param descriptor the descriptor for the target column
   * @param from a file stream to read from
   * @param chunk the column chunk to be copied
   * @param bloomFilter the bloomFilter for this chunk
   * @param columnIndex the column index for this chunk
   * @param offsetIndex the offset index for this chunk
   * @throws IOException
   */
  @Override
  public void appendColumnChunk(ColumnDescriptor descriptor, SeekableInputStream from, ColumnChunkMetaData chunk,
                                BloomFilter bloomFilter, ColumnIndex columnIndex, OffsetIndex offsetIndex) throws IOException {
    long start = chunk.getStartingPos();
    long length = chunk.getTotalSize();
    long newChunkStart = out.getPos();

    // 'offsetIndex != null' is what we need, but not included in ParquetFileWriter 1.12.3
    if (offsetIndex != null && newChunkStart != start) {
      offsetIndex = OffsetIndexBuilder.getBuilder()
          .fromOffsetIndex(offsetIndex)
          .build(newChunkStart - start);
    }

    try {
      copyMethod.invoke(this, from, out, start, length);

      Map<String, BloomFilter> currentBloomFiltersValue = getFieldValueFromSuper(currentBloomFiltersField, Map.class);
      currentBloomFiltersValue.put(String.join(".", descriptor.getPath()), bloomFilter);

      List<ColumnIndex> currentColumnIndexesValue = getFieldValueFromSuper(currentColumnIndexesField, List.class);
      currentColumnIndexesValue.add(columnIndex);

      List<OffsetIndex> currentOffsetIndexesValue = getFieldValueFromSuper(currentOffsetIndexesField, List.class);
      currentOffsetIndexesValue.add(offsetIndex);

      BlockMetaData currentBlockValue = getFieldValueFromSuper(currentBlockField, BlockMetaData.class);
      Offsets offsets = Offsets.getOffsets(from, chunk, newChunkStart);
      currentBlockValue.addColumn(ColumnChunkMetaData.get(
          chunk.getPath(),
          chunk.getPrimitiveType(),
          chunk.getCodec(),
          chunk.getEncodingStats(),
          chunk.getEncodings(),
          chunk.getStatistics(),
          offsets.firstDataPageOffset,
          offsets.dictionaryPageOffset,
          chunk.getValueCount(),
          chunk.getTotalSize(),
          chunk.getTotalUncompressedSize()));

      currentBlockValue.setTotalByteSize(currentBlockValue.getTotalByteSize() + chunk.getTotalUncompressedSize());
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  private <T> T getFieldValueFromSuper(Field field, Class<T> clazz) throws IllegalAccessException {
    Object obj = field.get(this);
    if (clazz.isInstance(obj)) {
      return (T) obj;
    }
    return null;
  }

}
