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

package org.apache.hudi.io.storage;

import com.lancedb.lance.file.LanceFileReader;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hudi.HoodieSchemaConversionUtils;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.io.memory.HoodieArrowAllocator;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StoragePath;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hudi.common.util.TypeUtils.unsafeCast;

/**
 * {@link HoodieSparkFileReader} implementation for Lance file format.
 */
public class HoodieSparkLanceReader implements HoodieSparkFileReader {
  // Memory size for data read operations: 120MB
  public static final long LANCE_DATA_ALLOCATOR_SIZE = 120 * 1024 * 1024;

  // Memory size for metadata operations: 8MB
  private static final long LANCE_METADATA_ALLOCATOR_SIZE = 8 * 1024 * 1024;

  // number of rows to read
  private static final int DEFAULT_BATCH_SIZE = 512;
  private final StoragePath path;

  public HoodieSparkLanceReader(StoragePath path) {
    this.path = path;
  }

  @Override
  public String[] readMinMaxRecordKeys() {
    throw new UnsupportedOperationException("Min/max record key tracking is not yet supported for Lance file format");
  }

  @Override
  public BloomFilter readBloomFilter() {
    throw new UnsupportedOperationException("Bloom filter is not yet supported for Lance file format");
  }

  @Override
  public Set<Pair<String, Long>> filterRowKeys(Set<String> candidateRowKeys) {
    Set<Pair<String, Long>> result = new HashSet<>();
    long position = 0;

    try (ClosableIterator<String> keyIterator = getRecordKeyIterator()) {
      while (keyIterator.hasNext()) {
        String recordKey = keyIterator.next();
        // If filter is empty/null, then all keys will be added.
        // if filter has specific keys, then ensure only those are added
        if (candidateRowKeys == null || candidateRowKeys.isEmpty()
                || candidateRowKeys.contains(recordKey)) {
          result.add(Pair.of(recordKey, position));
        }
        position++;
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to filter row keys from Lance file: " + path, e);
    }

    return result;
  }

  @Override
  public ClosableIterator<HoodieRecord<InternalRow>> getRecordIterator(HoodieSchema readerSchema, HoodieSchema requestedSchema) throws IOException {
    return getRecordIterator(requestedSchema);
  }

  @Override
  public ClosableIterator<HoodieRecord<InternalRow>> getRecordIterator(HoodieSchema schema) throws IOException {
    ClosableIterator<UnsafeRow> iterator = getUnsafeRowIterator(schema);
    //TODO .copy() is currently needed for correctness, https://github.com/apache/hudi/issues/17754
    return new CloseableMappingIterator<>(iterator, data -> unsafeCast(new HoodieSparkRecord(data.copy())));
  }

  @Override
  public ClosableIterator<String> getRecordKeyIterator() throws IOException {
    // Get schema with only the record key field for efficient column pruning
    HoodieSchema recordKeySchema = HoodieSchemaUtils.getRecordKeySchema();
    ClosableIterator<UnsafeRow> iterator = getUnsafeRowIterator(recordKeySchema);

    // Map each UnsafeRow to extract the record key string directly from index 0
    // The record key is at index 0 because we're using lance column projection which has only the record key field
    return new CloseableMappingIterator<>(iterator, data -> data.getUTF8String(0).toString());
  }

  /**
   * Get an iterator over UnsafeRows from the Lance file with column projection.
   * This allows reading only specific columns for better performance.
   *
   * @param requestedSchema schema specifying which columns to read
   * @return ClosableIterator over UnsafeRows
   * @throws IOException if reading fails
   */
  private ClosableIterator<UnsafeRow> getUnsafeRowIterator(HoodieSchema requestedSchema) {
    // Convert HoodieSchema to Spark StructType
    StructType requestedSparkSchema = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(requestedSchema);

    BufferAllocator allocator = HoodieArrowAllocator.newChildAllocator(
        getClass().getSimpleName() + "-data-" + path.getName(), LANCE_DATA_ALLOCATOR_SIZE);

    try {
      LanceFileReader lanceReader = LanceFileReader.open(path.toString(), allocator);

      // Build list of column names to project (read only requested columns)
      List<String> columnNames = new ArrayList<>(requestedSparkSchema.size());
      for (StructField field : requestedSparkSchema.fields()) {
        columnNames.add(field.name());
      }

      // Read only the requested columns from Lance file for efficiency
      ArrowReader arrowReader = lanceReader.readAll(columnNames, null, DEFAULT_BATCH_SIZE);

      return new LanceRecordIterator(allocator, lanceReader, arrowReader, requestedSparkSchema, path.toString());
    } catch (Exception e) {
      allocator.close();
      throw new HoodieException("Failed to create Lance reader for: " + path, e);
    }
  }

  @Override
  public HoodieSchema getSchema() {
    try (BufferAllocator allocator = HoodieArrowAllocator.newChildAllocator(
             getClass().getSimpleName() + "-metadata-" + path.getName(), LANCE_METADATA_ALLOCATOR_SIZE);
         LanceFileReader reader = LanceFileReader.open(path.toString(), allocator)) {
      Schema arrowSchema = reader.schema();
      StructType structType = LanceArrowUtils.fromArrowSchema(arrowSchema);
      return HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(structType, "record", "", true);
    } catch (Exception e) {
      throw new HoodieException("Failed to read schema from Lance file: " + path, e);
    }
  }

  @Override
  public void close() {
    // noop as resources are managed by the LanceRecordIterator and not within this reader class.
  }

  @Override
  public long getTotalRecords() {
    try (BufferAllocator allocator = HoodieArrowAllocator.newChildAllocator(
             getClass().getSimpleName() + "-metadata-" + path.getName(), LANCE_METADATA_ALLOCATOR_SIZE);
         LanceFileReader reader = LanceFileReader.open(path.toString(), allocator)) {
      return reader.numRows();
    } catch (Exception e) {
      throw new HoodieException("Failed to get row count from Lance file: " + path, e);
    }
  }
}
