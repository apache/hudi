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
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.hudi.HoodieSchemaConversionUtils;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.LanceArrowColumnVector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.hudi.common.util.TypeUtils.unsafeCast;

/**
 * {@link HoodieSparkFileReader} implementation for Lance file format.
 */
public class HoodieSparkLanceReader implements HoodieSparkFileReader {
  // number of rows to read 
  private static final int DEFAULT_BATCH_SIZE = 512; 
  private final StoragePath path;
  private final HoodieStorage storage;

  public HoodieSparkLanceReader(HoodieStorage storage, StoragePath path) {
    this.path = path;
    this.storage = storage;
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
    ClosableIterator<UnsafeRow> iterator = getUnsafeRowIterator();
    return new CloseableMappingIterator<>(iterator, data -> unsafeCast(new HoodieSparkRecord(data)));
  }

  @Override
  public ClosableIterator<String> getRecordKeyIterator() throws IOException {
    //TODO to revisit adding support for when metadata fields are not persisted.

    // Get schema with only the record key field for efficient column pruning
    HoodieSchema recordKeySchema = HoodieSchemaUtils.getRecordKeySchema();
    ClosableIterator<UnsafeRow> iterator = getUnsafeRowIterator(recordKeySchema);

    // Map each UnsafeRow to extract the record key string directly from index 0
    // The record key is at index 0 because we're using lance column projection which has only the record key field
    return new CloseableMappingIterator<>(iterator, data -> data.getUTF8String(0).toString());
  }

  /**
   * Get an iterator over UnsafeRows from the Lance file.
   *
   * @return ClosableIterator over UnsafeRows
   * @throws IOException if reading fails
   */
  public ClosableIterator<UnsafeRow> getUnsafeRowIterator() {
    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    try {
      LanceFileReader lanceReader = LanceFileReader.open(path.toString(), allocator);

      // Get schema from Lance file and convert to Spark StructType
      org.apache.arrow.vector.types.pojo.Schema arrowSchema = lanceReader.schema();
      StructType sparkSchema = LanceArrowUtils.fromArrowSchema(arrowSchema);

      ArrowReader arrowReader = lanceReader.readAll(null, null, DEFAULT_BATCH_SIZE);

      return new LanceRecordIterator(allocator, lanceReader, arrowReader, sparkSchema);
    } catch (Exception e) {
      allocator.close();
      throw new HoodieException("Failed to create Lance reader for: " + path, e);
    }
  }

  /**
   * Get an iterator over UnsafeRows from the Lance file with column projection.
   * This allows reading only specific columns for better performance.
   *
   * @param requestedSchema Avro schema specifying which columns to read
   * @return ClosableIterator over UnsafeRows
   * @throws IOException if reading fails
   */
  public ClosableIterator<UnsafeRow> getUnsafeRowIterator(HoodieSchema requestedSchema) {
    // Convert HoodieSchema to Spark StructType
    StructType requestedSparkSchema = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(requestedSchema);

    BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    try {
      LanceFileReader lanceReader = LanceFileReader.open(path.toString(), allocator);

      // Build list of column names to project (read only requested columns)
      List<String> columnNames = new ArrayList<>();
      for (StructField field : requestedSparkSchema.fields()) {
        columnNames.add(field.name());
      }

      // Read only the requested columns from Lance file for efficiency
      ArrowReader arrowReader = lanceReader.readAll(columnNames, null, DEFAULT_BATCH_SIZE);

      return new LanceRecordIterator(allocator, lanceReader, arrowReader, requestedSparkSchema);
    } catch (Exception e) {
      allocator.close();
      throw new HoodieException("Failed to create Lance reader for: " + path, e);
    }
  }

  @Override
  public HoodieSchema getSchema() {
    // Read Arrow schema from Lance file and convert to Avro
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         LanceFileReader reader = LanceFileReader.open(path.toString(), allocator)) {
      org.apache.arrow.vector.types.pojo.Schema arrowSchema = reader.schema();
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
    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         LanceFileReader reader = LanceFileReader.open(path.toString(), allocator)) {
      return reader.numRows();
    } catch (Exception e) {
      throw new HoodieException("Failed to get row count from Lance file: " + path, e);
    }
  }

  /**
   * Iterator implementation that reads Lance file batches and converts to UnsafeRows.
   * Keeps ColumnarBatch alive while iterating to avoid unnecessary data copying.
   */
  private class LanceRecordIterator implements ClosableIterator<UnsafeRow> {
    private final BufferAllocator allocator;
    private final LanceFileReader lanceReader;
    private final ArrowReader arrowReader;
    private final StructType schema;
    private final UnsafeProjection projection;
    private ColumnarBatch currentBatch;
    private Iterator<InternalRow> rowIterator;

    public LanceRecordIterator(BufferAllocator allocator,
                               LanceFileReader lanceReader,
                               ArrowReader arrowReader,
                               StructType schema) {
      this.allocator = allocator;
      this.lanceReader = lanceReader;
      this.arrowReader = arrowReader;
      this.schema = schema;
      this.projection = UnsafeProjection.create(schema);
    }

    @Override
    public boolean hasNext() {
      // If we have records in current batch, return true
      if (rowIterator != null && rowIterator.hasNext()) {
        return true;
      }

      // Close previous batch before loading next
      if (currentBatch != null) {
        currentBatch.close();
        currentBatch = null;
      }

      // Try to load next batch
      try {
        if (arrowReader.loadNextBatch()) {
          VectorSchemaRoot root = arrowReader.getVectorSchemaRoot();

          // Wrap each Arrow FieldVector in LanceArrowColumnVector for type-safe access
          ColumnVector[] columns = root.getFieldVectors().stream()
                  .map(LanceArrowColumnVector::new)
                  .toArray(ColumnVector[]::new);

          // Create ColumnarBatch and keep it alive while iterating
          currentBatch = new ColumnarBatch(columns, root.getRowCount());
          rowIterator = currentBatch.rowIterator();
          return rowIterator.hasNext();
        }
      } catch (IOException e) {
        throw new HoodieException("Failed to read next batch from Lance file: " + path, e);
      }

      return false;
    }

    @Override
    public UnsafeRow next() {
      if (!hasNext()) {
        throw new IllegalStateException("No more records available");
      }
      InternalRow row = rowIterator.next();
      // Convert to UnsafeRow immediately while batch is still open
      return projection.apply(row);
    }

    @Override
    public void close() {
      IOException arrowException = null;
      Exception lanceException = null;

      // Close current batch if exists
      if (currentBatch != null) {
        currentBatch.close();
      }

      // Close Arrow reader
      if (arrowReader != null) {
        try {
          arrowReader.close();
        } catch (IOException e) {
          arrowException = e;
        }
      }

      // Close Lance reader
      if (lanceReader != null) {
        try {
          lanceReader.close();
        } catch (Exception e) {
          lanceException = e;
        }
      }

      // Always close allocator
      if (allocator != null) {
        allocator.close();
      }

      // Throw any exceptions that occurred
      if (arrowException != null) {
        throw new HoodieIOException("Failed to close Arrow reader", arrowException);
      }
      if (lanceException != null) {
        throw new HoodieException("Failed to close Lance reader", lanceException);
      }
    }
  }
}