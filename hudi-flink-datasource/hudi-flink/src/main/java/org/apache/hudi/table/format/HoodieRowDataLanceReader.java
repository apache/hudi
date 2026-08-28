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

package org.apache.hudi.table.format;

import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.HoodieDynamicBoundedBloomFilter;
import org.apache.hudi.common.bloom.SimpleBloomFilter;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.io.memory.HoodieArrowAllocator;
import org.apache.hudi.io.storage.row.lance.HoodieFlinkLanceArrowUtils;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.hudi.util.RowDataQueryContexts;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.lance.file.LanceFileReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY;
import static org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport.HOODIE_BLOOM_FILTER_TYPE_CODE;
import static org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER;
import static org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER;

/**
 * Lance reader for Flink RowData base files.
 */
public class HoodieRowDataLanceReader implements HoodieRowDataFileReader {

  private static final int DEFAULT_BATCH_SIZE = 512;

  private final StoragePath path;
  private final long dataAllocatorSize;
  private final BufferAllocator metadataAllocator;
  private final LanceFileReader metadataReader;
  private final Schema arrowSchema;
  private boolean closed;

  public HoodieRowDataLanceReader(StoragePath path, HoodieConfig hoodieConfig) {
    this.path = path;
    this.dataAllocatorSize = hoodieConfig.getLongOrDefault(HoodieStorageConfig.LANCE_READ_ALLOCATOR_SIZE_BYTES);
    this.metadataAllocator = HoodieArrowAllocator.newChildAllocator(
        getClass().getSimpleName() + "-metadata-" + path.getName(),
        hoodieConfig.getLongOrDefault(HoodieStorageConfig.LANCE_READ_METADATA_ALLOCATOR_SIZE_BYTES));
    try {
      this.metadataReader = LanceFileReader.open(path.toString(), metadataAllocator);
      this.arrowSchema = metadataReader.schema();
    } catch (Exception e) {
      close();
      throw new HoodieException("Failed to create Lance reader for: " + path, e);
    }
  }

  @Override
  public String[] readMinMaxRecordKeys() {
    Map<String, String> metadata = arrowSchema.getCustomMetadata();
    if (metadata != null) {
      String minKey = metadata.get(HOODIE_MIN_RECORD_KEY_FOOTER);
      String maxKey = metadata.get(HOODIE_MAX_RECORD_KEY_FOOTER);
      if (minKey != null && maxKey != null) {
        return new String[] {minKey, maxKey};
      }
    }
    throw new HoodieException("Could not read min/max record key out of Lance file: " + path);
  }

  @Override
  public BloomFilter readBloomFilter() {
    Map<String, String> metadata = arrowSchema.getCustomMetadata();
    if (metadata == null || !metadata.containsKey(HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY)) {
      return null;
    }
    String bloomSer = metadata.get(HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY);
    String filterType = metadata.get(HOODIE_BLOOM_FILTER_TYPE_CODE);
    if (filterType != null && filterType.contains(HoodieDynamicBoundedBloomFilter.TYPE_CODE_PREFIX)) {
      return new HoodieDynamicBoundedBloomFilter(bloomSer);
    }
    return new SimpleBloomFilter(bloomSer);
  }

  @Override
  public Set<Pair<String, Long>> filterRowKeys(Set<String> candidateRowKeys) {
    Set<Pair<String, Long>> result = new HashSet<>();
    long position = 0;
    boolean includeAllKeys = candidateRowKeys == null || candidateRowKeys.isEmpty();

    try (ClosableIterator<String> keyIterator = getRecordKeyIterator()) {
      while (keyIterator.hasNext()) {
        String recordKey = keyIterator.next();
        if (includeAllKeys || candidateRowKeys.contains(recordKey)) {
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
  public ClosableIterator<HoodieRecord<RowData>> getRecordIterator(HoodieSchema readerSchema, HoodieSchema requestedSchema) throws IOException {
    ClosableIterator<RowData> rowDataItr = getRowDataIterator(RowDataQueryContexts.fromSchema(requestedSchema).getRowType(), requestedSchema);
    return new CloseableMappingIterator<>(rowDataItr, HoodieFlinkRecord::new);
  }

  @Override
  public ClosableIterator<String> getRecordKeyIterator() throws IOException {
    HoodieSchema schema = HoodieSchemaUtils.getRecordKeySchema();
    ClosableIterator<RowData> rowDataItr = getRowDataIterator(RowDataQueryContexts.fromSchema(schema).getRowType(), schema);
    return new CloseableMappingIterator<>(rowDataItr, rowData -> rowData.getString(0).toString());
  }

  @Override
  public ClosableIterator<RowData> getRowDataIterator(
      HoodieSchema dataSchema,
      HoodieSchema requiredSchema,
      InternalSchemaManager internalSchemaManager,
      List<ExpressionPredicates.Predicate> predicates) {
    // Lance base-file reading does not support schema evolution.
    if (internalSchemaManager != InternalSchemaManager.DISABLED
        && !internalSchemaManager.getMergeSchema(path.getName()).isEmptySchema()) {
      throw new HoodieValidationException("Flink Lance base-file support does not support schema evolution.");
    }
    return getRowDataIterator(RowDataQueryContexts.fromSchema(requiredSchema).getRowType(), requiredSchema);
  }

  public ClosableIterator<RowData> getRowDataIterator(DataType dataType, HoodieSchema requestedSchema) {
    RowType rowType = (RowType) dataType.getLogicalType();
    List<String> columnNames = new ArrayList<>(rowType.getFieldCount());
    for (RowType.RowField field : rowType.getFields()) {
      columnNames.add(field.getName());
    }
    BufferAllocator allocator = HoodieArrowAllocator.newChildAllocator(
        getClass().getSimpleName() + "-data-" + path.getName(), dataAllocatorSize);
    LanceFileReader lanceReader = null;
    ArrowReader arrowReader = null;
    try {
      lanceReader = LanceFileReader.open(path.toString(), allocator);
      arrowReader = lanceReader.readAll(columnNames, null, DEFAULT_BATCH_SIZE);
      return new LanceRowDataIterator(allocator, lanceReader, arrowReader, rowType, this);
    } catch (Exception e) {
      if (arrowReader != null) {
        try {
          arrowReader.close();
        } catch (Exception closeException) {
          e.addSuppressed(closeException);
        }
      }
      if (lanceReader != null) {
        try {
          lanceReader.close();
        } catch (Exception closeException) {
          e.addSuppressed(closeException);
        }
      }
      allocator.close();
      throw new HoodieException("Failed to create Lance row iterator for: " + path, e);
    }
  }

  @Override
  public HoodieSchema getSchema() {
    RowType rowType = HoodieFlinkLanceArrowUtils.toRowType(arrowSchema);
    return HoodieSchemaConverter.convertToSchema(rowType);
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    if (metadataReader != null) {
      try {
        metadataReader.close();
      } catch (Exception e) {
        // ignore close failure; readers surface data-path exceptions earlier
      }
    }
    if (metadataAllocator != null) {
      metadataAllocator.close();
    }
  }

  @Override
  public long getTotalRecords() {
    try {
      return metadataReader.numRows();
    } catch (Exception e) {
      throw new HoodieException("Failed to read row count from Lance file: " + path, e);
    }
  }

  private static class LanceRowDataIterator implements ClosableIterator<RowData> {
    private final BufferAllocator allocator;
    private final LanceFileReader lanceReader;
    private final ArrowReader arrowReader;
    private final RowType rowType;
    private final HoodieRowDataLanceReader reader;
    private VectorSchemaRoot batch;
    private List<FieldVector> orderedVectors;
    private int rowId;
    private boolean hasNext;
    private boolean closed;

    private LanceRowDataIterator(
        BufferAllocator allocator,
        LanceFileReader lanceReader,
        ArrowReader arrowReader,
        RowType rowType,
        HoodieRowDataLanceReader reader) {
      this.allocator = allocator;
      this.lanceReader = lanceReader;
      this.arrowReader = arrowReader;
      this.rowType = rowType;
      this.reader = reader;
      loadNextBatch();
    }

    @Override
    public boolean hasNext() {
      return hasNext;
    }

    @Override
    public RowData next() {
      RowData rowData = HoodieFlinkLanceArrowUtils.toRowData(rowType, orderedVectors, rowId++);
      if (rowId >= batch.getRowCount()) {
        loadNextBatch();
      }
      return rowData;
    }

    private void loadNextBatch() {
      try {
        do {
          hasNext = arrowReader.loadNextBatch();
          if (hasNext) {
            batch = arrowReader.getVectorSchemaRoot();
            orderedVectors = orderVectors(rowType, batch.getFieldVectors());
            rowId = 0;
          }
        } while (hasNext && batch.getRowCount() == 0);
      } catch (IOException e) {
        throw new HoodieIOException("Failed to read Lance batch", e);
      }
    }

    private static List<FieldVector> orderVectors(RowType rowType, List<FieldVector> vectors) {
      Map<String, FieldVector> vectorsByName = new HashMap<>();
      for (FieldVector vector : vectors) {
        vectorsByName.put(vector.getName(), vector);
      }
      List<FieldVector> orderedVectors = new ArrayList<>(rowType.getFieldCount());
      for (RowType.RowField field : rowType.getFields()) {
        FieldVector vector = vectorsByName.get(field.getName());
        if (vector == null) {
          throw new HoodieException("Missing Lance column in projected batch: " + field.getName());
        }
        orderedVectors.add(vector);
      }
      return orderedVectors;
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      try {
        arrowReader.close();
      } catch (Exception e) {
        throw new HoodieException("Failed to close Lance Arrow reader", e);
      } finally {
        try {
          lanceReader.close();
        } catch (Exception e) {
          throw new HoodieException("Failed to close Lance reader", e);
        } finally {
          try {
            allocator.close();
          } finally {
            reader.close();
          }
        }
      }
    }
  }
}
