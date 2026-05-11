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
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.source.ExpressionPredicates.Predicate;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.RowDataQueryContexts;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Implementation of {@link HoodieFileReader} to read {@link RowData}s from base file.
 *
 * <p><b>Schema enforcement:</b> This reader requires the table-level {@link HoodieSchema} to be
 * set via {@link #withTableSchema(HoodieSchema)} before reading. The HoodieSchema is the
 * authoritative source for Hudi logical types (Variant, Blob, Vector) — these types cannot be
 * inferred from the Parquet physical schema alone because their binary/group layouts are
 * indistinguishable from ordinary BYTES or ROW columns without schema context. The Parquet
 * {@code VARIANT} annotation (parquet-java 1.15.2+) is also insufficient because older files
 * lack it and it cannot distinguish Blob/Vector from Variant.
 *
 * <p>All callers (e.g. {@code FlinkRowDataReaderContext}, {@code ClusteringOperator}) supply the
 * schema from {@code TableSchemaResolver}. An {@link IllegalStateException} is thrown if the
 * schema is missing, preventing silent mis-inference of logical types.
 */
public class HoodieRowDataParquetReader implements HoodieFileReader<RowData>  {
  private final HoodieStorage storage;
  private final ParquetUtils parquetUtils;
  private final StoragePath path;
  private HoodieSchema fileSchema;
  private DataType fileRowType;
  private HoodieSchema tableSchema;
  private final List<ClosableIterator<RowData>> readerIterators = new ArrayList<>();

  public HoodieRowDataParquetReader(HoodieStorage storage, StoragePath path) {
    this.storage = storage;
    this.parquetUtils = (ParquetUtils) HoodieIOFactory.getIOFactory(storage).getFileFormatUtils(HoodieFileFormat.PARQUET);
    this.path = path;
  }

  /**
   * Sets the table-level HoodieSchema. This schema is used directly to derive the Flink
   * RowType (via {@code HoodieSchemaConverter}), ensuring Hudi logical types (Variant, Blob,
   * Vector) are correctly represented in the Flink type system.
   *
   * <p><b>Must</b> be called before {@link #getRowType()}, {@link #getSchema()}, or
   * {@link #getRecordKeyIterator()}. An {@link IllegalStateException} is thrown otherwise.
   */
  public HoodieRowDataParquetReader withTableSchema(HoodieSchema tableSchema) {
    this.tableSchema = tableSchema;
    return this;
  }

  @Override
  public String[] readMinMaxRecordKeys() {
    return parquetUtils.readMinMaxRecordKeys(storage, path);
  }

  @Override
  public BloomFilter readBloomFilter() {
    return parquetUtils.readBloomFilterFromMetadata(storage, path);
  }

  @Override
  public Set<Pair<String, Long>> filterRowKeys(Set<String> candidateRowKeys) {
    return parquetUtils.filterRowKeys(storage, path, candidateRowKeys);
  }

  @Override
  public ClosableIterator<HoodieRecord<RowData>> getRecordIterator(HoodieSchema readerSchema, HoodieSchema requestedSchema) throws IOException {
    DataType rowType = RowDataQueryContexts.fromSchema(readerSchema).getRowType();
    ClosableIterator<RowData> rowDataItr = getRowDataIterator(InternalSchemaManager.DISABLED, rowType, requestedSchema, Collections.emptyList());
    readerIterators.add(rowDataItr);
    return new CloseableMappingIterator<>(rowDataItr, HoodieFlinkRecord::new);
  }

  @Override
  public ClosableIterator<String> getRecordKeyIterator() throws IOException {
    HoodieSchema schema = HoodieSchemaUtils.getRecordKeySchema();
    ClosableIterator<RowData> rowDataItr = getRowDataIterator(InternalSchemaManager.DISABLED, getRowType(), schema, Collections.emptyList());
    return new CloseableMappingIterator<>(rowDataItr, rowData -> Objects.toString(rowData.getString(0)));
  }

  public ClosableIterator<RowData> getRowDataIterator(
      InternalSchemaManager internalSchemaManager,
      DataType dataType,
      HoodieSchema requestedSchema,
      List<Predicate> predicates) throws IOException {
    return RecordIterators.getParquetRecordIterator(storage.getConf(), internalSchemaManager, dataType, requestedSchema, path, predicates);
  }

  @Override
  public HoodieSchema getSchema() {
    if (fileSchema == null) {
      checkTableSchemaPresent();
      fileSchema = tableSchema;
    }
    return fileSchema;
  }

  public DataType getRowType() {
    if (fileRowType == null) {
      checkTableSchemaPresent();
      fileRowType = RowDataQueryContexts.fromSchema(tableSchema).getRowType();
    }
    return fileRowType;
  }

  private void checkTableSchemaPresent() {
    if (tableSchema == null) {
      throw new IllegalStateException(
          "tableSchema must be set via withTableSchema() before calling getRowType()/getSchema(). "
              + "The Flink Parquet reader requires the HoodieSchema to correctly infer "
              + "logical types (Variant, Blob, Vector). File: " + path);
    }
  }

  @Override
  public void close() {
    readerIterators.forEach(ClosableIterator::close);
  }

  @Override
  public long getTotalRecords() {
    return parquetUtils.getRowCount(storage, path);
  }
}
