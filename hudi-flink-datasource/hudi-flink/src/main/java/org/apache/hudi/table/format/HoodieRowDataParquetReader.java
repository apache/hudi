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
import org.apache.hudi.common.util.Option;
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
 * <p>The table-level {@link HoodieSchema} is supplied at construction time via the factory
 * ({@link HoodieRowDataFileReaderFactory}). When present, the schema is used to derive a Flink
 * RowType that correctly preserves Hudi logical types (Variant, Blob, Vector). These types are
 * indistinguishable from ordinary BYTES or ROW columns in the Parquet physical schema.
 *
 * <p>The schema may be absent ({@code Option.empty()}) for metadata-only operations such as
 * bloom filter lookups, min/max key reads, and record counts. In that case, calling
 * {@link #getRowType()}, {@link #getSchema()}, or any record-reading method will throw
 * {@link IllegalStateException}.
 */
public class HoodieRowDataParquetReader implements HoodieFileReader<RowData>  {
  private final HoodieStorage storage;
  private final ParquetUtils parquetUtils;
  private final StoragePath path;
  private final Option<HoodieSchema> tableSchema;
  private HoodieSchema fileSchema;
  private DataType fileRowType;
  private final List<ClosableIterator<RowData>> readerIterators = new ArrayList<>();

  public HoodieRowDataParquetReader(HoodieStorage storage, StoragePath path, Option<HoodieSchema> schemaOption) {
    this.storage = storage;
    this.parquetUtils = (ParquetUtils) HoodieIOFactory.getIOFactory(storage).getFileFormatUtils(HoodieFileFormat.PARQUET);
    this.path = path;
    this.tableSchema = schemaOption;
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
    ClosableIterator<RowData> rowDataItr = getRowDataIterator(InternalSchemaManager.DISABLED, getRowType(), requestedSchema, Collections.emptyList());
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
      checkSchemaPresent();
      fileSchema = tableSchema.get();
    }
    return fileSchema;
  }

  public DataType getRowType() {
    if (fileRowType == null) {
      checkSchemaPresent();
      fileRowType = RowDataQueryContexts.fromSchema(tableSchema.get()).getRowType();
    }
    return fileRowType;
  }

  /**
   * Ensures the HoodieSchema was supplied at construction time. Required for Flink MOR/COW
   * unstructured type support: Hudi logical types (Variant, Blob, Vector) cannot be inferred
   * from the Parquet physical schema alone because their binary/group layouts are
   * indistinguishable from ordinary BYTES or ROW columns. Only the HoodieSchema carries the
   * logical type annotations needed for correct Flink RowType derivation.
   */
  private void checkSchemaPresent() {
    if (!tableSchema.isPresent()) {
      throw new IllegalStateException(
          "HoodieSchema is required for record-reading operations but was not supplied. "
              + "Pass the schema via getFileReader(config, path, Option.of(schema)) or the "
              + "4-arg getFileReader overload. File: " + path);
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
