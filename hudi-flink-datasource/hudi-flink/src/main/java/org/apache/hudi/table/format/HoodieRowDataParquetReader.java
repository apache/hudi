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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.io.storage.row.parquet.ParquetSchemaConverter;
import org.apache.hudi.source.ExpressionPredicates.Predicate;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.AvroSchemaConverter;

import org.apache.avro.Schema;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Implementation of {@link HoodieFileReader} to read {@link RowData}s from base file.
 */
public class HoodieRowDataParquetReader implements HoodieFileReader<RowData>  {
  private final HoodieStorage storage;
  private final ParquetUtils parquetUtils;
  private final StoragePath path;
  private Schema fileSchema;
  private DataType fileRowType;
  private final List<ClosableIterator<RowData>> readerIterators = new ArrayList<>();

  public HoodieRowDataParquetReader(HoodieStorage storage, StoragePath path) {
    this.storage = storage;
    this.parquetUtils = (ParquetUtils) HoodieIOFactory.getIOFactory(storage).getFileFormatUtils(HoodieFileFormat.PARQUET);
    this.path = path;
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
    //TODO boundary to follow up in later pr
    ClosableIterator<RowData> rowDataItr = getRowDataIterator(InternalSchemaManager.DISABLED, getRowType(), requestedSchema.getAvroSchema(), Collections.emptyList());
    readerIterators.add(rowDataItr);
    return new CloseableMappingIterator<>(rowDataItr, HoodieFlinkRecord::new);
  }

  @Override
  public ClosableIterator<String> getRecordKeyIterator() throws IOException {
    Schema schema = HoodieAvroUtils.getRecordKeySchema();
    ClosableIterator<RowData> rowDataItr = getRowDataIterator(InternalSchemaManager.DISABLED, getRowType(), schema, Collections.emptyList());
    return new CloseableMappingIterator<>(rowDataItr, rowData -> Objects.toString(rowData.getString(0)));
  }

  public ClosableIterator<RowData> getRowDataIterator(
      InternalSchemaManager internalSchemaManager,
      DataType dataType,
      Schema requestedSchema,
      List<Predicate> predicates) throws IOException {
    return RecordIterators.getParquetRecordIterator(storage.getConf(), internalSchemaManager, dataType, requestedSchema, path, predicates);
  }

  @Override
  public HoodieSchema getSchema() {
    if (fileSchema == null) {
      fileSchema = AvroSchemaConverter.convertToSchema(getRowType().notNull().getLogicalType());
    }
    //TODO to revisit in later pr to use HoodieSchema directly
    return HoodieSchema.fromAvroSchema(fileSchema);
  }

  public DataType getRowType() {
    if (fileRowType == null) {
      // Some types in avro are not compatible with parquet.
      // Avro only supports representing Decimals as fixed byte array
      // and therefore if we convert to Avro directly we'll lose logical type-info.
      MessageType messageType = parquetUtils.readSchema(storage, path);
      RowType rowType = ParquetSchemaConverter.convertToRowType(messageType);
      fileRowType = DataTypes.of(rowType);
    }
    return fileRowType;
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
