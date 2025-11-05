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

import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetReaderIterator;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.SchemaRepair;
import org.apache.spark.sql.HoodieInternalRowUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.datasources.parquet.HoodieParquetReadSupport;
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport;
import org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter;
import org.apache.spark.sql.execution.datasources.parquet.SparkBasicSchemaEvolution;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import scala.Option$;

import static org.apache.hudi.common.util.TypeUtils.unsafeCast;
import static org.apache.parquet.avro.AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS;
import static org.apache.parquet.avro.HoodieAvroParquetSchemaConverter.getAvroSchemaConverter;

public class HoodieSparkParquetReader implements HoodieSparkFileReader {

  private final StoragePath path;
  private final HoodieStorage storage;
  private final FileFormatUtils parquetUtils;
  private final List<ClosableIterator> readerIterators = new ArrayList<>();
  private Option<MessageType> fileSchemaOption = Option.empty();
  private Option<StructType> structTypeOption = Option.empty();
  private Option<Schema> schemaOption = Option.empty();

  public HoodieSparkParquetReader(HoodieStorage storage, StoragePath path) {
    this.path = path;
    this.storage = storage.newInstance(path, storage.getConf().newInstance());
    // Avoid adding record in list element when convert parquet schema to avro schema
    this.storage.getConf().set(ADD_LIST_ELEMENT_RECORDS, "false");
    this.parquetUtils = HoodieIOFactory.getIOFactory(storage)
        .getFileFormatUtils(HoodieFileFormat.PARQUET);
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
  public ClosableIterator<HoodieRecord<InternalRow>> getRecordIterator(Schema readerSchema, Schema requestedSchema) throws IOException {
    return getRecordIterator(requestedSchema);
  }

  @Override
  public ClosableIterator<HoodieRecord<InternalRow>> getRecordIterator(Schema schema) throws IOException {
    ClosableIterator<UnsafeRow> iterator = getUnsafeRowIterator(schema);
    return new CloseableMappingIterator<>(iterator, data -> unsafeCast(new HoodieSparkRecord(data)));
  }

  @Override
  public ClosableIterator<String> getRecordKeyIterator() throws IOException {
    Schema schema = HoodieAvroUtils.getRecordKeySchema();
    ClosableIterator<UnsafeRow> iterator = getUnsafeRowIterator(schema);
    return new CloseableMappingIterator<>(iterator, data -> {
      HoodieSparkRecord record = unsafeCast(new HoodieSparkRecord(data));
      return record.getRecordKey();
    });
  }

  public ClosableIterator<UnsafeRow> getUnsafeRowIterator(Schema requestedSchema) throws IOException {
    Schema nonNullSchema = AvroSchemaUtils.resolveNullableSchema(requestedSchema);
    StructType nonNullStructSchema = HoodieInternalRowUtils.getCachedSchema(nonNullSchema);
    Option<MessageType> messageSchema = Option.of(getAvroSchemaConverter(storage.getConf().unwrapAs(Configuration.class)).convert(nonNullSchema));
    StructType repairedDataStructType = convertToStruct(SchemaRepair.repairLogicalTypes(getFileSchema(), messageSchema));
    SparkBasicSchemaEvolution evolution = new SparkBasicSchemaEvolution(repairedDataStructType, nonNullStructSchema, SQLConf.get().sessionLocalTimeZone());
    String readSchemaJson = evolution.getRequestSchema().json();
    storage.getConf().set(ParquetReadSupport.PARQUET_READ_SCHEMA, readSchemaJson);
    storage.getConf().set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA(), readSchemaJson);
    storage.getConf().set(SQLConf.PARQUET_BINARY_AS_STRING().key(), SQLConf.get().getConf(SQLConf.PARQUET_BINARY_AS_STRING()).toString());
    storage.getConf().set(SQLConf.PARQUET_INT96_AS_TIMESTAMP().key(), SQLConf.get().getConf(SQLConf.PARQUET_INT96_AS_TIMESTAMP()).toString());
    ParquetReader<InternalRow> reader = ParquetReader.builder(new HoodieParquetReadSupport(Option$.MODULE$.empty(), true,
            SparkAdapterSupport$.MODULE$.sparkAdapter().getRebaseSpec("CORRECTED"),
            SparkAdapterSupport$.MODULE$.sparkAdapter().getRebaseSpec("LEGACY"), messageSchema),
            new Path(path.toUri()))
        .withConf(storage.getConf().unwrapAs(Configuration.class))
        .build();
    UnsafeProjection projection = evolution.generateUnsafeProjection();
    ParquetReaderIterator<InternalRow> parquetReaderIterator = new ParquetReaderIterator<>(reader);
    CloseableMappingIterator<InternalRow, UnsafeRow> projectedIterator = new CloseableMappingIterator<>(parquetReaderIterator, projection::apply);
    readerIterators.add(projectedIterator);
    return projectedIterator;
  }

  private MessageType getFileSchema() {
    if (fileSchemaOption.isEmpty()) {
      MessageType messageType = ((ParquetUtils) parquetUtils).readSchema(storage, path);
      fileSchemaOption = Option.of(messageType);
    }
    return fileSchemaOption.get();
  }

  @Override
  public Schema getSchema() {
    if (schemaOption.isEmpty()) {
      // Some types in avro are not compatible with parquet.
      // Avro only supports representing Decimals as fixed byte array
      // and therefore if we convert to Avro directly we'll lose logical type-info.
      MessageType messageType = getFileSchema();
      StructType structType = getStructSchema();
      schemaOption = Option.of(SparkAdapterSupport$.MODULE$.sparkAdapter()
          .getAvroSchemaConverters()
          .toAvroType(structType, true, messageType.getName(), StringUtils.EMPTY_STRING));
    }
    return schemaOption.get();
  }

  protected StructType getStructSchema() {
    if (structTypeOption.isEmpty()) {
      MessageType messageType = getFileSchema();
      structTypeOption = Option.of(convertToStruct(messageType));
    }
    return structTypeOption.get();
  }

  private StructType convertToStruct(MessageType messageType) {
    return new ParquetToSparkSchemaConverter(storage.getConf().unwrapAs(Configuration.class)).convert(messageType);
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
