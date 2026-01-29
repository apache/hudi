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
import org.apache.hudi.common.util.BaseFileUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetReaderIterator;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.HoodieInternalRowUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport;
import org.apache.spark.sql.execution.datasources.parquet.ParquetToSparkSchemaConverter;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.apache.hudi.common.util.TypeUtils.unsafeCast;
import static org.apache.parquet.avro.AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS;
import static org.apache.parquet.avro.HoodieAvroParquetSchemaConverter.getAvroSchemaConverter;

public class HoodieSparkParquetReader implements HoodieSparkFileReader {

  private final Path path;
  private final Configuration conf;
  private final BaseFileUtils parquetUtils;
  private List<ParquetReaderIterator> readerIterators = new ArrayList<>();
  public static final String ENABLE_LOGICAL_TIMESTAMP_REPAIR = "spark.hudi.logicalTimestampField.repair.enable";
  private final StoragePath path;
  private final HoodieStorage storage;
  private final FileFormatUtils parquetUtils;
  private List<ClosableIterator> readerIterators = new ArrayList<>();
  private Option<MessageType> fileSchemaOption = Option.empty();
  private Option<StructType> structTypeOption = Option.empty();
  private Option<Schema> schemaOption = Option.empty();

  public HoodieSparkParquetReader(Configuration conf, Path path) {
    this.path = path;
    this.conf = new Configuration(conf);
    // Avoid adding record in list element when convert parquet schema to avro schema
    conf.set(ADD_LIST_ELEMENT_RECORDS, "false");
    this.parquetUtils = BaseFileUtils.getInstance(HoodieFileFormat.PARQUET);
  }

  @Override
  public String[] readMinMaxRecordKeys() {
    return parquetUtils.readMinMaxRecordKeys(conf, path);
  }

  @Override
  public BloomFilter readBloomFilter() {
    return parquetUtils.readBloomFilterFromMetadata(conf, path);
  }

  @Override
  public Set<String> filterRowKeys(Set<String> candidateRowKeys) {
    return parquetUtils.filterRowKeys(conf, path, candidateRowKeys);
  }

  @Override
  public ClosableIterator<HoodieRecord<InternalRow>> getRecordIterator(Schema readerSchema, Schema requestedSchema) throws IOException {
    ClosableIterator<InternalRow> iterator = getInternalRowIterator(readerSchema, requestedSchema);
    StructType structType = HoodieInternalRowUtils.getCachedSchema(requestedSchema);
    UnsafeProjection projection = HoodieInternalRowUtils.getCachedUnsafeProjection(structType, structType);

    return new CloseableMappingIterator<>(iterator, data -> {
      // NOTE: We have to do [[UnsafeProjection]] of incoming [[InternalRow]] to convert
      //       it to [[UnsafeRow]] holding just raw bytes
      UnsafeRow unsafeRow = projection.apply(data);
      return unsafeCast(new HoodieSparkRecord(unsafeRow));
    });
  }

  @Override
  public ClosableIterator<String> getRecordKeyIterator() throws IOException {
    Schema schema = HoodieAvroUtils.getRecordKeySchema();
    ClosableIterator<InternalRow> iterator = getInternalRowIterator(schema, schema);
    StructType structType = HoodieInternalRowUtils.getCachedSchema(schema);
    UnsafeProjection projection = HoodieInternalRowUtils.getCachedUnsafeProjection(structType, structType);

    return new CloseableMappingIterator<>(iterator, data -> {
      // NOTE: We have to do [[UnsafeProjection]] of incoming [[InternalRow]] to convert
      //       it to [[UnsafeRow]] holding just raw bytes
      UnsafeRow unsafeRow = projection.apply(data);
      HoodieSparkRecord record = unsafeCast(new HoodieSparkRecord(unsafeRow));
      return record.getRecordKey();
    });
  }

  private ClosableIterator<InternalRow> getInternalRowIterator(Schema readerSchema, Schema requestedSchema) throws IOException {
    if (requestedSchema == null) {
      requestedSchema = readerSchema;
    }
    // Set configuration for timestamp_millis type repair.
    storage.getConf().set(ENABLE_LOGICAL_TIMESTAMP_REPAIR, Boolean.toString(AvroSchemaUtils.hasTimestampMillisField(readerSchema)));

    MessageType fileSchema = getFileSchema();
    Schema nonNullSchema = AvroSchemaUtils.getNonNullTypeFromUnion(requestedSchema);
    Option<MessageType> messageSchema = Option.of(getAvroSchemaConverter(conf).convert(nonNullSchema));
    Pair<StructType, StructType> readerSchemas =
        SparkAdapterSupport$.MODULE$.sparkAdapter().getReaderSchemas(conf, readerSchema, requestedSchema, fileSchema);
    conf.set(ParquetReadSupport.PARQUET_READ_SCHEMA, readerSchemas.getLeft().json());
    conf.set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA(), readerSchemas.getRight().json());
    conf.set(SQLConf.PARQUET_BINARY_AS_STRING().key(), SQLConf.get().getConf(SQLConf.PARQUET_BINARY_AS_STRING()).toString());
    conf.set(SQLConf.PARQUET_INT96_AS_TIMESTAMP().key(), SQLConf.get().getConf(SQLConf.PARQUET_INT96_AS_TIMESTAMP()).toString());
    ParquetReader<InternalRow> reader = ParquetReader.<InternalRow>builder(
        (ReadSupport) SparkAdapterSupport$.MODULE$.sparkAdapter().getParquetReadSupport(storage, messageSchema),
            new Path(path.toUri()))
        .withConf(storage.getConf().unwrapAs(Configuration.class))
        .build();
    ParquetReaderIterator<InternalRow> parquetReaderIterator = new ParquetReaderIterator<>(reader);
    readerIterators.add(parquetReaderIterator);
    return parquetReaderIterator;
  }

  private MessageType getFileSchema() {
    if (!fileSchemaOption.isPresent()) {
      MessageType messageType = ((ParquetUtils) parquetUtils).readSchema(conf, path);
      fileSchemaOption = Option.of(messageType);
    }
    return fileSchemaOption.get();
  }

  @Override
  public Schema getSchema() {
    if (!schemaOption.isPresent()) {
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
    if (!structTypeOption.isPresent()) {
      MessageType messageType = getFileSchema();
      structTypeOption = Option.of(convertToStruct(messageType));
    }
    return structTypeOption.get();
  }

  private StructType convertToStruct(MessageType messageType) {
    return new ParquetToSparkSchemaConverter(conf).convert(messageType);
  }

  @Override
  public void close() {
    readerIterators.forEach(it -> it.close());
  }

  @Override
  public long getTotalRecords() {
    return parquetUtils.getRowCount(conf, path);
  }
}
