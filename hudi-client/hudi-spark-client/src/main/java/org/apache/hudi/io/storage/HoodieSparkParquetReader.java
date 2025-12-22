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
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.common.util.ParquetReaderIterator;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

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

public class HoodieSparkParquetReader implements HoodieSparkFileReader {

  private final StoragePath path;
  private final HoodieStorage storage;
  private final FileFormatUtils parquetUtils;
  private List<ParquetReaderIterator> readerIterators = new ArrayList<>();

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
  public Set<String> filterRowKeys(Set<String> candidateRowKeys) {
    return parquetUtils.filterRowKeys(storage, path, candidateRowKeys);
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
    StructType readerStructType = HoodieInternalRowUtils.getCachedSchema(readerSchema);
    StructType requestedStructType = HoodieInternalRowUtils.getCachedSchema(requestedSchema);
    storage.getConf().set(ParquetReadSupport.PARQUET_READ_SCHEMA, readerStructType.json());
    storage.getConf().set(ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA(), requestedStructType.json());
    storage.getConf().set(SQLConf.PARQUET_BINARY_AS_STRING().key(), SQLConf.get().getConf(SQLConf.PARQUET_BINARY_AS_STRING()).toString());
    storage.getConf().set(SQLConf.PARQUET_INT96_AS_TIMESTAMP().key(), SQLConf.get().getConf(SQLConf.PARQUET_INT96_AS_TIMESTAMP()).toString());
    ParquetReader<InternalRow> reader = ParquetReader.<InternalRow>builder((ReadSupport) new ParquetReadSupport(), new Path(path.toUri()))
        .withConf(storage.getConf().unwrapAs(Configuration.class))
        .build();
    ParquetReaderIterator<InternalRow> parquetReaderIterator = new ParquetReaderIterator<>(reader);
    readerIterators.add(parquetReaderIterator);
    return parquetReaderIterator;
  }

  @Override
  public Schema getSchema() {
    // Some types in avro are not compatible with parquet.
    // Avro only supports representing Decimals as fixed byte array
    // and therefore if we convert to Avro directly we'll lose logical type-info.
    MessageType messageType = ((ParquetUtils) parquetUtils).readSchema(storage, path);
    StructType structType = new ParquetToSparkSchemaConverter(storage.getConf().unwrapAs(Configuration.class)).convert(messageType);
    return SparkAdapterSupport$.MODULE$.sparkAdapter()
        .getAvroSchemaConverters()
        .toAvroType(structType, true, messageType.getName(), StringUtils.EMPTY_STRING);
  }

  @Override
  public void close() {
    readerIterators.forEach(ParquetReaderIterator::close);
  }

  @Override
  public long getTotalRecords() {
    return parquetUtils.getRowCount(storage, path);
  }
}
