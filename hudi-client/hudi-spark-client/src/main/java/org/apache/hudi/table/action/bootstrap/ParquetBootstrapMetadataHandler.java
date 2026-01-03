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

package org.apache.hudi.table.action.bootstrap;

import org.apache.avro.Schema;
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.queue.HoodieExecutor;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.HoodieBootstrapHandle;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.keygen.KeyGeneratorInterface;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.util.ExecutorFactory;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.HoodieInternalRowUtils$;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.io.IOException;
import java.util.function.Function;

import static org.apache.hudi.io.HoodieBootstrapHandle.METADATA_BOOTSTRAP_RECORD_SCHEMA;
import static org.apache.hudi.io.storage.HoodieSparkIOFactory.getHoodieSparkIOFactory;
import static org.apache.parquet.avro.HoodieAvroParquetSchemaConverter.getAvroSchemaConverter;

class ParquetBootstrapMetadataHandler extends BaseBootstrapMetadataHandler {

  public ParquetBootstrapMetadataHandler(HoodieWriteConfig config, HoodieTable table, HoodieFileStatus srcFileStatus) {
    super(config, table, srcFileStatus);
  }

  @Override
  HoodieSchema getSchema(StoragePath sourceFilePath) throws IOException {
    ParquetMetadata readFooter = ParquetFileReader.readFooter(
        (Configuration) table.getStorageConf().unwrap(), new Path(sourceFilePath.toUri()),
        ParquetMetadataConverter.NO_FILTER);
    MessageType parquetSchema = readFooter.getFileMetaData().getSchema();
    Schema schema = getAvroSchemaConverter((Configuration) table.getStorageConf().unwrap()).convert(parquetSchema);
    return HoodieSchema.fromAvroSchema(schema);
  }

  @Override
  protected void executeBootstrap(HoodieBootstrapHandle<?, ?, ?, ?> bootstrapHandle,
                                  StoragePath sourceFilePath,
                                  KeyGeneratorInterface keyGenerator,
                                  String partitionPath,
                                  HoodieSchema schema) throws Exception {
    HoodieRecord.HoodieRecordType recordType = table.getConfig().getRecordMerger().getRecordType();

    HoodieFileReader reader = getHoodieSparkIOFactory(table.getStorage()).getReaderFactory(recordType)
        .getFileReader(table.getConfig(), sourceFilePath);

    HoodieExecutor<Void> executor = null;
    try {
      Function<HoodieRecord, HoodieRecord> transformer = record -> {
        String recordKey = record.getRecordKey(schema, Option.of(keyGenerator));
        return createNewMetadataBootstrapRecord(recordKey, partitionPath, recordType)
            // NOTE: Record have to be cloned here to make sure if it holds low-level engine-specific
            //       payload pointing into a shared, mutable (underlying) buffer we get a clean copy of
            //       it since these records will be inserted into the queue later.
            .copy();
      };
      ClosableIterator<HoodieRecord> recordIterator = reader.getRecordIterator(schema);
      executor = ExecutorFactory.create(config, recordIterator,
          new BootstrapRecordConsumer(bootstrapHandle), transformer, table.getPreExecuteRunnable());
      executor.execute();
    } catch (Exception e) {
      throw new HoodieException(e);
    } finally {
      // NOTE: If executor is initialized it's responsible for gracefully shutting down
      //       both producer and consumer
      if (executor != null) {
        executor.shutdownNow();
        executor.awaitTermination();
      } else {
        reader.close();
        bootstrapHandle.close();
      }
    }
  }

  private HoodieRecord createNewMetadataBootstrapRecord(String recordKey, String partitionPath, HoodieRecord.HoodieRecordType recordType) {
    HoodieKey hoodieKey = new HoodieKey(recordKey, partitionPath);
    switch (recordType) {
      case AVRO:
        GenericRecord avroRecord = new GenericData.Record(METADATA_BOOTSTRAP_RECORD_SCHEMA.toAvroSchema());
        avroRecord.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, recordKey);
        return new HoodieAvroIndexedRecord(hoodieKey, avroRecord);

      case SPARK:
        StructType schema = HoodieInternalRowUtils$.MODULE$.getCachedSchema(METADATA_BOOTSTRAP_RECORD_SCHEMA);
        UnsafeProjection unsafeProjection = HoodieInternalRowUtils$.MODULE$.getCachedUnsafeProjection(schema, schema);

        GenericInternalRow row = new GenericInternalRow(METADATA_BOOTSTRAP_RECORD_SCHEMA.getFields().size());
        row.update(HoodieRecord.RECORD_KEY_META_FIELD_ORD, UTF8String.fromString(recordKey));

        UnsafeRow unsafeRow = unsafeProjection.apply(row);

        return new HoodieSparkRecord(hoodieKey, unsafeRow,false);

      default:
        throw new UnsupportedOperationException(String.format("Record type %s is not supported yet!", recordType));
    }

  }
}

