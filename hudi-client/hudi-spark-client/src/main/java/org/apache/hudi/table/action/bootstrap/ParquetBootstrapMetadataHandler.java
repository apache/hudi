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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.client.bootstrap.BootstrapRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ParquetReaderIterator;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.execution.SparkBoundedInMemoryExecutor;
import org.apache.hudi.io.HoodieBootstrapHandle;
import org.apache.hudi.keygen.KeyGeneratorInterface;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;

class ParquetBootstrapMetadataHandler extends BaseBootstrapMetadataHandler {
  private static final Logger LOG = LogManager.getLogger(ParquetBootstrapMetadataHandler.class);

  public ParquetBootstrapMetadataHandler(HoodieWriteConfig config, HoodieTable table, HoodieFileStatus srcFileStatus) {
    super(config, table, srcFileStatus);
  }

  @Override
  Schema getAvroSchema(Path sourceFilePath) throws IOException {
    ParquetMetadata readFooter = ParquetFileReader.readFooter(table.getHadoopConf(), sourceFilePath,
        ParquetMetadataConverter.NO_FILTER);
    MessageType parquetSchema = readFooter.getFileMetaData().getSchema();
    return new AvroSchemaConverter().convert(parquetSchema);
  }

  @Override
  void executeBootstrap(HoodieBootstrapHandle<?, ?, ?, ?> bootstrapHandle,
                        Path sourceFilePath, KeyGeneratorInterface keyGenerator, String partitionPath, Schema avroSchema) throws Exception {
    BoundedInMemoryExecutor<GenericRecord, HoodieRecord, Void> wrapper = null;
    try {
      ParquetReader<IndexedRecord> reader =
          AvroParquetReader.<IndexedRecord>builder(sourceFilePath).withConf(table.getHadoopConf()).build();
      wrapper = new SparkBoundedInMemoryExecutor<GenericRecord, HoodieRecord, Void>(config,
          new ParquetReaderIterator(reader), new BootstrapRecordConsumer(bootstrapHandle), inp -> {
        String recKey = keyGenerator.getKey(inp).getRecordKey();
        GenericRecord gr = new GenericData.Record(HoodieAvroUtils.RECORD_KEY_SCHEMA);
        gr.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, recKey);
        BootstrapRecordPayload payload = new BootstrapRecordPayload(gr);
        HoodieRecord rec = new HoodieRecord(new HoodieKey(recKey, partitionPath), payload);
        return rec;
      });
      wrapper.execute();
    } catch (Exception e) {
      throw new HoodieException(e);
    } finally {
      bootstrapHandle.close();
      if (null != wrapper) {
        wrapper.shutdownNow();
      }
    }
  }
}

