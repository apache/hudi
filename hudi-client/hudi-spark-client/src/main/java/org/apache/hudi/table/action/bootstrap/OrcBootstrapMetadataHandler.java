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

import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.client.bootstrap.BootstrapRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.util.AvroOrcUtils;
import org.apache.hudi.common.util.OrcReaderIterator;
import org.apache.hudi.common.util.queue.HoodieExecutor;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.HoodieBootstrapHandle;
import org.apache.hudi.keygen.KeyGeneratorInterface;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.util.ExecutorFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hudi.io.HoodieBootstrapHandle.METADATA_BOOTSTRAP_RECORD_SCHEMA;

class OrcBootstrapMetadataHandler extends BaseBootstrapMetadataHandler {
  private static final Logger LOG = LoggerFactory.getLogger(OrcBootstrapMetadataHandler.class);

  public OrcBootstrapMetadataHandler(HoodieWriteConfig config, HoodieTable table, HoodieFileStatus srcFileStatus) {
    super(config, table, srcFileStatus);
  }

  @Override
  Schema getAvroSchema(Path sourceFilePath) throws IOException {
    Reader orcReader = OrcFile.createReader(sourceFilePath, OrcFile.readerOptions(table.getHadoopConf()));
    TypeDescription orcSchema = orcReader.getSchema();
    return AvroOrcUtils.createAvroSchema(orcSchema);
  }

  @Override
  void executeBootstrap(HoodieBootstrapHandle<?, ?, ?, ?> bootstrapHandle, Path sourceFilePath, KeyGeneratorInterface keyGenerator,
                        String partitionPath, Schema avroSchema) throws Exception {
    // TODO support spark orc reader
    if (config.getRecordMerger().getRecordType() == HoodieRecordType.SPARK) {
      throw new UnsupportedOperationException();
    }
    Reader orcReader = OrcFile.createReader(sourceFilePath, OrcFile.readerOptions(table.getHadoopConf()));
    TypeDescription orcSchema = AvroOrcUtils.createOrcSchema(avroSchema);
    HoodieExecutor<Void> executor = null;
    RecordReader reader = orcReader.rows(new Reader.Options(table.getHadoopConf()).schema(orcSchema));
    try {
      executor = ExecutorFactory.create(config, new OrcReaderIterator<GenericRecord>(reader, avroSchema, orcSchema),
          new BootstrapRecordConsumer(bootstrapHandle), inp -> {
            String recKey = keyGenerator.getKey(inp).getRecordKey();
            GenericRecord gr = new GenericData.Record(METADATA_BOOTSTRAP_RECORD_SCHEMA);
            gr.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, recKey);
            BootstrapRecordPayload payload = new BootstrapRecordPayload(gr);
            HoodieRecord rec = new HoodieAvroRecord(new HoodieKey(recKey, partitionPath), payload);
            return rec;
          }, table.getPreExecuteRunnable());
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
}

