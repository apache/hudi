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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.bootstrap.BootstrapRecordPayload;
import org.apache.hudi.client.bootstrap.BootstrapWriteStatus;
import org.apache.hudi.common.bootstrap.FileStatusUtils;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BootstrapFileMapping;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.AvroOrcUtils;
import org.apache.hudi.common.util.OrcReaderIterator;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.execution.SparkBoundedInMemoryExecutor;
import org.apache.hudi.io.HoodieBootstrapHandle;
import org.apache.hudi.keygen.KeyGeneratorInterface;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.hudi.avro.model.HoodieFileStatus;

import java.io.IOException;

class OrcMetadataHandler implements MetadataHandler {
  private static final Logger LOG = LogManager.getLogger(OrcMetadataHandler.class);

  HoodieWriteConfig config;

  HoodieTable table;

  HoodieFileStatus srcFileStatus;

  public OrcMetadataHandler(HoodieWriteConfig config, HoodieTable table, HoodieFileStatus srcFileStatus) {
    this.config = config;
    this.table = table;
    this.srcFileStatus = srcFileStatus;
  }

  public BootstrapWriteStatus runMetadataBootstrap(String srcPartitionPath, String partitionPath, KeyGeneratorInterface keyGenerator) {
    Path sourceFilePath = FileStatusUtils.toPath(srcFileStatus.getPath());
    HoodieBootstrapHandle<?, ?, ?, ?> bootstrapHandle = new HoodieBootstrapHandle(config,
            HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS, table, partitionPath, FSUtils.createNewFileIdPfx(),
            table.getTaskContextSupplier());
    Schema avroSchema = null;
    try {
      Reader orcReader = OrcFile.createReader(sourceFilePath, OrcFile.readerOptions(table.getHadoopConf()));

      TypeDescription orcSchema = orcReader.getSchema();

      avroSchema =  AvroOrcUtils.createAvroSchema(orcSchema);

      Schema recordKeySchema = HoodieAvroUtils.generateProjectionSchema(avroSchema,
              keyGenerator.getRecordKeyFieldNames());
      LOG.info("Schema to be used for reading record Keys :" + recordKeySchema);
      AvroReadSupport.setAvroReadSchema(table.getHadoopConf(), recordKeySchema);
      AvroReadSupport.setRequestedProjection(table.getHadoopConf(), recordKeySchema);

      BoundedInMemoryExecutor<GenericRecord, HoodieRecord, Void> wrapper = null;
      try (RecordReader reader = orcReader.rows(new Reader.Options(table.getHadoopConf()).schema(orcSchema))) {
        wrapper = new SparkBoundedInMemoryExecutor<GenericRecord, HoodieRecord, Void>(config,
                new OrcReaderIterator(reader,avroSchema,orcSchema), new BootstrapRecordConsumer(bootstrapHandle), inp -> {
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
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }

    BootstrapWriteStatus writeStatus = (BootstrapWriteStatus) bootstrapHandle.writeStatuses().get(0);
    BootstrapFileMapping bootstrapFileMapping = new BootstrapFileMapping(config.getBootstrapSourceBasePath(),
            srcPartitionPath, partitionPath, srcFileStatus, writeStatus.getFileId());
    writeStatus.setBootstrapSourceFileMapping(bootstrapFileMapping);
    return writeStatus;

  }
}

