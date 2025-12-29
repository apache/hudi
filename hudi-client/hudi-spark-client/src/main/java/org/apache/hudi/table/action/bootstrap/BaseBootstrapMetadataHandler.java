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
import org.apache.hudi.avro.model.HoodiePath;
import org.apache.hudi.client.bootstrap.BootstrapWriteStatus;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BootstrapFileMapping;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.HoodieBootstrapHandle;
import org.apache.hudi.keygen.KeyGeneratorInterface;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public abstract class BaseBootstrapMetadataHandler implements BootstrapMetadataHandler {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetBootstrapMetadataHandler.class);
  protected HoodieWriteConfig config;
  protected HoodieTable table;
  protected HoodieFileStatus srcFileStatus;

  public BaseBootstrapMetadataHandler(HoodieWriteConfig config, HoodieTable table, HoodieFileStatus srcFileStatus) {
    this.config = config;
    this.table = table;
    this.srcFileStatus = srcFileStatus;
  }

  public BootstrapWriteStatus runMetadataBootstrap(String srcPartitionPath, String partitionPath, KeyGeneratorInterface keyGenerator) {
    HoodiePath path = srcFileStatus.getPath();
    StoragePath sourceFilePath = path != null ? new StoragePath(path.getUri()) : null;
    HoodieBootstrapHandle<?, ?, ?, ?> bootstrapHandle = new HoodieBootstrapHandle(config, HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS,
        table, partitionPath, FSUtils.createNewFileIdPfx(), table.getTaskContextSupplier());
    try {
      HoodieSchema schema = getSchema(sourceFilePath);
      List<String> recordKeyColumns = keyGenerator.getRecordKeyFieldNames().stream()
          .map(HoodieAvroUtils::getRootLevelFieldName)
          .collect(Collectors.toList());
      HoodieSchema recordKeySchema = HoodieSchemaUtils.generateProjectionSchema(schema, recordKeyColumns);

      LOG.info("Schema to be used for reading record keys: " + recordKeySchema);

      executeBootstrap(bootstrapHandle, sourceFilePath, keyGenerator, partitionPath, recordKeySchema);
    } catch (Exception e) {
      throw new HoodieException(e.getMessage(), e);
    }

    BootstrapWriteStatus writeStatus = (BootstrapWriteStatus) bootstrapHandle.getWriteStatuses().get(0);
    BootstrapFileMapping bootstrapFileMapping = new BootstrapFileMapping(
        config.getBootstrapSourceBasePath(), srcPartitionPath, partitionPath,
        srcFileStatus, writeStatus.getFileId());
    writeStatus.setBootstrapSourceFileMapping(bootstrapFileMapping);
    return writeStatus;
  }

  abstract HoodieSchema getSchema(StoragePath sourceFilePath) throws IOException;

  abstract void executeBootstrap(HoodieBootstrapHandle<?, ?, ?, ?> bootstrapHandle,
                                 StoragePath sourceFilePath, KeyGeneratorInterface keyGenerator, String partitionPath, HoodieSchema schema) throws Exception;
}
