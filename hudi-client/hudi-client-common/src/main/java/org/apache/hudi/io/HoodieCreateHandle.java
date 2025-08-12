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

package org.apache.hudi.io;

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Map;

@NotThreadSafe
public class HoodieCreateHandle<T, I, K, O> extends AbstractHoodieCreateHandle<T, I, K, O> {

  public HoodieCreateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                            String partitionPath, String fileId, TaskContextSupplier taskContextSupplier) {
    this(config, instantTime, hoodieTable, partitionPath, fileId, Option.empty(),
        taskContextSupplier, false);
  }

  public HoodieCreateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                            String partitionPath, String fileId, TaskContextSupplier taskContextSupplier,
                            boolean preserveMetadata) {
    this(config, instantTime, hoodieTable, partitionPath, fileId, Option.empty(),
        taskContextSupplier, preserveMetadata);
  }

  public HoodieCreateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                            String partitionPath, String fileId, Option<Schema> overriddenSchema,
                            TaskContextSupplier taskContextSupplier) {
    this(config, instantTime, hoodieTable, partitionPath, fileId, overriddenSchema, taskContextSupplier, false);
  }

  public HoodieCreateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
                            String partitionPath, String fileId, Option<Schema> overriddenSchema,
                            TaskContextSupplier taskContextSupplier, boolean preserveMetadata) {
    super(config, instantTime, hoodieTable, partitionPath, fileId, overriddenSchema,
        taskContextSupplier, preserveMetadata);
    this.logger = LoggerFactory.getLogger(HoodieCreateHandle.class);

    try {
      HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(fs, instantTime,
          new Path(config.getBasePath()), FSUtils.getPartitionPath(config.getBasePath(), partitionPath),
          hoodieTable.getPartitionMetafileFormat());
      partitionMetadata.trySave(getPartitionId());
      createMarkerFile(partitionPath, FSUtils.makeBaseFileName(this.instantTime, this.writeToken, this.fileId, hoodieTable.getBaseFileExtension()));
      this.fileWriter = HoodieFileWriterFactory.getFileWriter(instantTime, path, hoodieTable.getHadoopConf(), config,
        writeSchemaWithMetaFields, this.taskContextSupplier, config.getRecordMerger().getRecordType());
    } catch (IOException e) {
      throw new HoodieInsertException("Failed to initialize HoodieStorageWriter for path " + path, e);
    }
    logger.info("New CreateHandle for partition {} with fileId {}", partitionPath, fileId);
  }

  /**
   * Called by the compactor code path.
   */
  public HoodieCreateHandle(HoodieWriteConfig config, String instantTime, HoodieTable<T, I, K, O> hoodieTable,
      String partitionPath, String fileId, Map<String, HoodieRecord<T>> recordMap,
      TaskContextSupplier taskContextSupplier) {
    // preserveMetadata is disabled by default for MDT but enabled otherwise
    this(config, instantTime, hoodieTable, partitionPath, fileId, taskContextSupplier, !HoodieTableMetadata.isMetadataTable(config.getBasePath()));
    this.recordMap = recordMap;
    this.useWriterSchema = true;
  }
}
