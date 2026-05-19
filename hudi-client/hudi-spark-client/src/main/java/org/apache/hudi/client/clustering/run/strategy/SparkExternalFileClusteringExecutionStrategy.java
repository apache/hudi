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

package org.apache.hudi.client.clustering.run.strategy;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ClusteringGroupInfo;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.io.ExternalFileClusteringWriteHandle;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * This class gives skeleton implementation for set of clustering execution strategy
 * that use external file transformation commands.
 */
public abstract class SparkExternalFileClusteringExecutionStrategy<T extends HoodieRecordPayload<T>>
    extends SingleSparkJobExecutionStrategy<T> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkExternalFileClusteringExecutionStrategy.class);

  public SparkExternalFileClusteringExecutionStrategy(
      HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  protected List<WriteStatus> performClusteringForGroup(ReaderContextFactory<T> readerContextFactory,
                                                        ClusteringGroupInfo clusteringOps,
                                                        Map<String, String> strategyParams,
                                                        boolean preserveHoodieMetadata,
                                                        HoodieSchema schema,
                                                        TaskContextSupplier taskContextSupplier,
                                                        String instantTime) {
    LOG.info("Starting clustering operation on input file ids.");
    if (!preserveHoodieMetadata) {
      throw new HoodieClusteringException(
          "External file clustering strategy cannot rewrite Hudi metadata fields. preserveHoodieMetadata must be true.");
    }
    List<ClusteringOperation> clusteringOperations = clusteringOps.getOperations();
    if (clusteringOperations.size() != 1) {
      throw new HoodieClusteringException("Expect only one clustering operation during rewrite: " + getClass().getName());
    }

    ClusteringOperation clusteringOperation = clusteringOperations.get(0);
    String fileId = FSUtils.createNewFileIdPfx();
    String partitionPath = clusteringOperation.getPartitionPath();
    String dataFilePathStr = clusteringOperation.getDataFilePath();
    StoragePath oldFilePath = new StoragePath(dataFilePathStr);
    ExternalFileClusteringWriteHandle writeHandler = new ExternalFileClusteringWriteHandle(getWriteConfig(), instantTime, getHoodieTable(),
        partitionPath, fileId, taskContextSupplier, oldFilePath);

    try {
      // Executes the file transformation.
      transformFile(oldFilePath, writeHandler.getPath());
    } catch (Exception e) {
      // Clean up partial output file if transformation fails.
      try {
        getHoodieTable().getStorage().deleteFile(writeHandler.getPath());
      } catch (Exception deleteEx) {
        LOG.warn("Failed to clean up partial output file: " + writeHandler.getPath(), deleteEx);
      }
      throw new HoodieClusteringException("Failed to transform file: " + dataFilePathStr, e);
    }
    return writeHandler.close();
  }

  /**
   * This method needs to be overridden by the child classes.
   * In this method external file transformation can be created and executed.
   * Assuming that the transformation operates per file basis, this interface allows the command to run once per file.
   */
  protected abstract void transformFile(StoragePath oldFilePath, StoragePath newFilePath);

}
