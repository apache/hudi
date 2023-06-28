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

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.ClusteringGroupInfo;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.io.HoodieFileWriteHandle;
import org.apache.hudi.table.HoodieTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * This class gives skeleton implementation for set of clustering execution strategy
 * that use parquet-tools commands.
 */
public abstract class ParquetToolsExecutionStrategy<T extends HoodieRecordPayload<T>>
    extends SingleSparkJobExecutionStrategy<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ParquetToolsExecutionStrategy.class);

  public ParquetToolsExecutionStrategy(
      HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  protected Stream<WriteStatus> runClusteringForGroup(ClusteringGroupInfo clusteringOps, Map<String, String> strategyParams,
                                                      boolean preserveHoodieMetadata, SerializableSchema schema,
                                                      TaskContextSupplier taskContextSupplier, String instantTime) {
    LOG.info("Starting clustering operation on input file ids.");
    List<ClusteringOperation> clusteringOperations = clusteringOps.getOperations();
    if (clusteringOperations.size() > 1) {
      throw new HoodieClusteringException("Expect only one clustering operation during rewrite: " + getClass().getName());
    }

    ClusteringOperation clusteringOperation = clusteringOperations.get(0);
    String fileId = clusteringOperation.getFileId();
    String partitionPath = clusteringOperation.getPartitionPath();
    String dataFilePathStr = clusteringOperation.getDataFilePath();
    Path oldFilePath = new Path(dataFilePathStr);
    HoodieFileWriteHandle writeHandler = new HoodieFileWriteHandle(getWriteConfig(), instantTime, getHoodieTable(),
        partitionPath, fileId, taskContextSupplier, oldFilePath);

    // Executes the parquet-tools command.
    executeTools(oldFilePath, writeHandler.getPath());
    return writeHandler.close().stream();
  }

  /**
   * This method needs to be overridden by the child classes.
   * In this method parquet-tools command can be created and executed.
   * Assuming that the parquet-tools command operate per file basis this interface allows command to run once per file.
   */
  protected abstract void executeTools(Path oldFilePath, Path newFilePath);

  /**
   * Since parquet-tools works at the file level, this method need not be used overridden.
   */
  @Override
  public Iterator<List<WriteStatus>> performClusteringWithRecordsIterator(
      final Iterator<HoodieRecord<T>> records, final int numOutputGroups, final String instantTime,
      final Map<String, String> strategyParams, final Schema schema, final List<HoodieFileGroupId> fileGroupIdList,
      final boolean preserveHoodieMetadata, final TaskContextSupplier taskContextSupplier) {
    throw new UnsupportedOperationException("Record iteration for parquet-tools is not supported.");
  }
}
