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

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ClusteringGroupInfo;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.HoodieBinaryCopyHandle;
import org.apache.hudi.io.BinaryCopyHandleFactory;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;

/**
 * Clustering strategy to submit single spark jobs using streaming copy
 * PAY ATTENTION!!!
 * IN THIS STRATEGY
 *  1. Only support clustering for cow table.
 *  2. Sort function is not supported yet.
 *  3. Each clustering group only has one task to write.
 */
public class SparkBinaryCopyClusteringExecutionStrategy<T> extends SingleSparkJobExecutionStrategy<T> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkBinaryCopyClusteringExecutionStrategy.class);

  public SparkBinaryCopyClusteringExecutionStrategy(
      HoodieTable table,
      HoodieEngineContext engineContext,
      HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> performClustering(
      HoodieClusteringPlan clusteringPlan,
      Schema schema,
      String instantTime) {
    if (!supportBinaryStreamCopy()) {
      String message = "1. Only support for CoW table 2. Not support for sort function";
      throw new UnsupportedOperationException(message);

    }
    LOG.info("Required conditions are currently satisfied, enabling the optimization of using binary stream copy ");
    return super.performClustering(clusteringPlan, schema, instantTime);
  }

  /**
   * Submit job to execute clustering for the group.
   */
  @Override
  protected List<WriteStatus> performClusteringForGroup(ReaderContextFactory<T> readerContextFactory, ClusteringGroupInfo clusteringOps, Map<String, String> strategyParams,
                                                    boolean preserveHoodieMetadata, SerializableSchema schema,
                                                    TaskContextSupplier taskContextSupplier, String instantTime) {
    List<HoodieFileGroupId> inputFileIds = clusteringOps.getOperations()
        .stream()
        .map(op -> new HoodieFileGroupId(op.getPartitionPath(), op.getFileId()))
        .collect(Collectors.toList());
    List<StoragePath> inputFilePaths = clusteringOps.getOperations()
        .stream()
        .map(op -> new StoragePath(op.getDataFilePath()))
        .collect(Collectors.toList());

    BinaryCopyHandleFactory factory = new BinaryCopyHandleFactory(inputFilePaths);
    HoodieBinaryCopyHandle handler = factory.create(
        getWriteConfig(),
        instantTime,
        getHoodieTable(),
        inputFileIds.get(0).getPartitionPath(),
        FSUtils.createNewFileIdPfx(),
        taskContextSupplier);

    handler.write();
    return new ArrayList<>(handler.close());
  }

  private boolean supportBinaryStreamCopy() {
    return this.getHoodieTable().getMetaClient().getTableType() == COPY_ON_WRITE;
  }
}
