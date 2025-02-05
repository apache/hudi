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
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.ClusteringGroupInfo;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.io.storage.HoodieFileReader;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.cluster.strategy.ClusteringExecutionStrategy;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.client.utils.SparkPartitionUtils.getPartitionFieldVals;
import static org.apache.hudi.io.storage.HoodieSparkIOFactory.getHoodieSparkIOFactory;

/**
 * Clustering strategy to submit single spark jobs.
 * MultipleSparkJobExecution strategy is not ideal for use cases that require large number of clustering groups
 */
public abstract class SingleSparkJobExecutionStrategy<T>
    extends ClusteringExecutionStrategy<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> {
  private static final Logger LOG = LoggerFactory.getLogger(SingleSparkJobExecutionStrategy.class);

  public SingleSparkJobExecutionStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> performClustering(final HoodieClusteringPlan clusteringPlan, final Schema schema, final String instantTime) {
    final TaskContextSupplier taskContextSupplier = getEngineContext().getTaskContextSupplier();
    final SerializableSchema serializableSchema = new SerializableSchema(schema);
    final List<ClusteringGroupInfo> clusteringGroupInfos = clusteringPlan.getInputGroups().stream().map(ClusteringGroupInfo::create).collect(Collectors.toList());

    HoodieData<WriteStatus> writeStatus = getEngineContext().parallelize(clusteringGroupInfos).map(group -> {
      return performClusteringForGroup(group, clusteringPlan.getStrategy().getStrategyParams(),
          Option.ofNullable(clusteringPlan.getPreserveHoodieMetadata()).orElse(false),
          serializableSchema, taskContextSupplier, instantTime);
    }).flatMap(List::iterator);
    HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata = new HoodieWriteMetadata<>();
    writeMetadata.setWriteStatuses(writeStatus);
    return writeMetadata;
  }

  /**
   * Submit a task to execute clustering for the group.
   */
  protected abstract List<WriteStatus> performClusteringForGroup(ClusteringGroupInfo clusteringGroup, Map<String, String> strategyParams,
                                                  boolean preserveHoodieMetadata, SerializableSchema schema,
                                                  TaskContextSupplier taskContextSupplier, String instantTime);

  protected Option<HoodieFileReader> getBaseOrBootstrapFileReader(ClusteringOperation clusteringOp) {
    HoodieStorage storage = getHoodieTable().getStorage();
    StorageConfiguration<?> storageConf = getHoodieTable().getStorageConf();
    HoodieTableConfig tableConfig = getHoodieTable().getMetaClient().getTableConfig();
    String bootstrapBasePath = tableConfig.getBootstrapBasePath().orElse(null);
    Option<String[]> partitionFields = tableConfig.getPartitionFields();
    Option<HoodieFileReader> baseFileReaderOpt = ClusteringUtils.getBaseFileReader(storage, recordType, writeConfig, clusteringOp.getDataFilePath());
    if (baseFileReaderOpt.isEmpty()) {
      return Option.empty();
    }
    try {
      HoodieFileReader baseFileReader = baseFileReaderOpt.get();
      // handle bootstrap path
      if (StringUtils.nonEmpty(clusteringOp.getBootstrapFilePath()) && StringUtils.nonEmpty(bootstrapBasePath)) {
        String bootstrapFilePath = clusteringOp.getBootstrapFilePath();
        Object[] partitionValues = new Object[0];
        if (partitionFields.isPresent()) {
          int startOfPartitionPath = bootstrapFilePath.indexOf(bootstrapBasePath) + bootstrapBasePath.length() + 1;
          String partitionFilePath = bootstrapFilePath.substring(startOfPartitionPath, bootstrapFilePath.lastIndexOf("/"));
          partitionValues = getPartitionFieldVals(partitionFields, partitionFilePath, bootstrapBasePath, baseFileReader.getSchema(),
              storageConf.unwrapAs(Configuration.class));
        }
        return Option.of(getHoodieSparkIOFactory(storage).getReaderFactory(recordType).newBootstrapFileReader(
            baseFileReader,
            getHoodieSparkIOFactory(storage).getReaderFactory(recordType).getFileReader(
                writeConfig, new StoragePath(bootstrapFilePath)), partitionFields,
            partitionValues));
      }
      return baseFileReaderOpt;
    } catch (IOException e) {
      throw new HoodieClusteringException("Error reading base file", e);
    }
  }

}
