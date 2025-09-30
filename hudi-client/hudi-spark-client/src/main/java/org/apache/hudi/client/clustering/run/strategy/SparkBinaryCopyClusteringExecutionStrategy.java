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
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ClusteringGroupInfo;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.io.HoodieBinaryCopyHandle;
import org.apache.hudi.io.BinaryCopyHandleFactory;
import org.apache.hudi.parquet.io.ParquetBinaryCopyChecker;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;
import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS;

/**
 * Clustering strategy to submit single spark jobs using streaming copy
 * PAY ATTENTION!!!
 * IN THIS STRATEGY
 *  1. Only support clustering for cow table.
 *  2. Sort function is not supported yet.
 *  3. Each clustering group only has one task to write.
 */
public class SparkBinaryCopyClusteringExecutionStrategy<T> extends SparkSortAndSizeExecutionStrategy<T> {

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

    List<ClusteringGroupInfo> clusteringGroupInfos = clusteringPlan.getInputGroups()
        .stream()
        .map(ClusteringGroupInfo::create)
        .collect(Collectors.toList());
    if (!supportBinaryStreamCopy(clusteringGroupInfos, clusteringPlan.getStrategy().getStrategyParams())) {
      LOG.info("Required conditions for binary stream copy are currently not satisfied, falling back to default clustering behavior");
      // reset write config
      this.writeConfig = HoodieWriteConfig.newBuilder().withProperties(writeConfig.getProps())
          .withStorageConfig(HoodieStorageConfig.newBuilder().parquetWriteLegacyFormat("false").build()).build();
      return super.performClustering(clusteringPlan, schema, instantTime);
    }
    LOG.info("Required conditions are currently satisfied, enabling the optimization of using binary stream copy ");

    JavaSparkContext engineContext = HoodieSparkEngineContext.getSparkContext(getEngineContext());
    TaskContextSupplier taskContextSupplier = getEngineContext().getTaskContextSupplier();
    SerializableSchema serializableSchema = new SerializableSchema(schema);
    boolean shouldPreserveMetadata = Option.ofNullable(clusteringPlan.getPreserveHoodieMetadata()).orElse(false);
    JavaRDD<ClusteringGroupInfo> groupInfoJavaRDD = engineContext.parallelize(clusteringGroupInfos, clusteringGroupInfos.size());
    LOG.info("number of partitions for clustering " + groupInfoJavaRDD.getNumPartitions());
    JavaRDD<WriteStatus> writeStatusRDD = groupInfoJavaRDD
        .mapPartitions(clusteringOps -> {
          Iterable<ClusteringGroupInfo> clusteringOpsIterable = () -> clusteringOps;
          return StreamSupport.stream(clusteringOpsIterable.spliterator(), false)
              .flatMap(clusteringOp ->
                  runClusteringForGroup(
                      clusteringOp,
                      clusteringPlan.getStrategy().getStrategyParams(),
                      shouldPreserveMetadata,
                      serializableSchema,
                      taskContextSupplier,
                      instantTime))
              .iterator();
        });

    HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata = new HoodieWriteMetadata<>();
    writeMetadata.setWriteStatuses(HoodieJavaRDD.of(writeStatusRDD));
    return writeMetadata;
  }

  /**
   * Submit job to execute clustering for the group.
   */
  private Stream<WriteStatus> runClusteringForGroup(ClusteringGroupInfo clusteringOps, Map<String, String> strategyParams,
                                                    boolean preserveHoodieMetadata, SerializableSchema schema,
                                                    TaskContextSupplier taskContextSupplier, String instantTime) {
    List<WriteStatus> statuses = new ArrayList<>();
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
    statuses.addAll(handler.close());
    return statuses.stream();
  }

  /**
   * 1. Check Table type
   * 2. Check bloom filter type code
   * 3. Check Array Type Schema consistency affected by hoodie.parquet.writelegacyformat.enabled and spark.hadoop.parquet.avro.write-old-list-structure
   * 4. Check Schema Optional or Required consistency for the same field
   */
  public boolean supportBinaryStreamCopy(List<ClusteringGroupInfo> inputGroups, Map<String, String> strategyParams) {
    if (getHoodieTable().getMetaClient().getTableType() != COPY_ON_WRITE) {
      LOG.warn("SparkBinaryCopyClusteringExecutionStrategy is only supported for COW tables. Will fall back to common clustering execution strategy.");
      return false;
    }
    Option<String[]> orderByColumnsOpt =
        Option.ofNullable(strategyParams.get(PLAN_STRATEGY_SORT_COLUMNS.key()))
            .map(listStr -> listStr.split(","));

    if (orderByColumnsOpt.isPresent()) {
      LOG.warn("SparkBinaryCopyClusteringExecutionStrategy does not support sort by columns. Will fall back to common clustering execution strategy.");
      return false;
    }

    if (!getHoodieTable().getMetaClient().getTableConfig().getBaseFileFormat().equals(PARQUET)) {
      LOG.warn("SparkBinaryCopyClusteringExecutionStrategy only supports parquet base files. Will fall back to common clustering execution strategy.");
      return false;
    }

    JavaSparkContext engineContext = HoodieSparkEngineContext.getSparkContext(getEngineContext());

    List<ParquetBinaryCopyChecker.ParquetFileInfo> fileStatus = engineContext.parallelize(inputGroups, inputGroups.size())
        .flatMap(group -> group.getOperations().iterator())
        .map(op -> {
          String filePath = op.getDataFilePath();
          return ParquetBinaryCopyChecker.collectFileInfo(getHoodieTable().getStorageConf().unwrapAs(Configuration.class), filePath);
        })
        .collect();
    return ParquetBinaryCopyChecker.verifyFiles(fileStatus);
  }
}
