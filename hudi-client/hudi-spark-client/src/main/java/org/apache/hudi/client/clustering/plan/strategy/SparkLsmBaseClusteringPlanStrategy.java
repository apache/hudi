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

package org.apache.hudi.client.clustering.plan.strategy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.lsm.HoodieLSMLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.strategy.LsmBaseClusteringPlanStrategy;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import scala.Tuple4;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SparkLsmBaseClusteringPlanStrategy<T,I,K,O> extends LsmBaseClusteringPlanStrategy<T,I,K,O> {
  private static final Logger LOG = LogManager.getLogger(SparkLsmBaseClusteringPlanStrategy.class);

  private transient HoodieSparkEngineContext hsc;
  private final SerializableConfiguration serializableConfiguration;

  public SparkLsmBaseClusteringPlanStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
    hsc = (HoodieSparkEngineContext) engineContext;
    Configuration configuration = hsc.getHadoopConf().get();
    serializableConfiguration = new SerializableConfiguration(configuration);
  }

  @Override
  public Option<HoodieClusteringPlan> generateClusteringPlan() {
    if (plan != null && plan.isPresent()) {
      return plan;
    }

    if (!checkPrecondition()) {
      return Option.empty();
    }

    HoodieTableMetaClient metaClient = getHoodieTable().getMetaClient();
    LOG.info("Scheduling clustering for " + metaClient.getBasePath());
    Pair<List<String>, Pair<Set<String>, Set<String>>> partitionsAndInstantsPair = getPartitionPathsToCluster();
    List<String> partitionPaths = partitionsAndInstantsPair.getLeft();
    Set<String> missingPartitions = new HashSet<>();

    if (partitionPaths.isEmpty()) {
      // In case no partitions could be picked, return no clustering plan
      return Option.empty();
    }

    // <Level1Existed, Partition path, All files in the same bucket>
    List<Tuple4<Boolean, String, String, List<HoodieLogFile>>> partitionToLogFiles = hsc.getJavaSparkContext().parallelize(partitionPaths, partitionPaths.size())
        .flatMap(partitionPath -> {
          Pair<List<FileSlice>, Set<String>> filesEligibleForClustering = getFilesEligibleForClustering(partitionPath);
          boolean level1Exists = !filesEligibleForClustering.getRight().isEmpty();
          return filesEligibleForClustering.getLeft().stream().map(slice -> {
            return new Tuple4<>(level1Exists, partitionPath, slice.getFileId(), slice.getLogFiles().collect(Collectors.toList()));
          }).iterator();
        }).collect();

    // <Missing partition, Clustering group>
    List<Pair<String, Stream<HoodieClusteringGroup>>> partitionClusteringGroupPair = hsc.map(
        partitionToLogFiles,
        tuple -> {
          // read footer when hoodie.clustering.lsm.readfooter.enabled is true
          List<HoodieLogFile> lsmLogFiles = getLSMLogFiles(getWriteConfig().isLsmReadFooterEnabled(), tuple._4());
          Set<String> level1InPendingClustering = getLevel1InPendingClustering(tuple._2());
          Pair<Boolean, Stream<HoodieClusteringGroup>> pair = buildClusteringGroups(lsmLogFiles, tuple._1(), level1InPendingClustering.contains(tuple._3()), tuple._2(), skipOP);
          if (pair.getLeft()) {
            return Pair.of(tuple._2(), pair.getRight());
          }
          return Pair.of("", pair.getRight());
        }, Math.max(1, Math.min(partitionToLogFiles.size(), getWriteConfig().getLsmReadFooterParallelism())));

    List<HoodieClusteringGroup> clusteringGroups = partitionClusteringGroupPair.stream().flatMap(pair -> {
      String missingPartition = pair.getLeft();
      if (!StringUtils.isNullOrEmpty(missingPartition)) {
        // missingPartition value is not empty, which means it related candidate fileSlices all not all processed.
        // so that we need to mark this kind of partition as missing partition.
        missingPartitions.add(missingPartition);
      }
      return pair.getRight();
    }).collect(Collectors.toList());

    if (clusteringGroups.isEmpty()) {
      LOG.info("No data available to cluster");
      return Option.empty();
    }

    return buildClusteringPlan(metaClient,
        clusteringGroups,
        getWriteConfig(),
        partitionsAndInstantsPair.getRight().getLeft(),
        partitionsAndInstantsPair.getRight().getRight(),
        missingPartitions,
        getStrategyParams(),
        getExtraMetadata(),
        getPlanVersion());
  }

  private List<HoodieLogFile> getLSMLogFiles(boolean readFooterEnabled, List<HoodieLogFile> logFileList) {
    if (readFooterEnabled) {
      return logFileList.stream().map(logFile -> {
        HoodieLSMLogFile lsmLogFile = (HoodieLSMLogFile) logFile;
        String[] minMax = PARQUET_UTILS.readMinMaxRecordKeys(serializableConfiguration.get(), lsmLogFile.getPath());
        lsmLogFile.setMin(minMax[0]);
        lsmLogFile.setMax(minMax[1]);
        return lsmLogFile;
      }).collect(Collectors.toList());
    }
    return logFileList;
  }

}