/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.action.cluster;

import org.apache.hudi.SparkHoodieRDDData;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieData;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.cluster.strategy.ClusteringExecutionStrategy;
import org.apache.hudi.table.action.commit.BaseClusteringHelper;
import org.apache.hudi.table.action.commit.BaseCommitHelper;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaRDD;

import java.util.Map;

import static org.apache.hudi.table.action.commit.SparkCommitHelper.getRdd;

public class SparkClusteringHelper<T extends HoodieRecordPayload<T>> extends BaseClusteringHelper<T> {
  private SparkClusteringHelper() {
  }

  private static class ClusteringHelperHolder {
    private static final SparkClusteringHelper SPARK_CLUSTERING_HELPER = new SparkClusteringHelper();
  }

  public static SparkClusteringHelper newInstance() {
    return ClusteringHelperHolder.SPARK_CLUSTERING_HELPER;
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute(
      HoodieEngineContext context, String instantTime, HoodieTable table, HoodieWriteConfig config,
      Option<Map<String, String>> extraMetadata,
      BaseCommitHelper<T> commitHelper) {
    SparkClusteringCommitHelper sparkClusteringCommitHelper = (SparkClusteringCommitHelper) commitHelper;
    HoodieInstant instant = HoodieTimeline.getReplaceCommitRequestedInstant(instantTime);
    // Mark instant as clustering inflight
    table.getActiveTimeline().transitionReplaceRequestedToInflight(instant, Option.empty());
    table.getMetaClient().reloadActiveTimeline();

    final Schema schema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(config.getSchema()));
    HoodieWriteMetadata<JavaRDD<WriteStatus>> writeMetaDataRdd = ((ClusteringExecutionStrategy<T, JavaRDD<HoodieRecord<? extends HoodieRecordPayload>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>>)
        ReflectionUtils.loadClass(config.getClusteringExecutionStrategyClass(),
            new Class<?>[] {HoodieTable.class, HoodieEngineContext.class, HoodieWriteConfig.class}, table, context, config))
        .performClustering(sparkClusteringCommitHelper.getClusteringPlan(), schema, instantTime);
    HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata = writeMetaDataRdd.clone(SparkHoodieRDDData.of(writeMetaDataRdd.getWriteStatuses()));
    JavaRDD<WriteStatus> writeStatusRDD = writeMetaDataRdd.getWriteStatuses();
    HoodieData<WriteStatus> statuses = sparkClusteringCommitHelper.updateIndex(writeStatusRDD, writeMetadata);
    writeMetadata.setWriteStats(getRdd(statuses).map(WriteStatus::getStat).collect());
    writeMetadata.setPartitionToReplaceFileIds(sparkClusteringCommitHelper.getPartitionToReplacedFileIds(writeMetadata));
    validateWriteResult(writeMetadata, instantTime, sparkClusteringCommitHelper.getClusteringPlan());
    sparkClusteringCommitHelper.commitOnAutoCommit(writeMetadata);
    if (!writeMetadata.getCommitMetadata().isPresent()) {
      HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(writeMetadata.getWriteStats().get(), writeMetadata.getPartitionToReplaceFileIds(),
          extraMetadata, WriteOperationType.CLUSTER, sparkClusteringCommitHelper.getSchemaToStoreInCommit(), sparkClusteringCommitHelper.getCommitActionType());
      writeMetadata.setCommitMetadata(Option.of(commitMetadata));
    }
    return writeMetadata;
  }

  /**
   * Validate actions taken by clustering. In the first implementation, we validate at least one new file is written.
   * But we can extend this to add more validation. E.g. number of records read = number of records written etc.
   * We can also make these validations in BaseCommitActionExecutor to reuse pre-commit hooks for multiple actions.
   */
  private void validateWriteResult(
      HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata, String instantTime,
      HoodieClusteringPlan clusteringPlan) {
    if (writeMetadata.getWriteStatuses().isEmpty()) {
      throw new HoodieClusteringException("Clustering plan produced 0 WriteStatus for " + instantTime
          + " #groups: " + clusteringPlan.getInputGroups().size() + " expected at least "
          + clusteringPlan.getInputGroups().stream().mapToInt(HoodieClusteringGroup::getNumOutputFileGroups).sum()
          + " write statuses");
    }
  }
}
