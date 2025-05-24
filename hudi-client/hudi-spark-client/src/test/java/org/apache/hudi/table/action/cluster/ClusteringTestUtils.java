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

package org.apache.hudi.table.action.cluster;

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.validator.SqlQueryEqualityPreCommitValidator;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.CompactionTestUtils;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodiePreCommitValidatorConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static org.apache.hudi.common.testutils.FileCreateUtilsLegacy.baseFileName;
import static org.apache.hudi.common.testutils.FileCreateUtilsLegacy.createBaseFile;
import static org.apache.hudi.common.testutils.HoodieTestUtils.DEFAULT_PARTITION_PATHS;
import static org.apache.hudi.utils.HoodieWriterClientTestHarness.getPropertiesForKeyGen;

public class ClusteringTestUtils {
  protected static int timelineServicePort =
      FileSystemViewStorageConfig.REMOTE_PORT_NUM.defaultValue();
  private static final String COUNT_SQL_QUERY_FOR_VALIDATION = "select count(*) from <TABLE_NAME>";

  public static HoodieWriteConfig getClusteringConfig(String basePath) {
    return getClusteringConfig(basePath, HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);
  }

  public static HoodieWriteConfig getClusteringConfig(String basePath, String schemaStr) {
    return getClusteringConfig(basePath, schemaStr, new Properties());
  }

  public static HoodieWriteConfig getClusteringConfig(String basePath, String schemaStr, Properties properties) {

    // Default configs.
    HoodieFailedWritesCleaningPolicy cleaningPolicy = HoodieFailedWritesCleaningPolicy.EAGER;
    HoodieIndex.IndexType indexType = HoodieIndex.IndexType.BLOOM;
    boolean populateMetaFields = true;

    // Clustering config
    HoodieClusteringConfig clusteringConfig = HoodieClusteringConfig.newBuilder().withClusteringMaxNumGroups(10)
        .withClusteringSortColumns(populateMetaFields ? "_hoodie_record_key" : "_row_key")
        .withClusteringTargetPartitions(0).withInlineClusteringNumCommits(0).withInlineClustering(false)
        .build();
    properties.putAll(getPropertiesForKeyGen());

    // write config builder
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(schemaStr)
        .withParallelism(2, 2).withBulkInsertParallelism(2).withFinalizeWriteParallelism(2).withDeleteParallelism(2)
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .withWriteStatusClass(MetadataMergeWriteStatus.class)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .compactionSmallFileSize(1024 * 1024).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder().withFailedWritesCleaningPolicy(cleaningPolicy).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024).parquetMaxFileSize(1024 * 1024).orcMaxFileSize(1024 * 1024).build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType).build())
        .withEmbeddedTimelineServerEnabled(true).withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withEnableBackupForRemoteFileSystemView(false) // Fail test if problem connecting to timeline-server
            .withRemoteServerPort(timelineServicePort)
            .build())
        .withClusteringConfig(clusteringConfig)
        .withPreCommitValidatorConfig(HoodiePreCommitValidatorConfig.newBuilder()
            .withPreCommitValidator(SqlQueryEqualityPreCommitValidator.class.getName())
            .withPrecommitValidatorEqualitySqlQueries(COUNT_SQL_QUERY_FOR_VALIDATION)
            .build())
        .withProps(properties)
        .build();
  }

  public static HoodieClusteringPlan getClusteringPlan(HoodieTableMetaClient client, HoodieInstant clusteringInstant) {
    return ClusteringUtils.getClusteringPlan(client, clusteringInstant).get().getRight();
  }

  public static HoodieClusteringPlan getClusteringPlan(HoodieTableMetaClient client, String clusteringInstantString) {
    HoodieInstant clusteringInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLUSTERING_ACTION,
        clusteringInstantString, InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
    return getClusteringPlan(client, clusteringInstant);
  }

  public static String runClustering(SparkRDDWriteClient<?> clusteringClient, boolean skipExecution, boolean shouldCommit) {
    String clusteringCommitTime = clusteringClient.scheduleClustering(Option.empty()).get();
    if (!skipExecution) {
      clusteringClient.cluster(clusteringCommitTime, shouldCommit);
    }
    return clusteringCommitTime;
  }

  public static HoodieClusteringPlan createClusteringPlan(HoodieTableMetaClient metaClient, String instantTime, String fileId) {
    try {
      String basePath = metaClient.getBasePath().toString();
      String partition = DEFAULT_PARTITION_PATHS[0];
      createBaseFile(basePath, partition, instantTime, fileId, 1);
      FileSlice slice = new FileSlice(partition, instantTime, fileId);
      slice.setBaseFile(new CompactionTestUtils.DummyHoodieBaseFile(Paths.get(basePath, partition,
          baseFileName(instantTime, fileId)).toString()));
      List<FileSlice>[] fileSliceGroups = new List[] {Collections.singletonList(slice)};
      HoodieClusteringPlan clusteringPlan = ClusteringUtils.createClusteringPlan("strategy", new HashMap<>(),
          fileSliceGroups, Collections.emptyMap());
      return clusteringPlan;
    } catch (Exception e) {
      throw new HoodieException(e.getMessage(), e);
    }
  }
}
