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

package org.apache.hudi.functional;

import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCancellationOfClustering extends HoodieClientTestBase {

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  @Test
  public void testEndToEndFuncWithEmptyReplaceCommit() throws Throwable {
    HoodieWriteConfig writeConfig = instantiateNewMetaclientAndGetWriteConfig();

    HoodieWriteConfig tableServiceConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withProps(writeConfig.getProps())
        .withTableServicesEnabled(true)
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withInlineClustering(true)
            .withInlineClusteringNumCommits(1)
            .build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(1, 3).build())
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withMaxCommitsBeforeCleaning(1)
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(1)
            .build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(true)
            .withInlineCompactionTriggerStrategy(CompactionTriggerStrategy.NUM_COMMITS)
            .withMaxNumDeltaCommitsBeforeCompaction(1)
            .build())
        .build();

    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(context, writeConfig);
         SparkRDDWriteClient tableServiceWriteClient = new SparkRDDWriteClient<>(context, tableServiceConfig)) {
      String instantTime = writeClient.startCommit();
      // First set of inserts
      List<HoodieRecord> inserts1 = dataGen.generateInserts(instantTime, 100);
      writeClient.insert(jsc.parallelize(inserts1, 1), instantTime);

      // Second set of inserts
      instantTime = writeClient.startCommit();
      List<HoodieRecord> inserts2 = dataGen.generateInserts(instantTime, 100);
      writeClient.insert(jsc.parallelize(inserts2, 1), instantTime);

      // Update the first 50 records
      instantTime = writeClient.startCommit();
      int rowCount = 50;
      List<HoodieRecord> updates1 = dataGen.generateUniqueUpdates(instantTime, 50);
      writeClient.upsert(jsc.parallelize(updates1, 1), instantTime);

      // Schedule a clustering operation but output an empty replace commit as the final result in the timeline to simulate an aborted operation
      String instant = (String) tableServiceWriteClient.scheduleClustering(Option.empty()).orElseThrow(() -> new RuntimeException("Failed to schedule clustering"));
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(jsc.hadoopConfiguration()).build();
      HoodieInstant clusteringInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, instant);
      metaClient.getActiveTimeline().transitionReplaceRequestedToInflight(clusteringInstant, Option.empty());
      metaClient.getActiveTimeline().transitionReplaceInflightToComplete(
          HoodieTimeline.getReplaceCommitInflightInstant(instant),
          Option.of(new HoodieReplaceCommitMetadata()));

      // Validate the empty replace commit does not impact the query
      List<Row> hudiRows = sqlContext.read().format("hudi").load(basePath).collectAsList();
      assertEquals(200, hudiRows.size());

      // write more updates
      instantTime = writeClient.startCommit();
      List<HoodieRecord> updates2 = dataGen.generateUniqueUpdates(instantTime, 50);
      writeClient.upsert(jsc.parallelize(updates2, 1), instantTime);

      instantTime = writeClient.startCommit();
      List<HoodieRecord> updates3 = dataGen.generateUniqueUpdates(instantTime, 50);
      writeClient.upsert(jsc.parallelize(updates3, 1), instantTime);

      // Now compact to force new file slice creation
      instantTime = (String) tableServiceWriteClient.scheduleCompaction(Option.empty()).orElseThrow(() -> new RuntimeException("Failed to schedule compaction"));
      tableServiceWriteClient.compact(instantTime);

      // Update and compact again to generate enough versions for cleaning
      instantTime = writeClient.startCommit();
      List<HoodieRecord> updates4 = dataGen.generateUniqueUpdates(instantTime, 100);
      writeClient.upsert(jsc.parallelize(updates4, 1), instantTime);

      instantTime = writeClient.startCommit();
      List<HoodieRecord> updates5 = dataGen.generateUniqueUpdates(instantTime, 100);
      writeClient.upsert(jsc.parallelize(updates5, 1), instantTime);

      instantTime = (String) tableServiceWriteClient.scheduleCompaction(Option.empty()).orElseThrow(() -> new RuntimeException("Failed to schedule compaction"));
      tableServiceWriteClient.compact(instantTime);

      // trigger clean and archive
      tableServiceWriteClient.clean();
      tableServiceWriteClient.archive();

      // Validate the clean and archive services ran
      metaClient.reloadActiveTimeline();
      assertEquals(1, metaClient.getActiveTimeline().getCleanerTimeline().countInstants());
      HoodieArchivedTimeline archivedTimeline = metaClient.getArchivedTimeline("000");
      archivedTimeline.loadCompletedInstantDetailsInMemory();
      // Empty replace commit should be archived
      assertEquals(1, archivedTimeline.getCompletedReplaceTimeline().countInstants());
      assertEquals(7, archivedTimeline.countInstants());
    }
    List<Row> hudiRows = sqlContext.read().format("hudi").load(basePath).collectAsList();
    assertEquals(200, hudiRows.size());
  }

  @Test
  public void testCancellationOfNonClusteringInstant() throws Throwable {
    HoodieWriteConfig writeConfig = instantiateNewMetaclientAndGetWriteConfig(false);

    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(context, writeConfig)) {
      String instantTime = writeClient.startCommit();
      // First set of inserts
      List<HoodieRecord> inserts1 = dataGen.generateInserts(instantTime, 100);
      writeClient.commit(instantTime, writeClient.insert(jsc.parallelize(inserts1, 1), instantTime));

      // insert_overwrite
      instantTime = writeClient.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION, metaClient);
      List<HoodieRecord> inserts2 = dataGen.generateInserts(instantTime, 100);
      HoodieWriteResult writeResult = writeClient.insertOverwrite(jsc.parallelize(inserts2, 1), instantTime);
      assertReplaceCommitPending(instantTime);

      // lets try to cancel the replace commit pertaining to non clustering instant.
      String finalInstantTime = instantTime;
      HoodieClusteringException exception = assertThrows(
          HoodieClusteringException.class,
          () -> writeClient.cancelAndNukeClusteringWithEmptyReplaceCommit(finalInstantTime),
          "Expects the cancellatin to throw a HoodieClusteringException");
      assertEquals(
          "Cannot cancel non clustering instant " + instantTime,
          exception.getMessage());
    }
  }

  @Test
  public void testCancellationOfClusteringInstantsUnhappyPaths() throws Throwable {
    HoodieWriteConfig writeConfig = instantiateNewMetaclientAndGetWriteConfig(false);

    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(context, writeConfig)) {
      String instantTime = writeClient.startCommit();
      // First set of inserts
      List<HoodieRecord> inserts1 = dataGen.generateInserts(instantTime, 100);
      writeClient.commit(instantTime, writeClient.insert(jsc.parallelize(inserts1, 1), instantTime));
    }

    HoodieWriteConfig clusteringEnabledWriteConfig = instantiateNewMetaclientAndGetWriteConfig(false, true, true);

    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(context, clusteringEnabledWriteConfig)) {
      // cluster
      Option<String> clusteringInstant = writeClient.scheduleClustering(Option.empty());
      writeClient.cluster(clusteringInstant.get(), true);
      assertReplaceCommitCompleted(clusteringInstant.get());

      // lets try to cancel the completed replace commit
      HoodieClusteringException exception1 = assertThrows(
          HoodieClusteringException.class,
          () -> writeClient.cancelAndNukeClusteringWithEmptyReplaceCommit(clusteringInstant.get()),
          "Expects the cancellatin to throw a HoodieClusteringException");
      assertEquals(
          "No matching pending clustering instants found for " + clusteringInstant.get(),
          exception1.getMessage());

      // lets also test cancellation of a ingestion commit which is inflight.
      String instantTime = writeClient.startCommit();
      List<HoodieRecord> inserts2 = dataGen.generateInserts(instantTime, 100);
      writeClient.insert(jsc.parallelize(inserts2, 1), instantTime);

      HoodieClusteringException exception2 = assertThrows(
          HoodieClusteringException.class,
          () -> writeClient.cancelAndNukeClusteringWithEmptyReplaceCommit(instantTime),
          "Expects the cancellatin to throw a HoodieClusteringException");
      assertEquals(
          "No matching pending clustering instants found for " + instantTime,
          exception2.getMessage());
    }
  }

  @Test
  public void testCancellationOfRequestedClusteringInstant() throws Throwable {
    HoodieWriteConfig writeConfig = instantiateNewMetaclientAndGetWriteConfig(false);

    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(context, writeConfig)) {
      String instantTime = writeClient.startCommit();
      // First set of inserts
      List<HoodieRecord> inserts1 = dataGen.generateInserts(instantTime, 100);
      writeClient.commit(instantTime, writeClient.insert(jsc.parallelize(inserts1, 1), instantTime));
    }

    HoodieWriteConfig clusteringEnabledWriteConfig = instantiateNewMetaclientAndGetWriteConfig(false, true, true);

    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(context, clusteringEnabledWriteConfig)) {
      // only schedule clustering.
      Option<String> clusteringInstant = writeClient.scheduleClustering(Option.empty());
      assertTrue(clusteringInstant.isPresent());

      // lets try to cancel
      assertTrue(writeClient.cancelAndNukeClusteringWithEmptyReplaceCommit(clusteringInstant.get()));
      assertReplaceCommitCompleted(clusteringInstant.get());
    }
  }

  private void assertReplaceCommitPending(String pendingReplaceCommit) {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertTrue(metaClient.getActiveTimeline().filterPendingReplaceTimeline().getInstants().stream().anyMatch(instant -> instant.getTimestamp().equals(pendingReplaceCommit)));
  }

  private void assertReplaceCommitCompleted(String replaceCommit) {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertTrue(metaClient.getActiveTimeline().filterCompletedInstants().filter(instant -> instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION))
        .getInstants().stream().anyMatch(instant -> instant.getTimestamp().equals(replaceCommit)));
  }

  private HoodieWriteConfig instantiateNewMetaclientAndGetWriteConfig() throws IOException {
    return instantiateNewMetaclientAndGetWriteConfig(true);
  }

  private HoodieWriteConfig instantiateNewMetaclientAndGetWriteConfig(boolean autoCommit) throws IOException {
    return instantiateNewMetaclientAndGetWriteConfig(true, autoCommit, false);
  }

  private HoodieWriteConfig instantiateNewMetaclientAndGetWriteConfig(boolean deleteAndRecreateTable, boolean autoCommit, boolean enableClustering) throws IOException {
    if (deleteAndRecreateTable) {
      // delete meta client and recreate it so that we can dictate the propeties for meta client instantiation
      metaClient.getFs().delete(new Path(basePath));

      Properties props = getPropertiesForKeyGen(true);
      initMetaClient(getTableType(), props);
    }

    HoodieWriteConfig.Builder builder = getConfigBuilder().withAutoCommit(autoCommit)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).withMaxNumDeltaCommitsBeforeCompaction(2).build());

    if (enableClustering) {
      return builder
          .withClusteringConfig(HoodieClusteringConfig.newBuilder().withInlineClustering(true).withInlineClusteringNumCommits(1).build())
          .build();
    } else {
      return builder.withTableServicesEnabled(false).build();
    }

  }
}
