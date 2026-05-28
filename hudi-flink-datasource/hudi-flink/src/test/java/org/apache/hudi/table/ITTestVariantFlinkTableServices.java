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

package org.apache.hudi.table;

import org.apache.hudi.adapter.DataTypeAdapter;
import org.apache.hudi.adapter.DataTypeAdapterTestUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.FlinkMiniCluster;
import org.apache.hudi.utils.TestTableEnvs;
import org.apache.hudi.utils.TestUtils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Flink-only integration tests for VARIANT columns through commit-triggered MOR compaction and
 * COW clustering. Writes block on {@code TableResult#await()}; table services are triggered by
 * {@code compaction.delta_commits} / {@code clustering.delta_commits} and tests poll the timeline
 * via {@link TestUtils#waitUntil} instead of sleeping.
 */
@ExtendWith(FlinkMiniCluster.class)
public class ITTestVariantFlinkTableServices {

  private static final int ASYNC_SERVICE_TIMEOUT_SECONDS = 60;

  private static boolean nativeVariantAvailable;

  @TempDir
  Path tempDir;

  @BeforeAll
  static void checkVariantSupport() {
    try {
      DataTypeAdapter.createVariantType();
      nativeVariantAvailable = true;
    } catch (UnsupportedOperationException e) {
      nativeVariantAvailable = false;
    }
  }

  private static void assumeVariantWriteSupport() {
    Assumptions.assumeTrue(nativeVariantAvailable, "VARIANT requires Flink 2.1+");
    Assumptions.assumeTrue(
        variantParquetAnnotationAvailable(),
        "VARIANT Parquet requires parquet-java 1.16.0+ and Hadoop 3.3+ "
            + "(CI: test-flink-variant job; local: -Dparquet.version=1.16.0 "
            + "-Dflink.format.parquet.version=1.16.0 -Dhadoop.version=3.3.0)");
  }

  private static boolean variantParquetAnnotationAvailable() {
    try {
      return DataTypeAdapter.variantParquetAnnotation().isPresent();
    } catch (UnsupportedOperationException e) {
      return false;
    }
  }

  @Test
  public void testFlinkMorCompactionPreservesVariant() throws Exception {
    assumeVariantWriteSupport();

    String tablePath = tempDir.resolve("variant_mor_compact").toString();
    TableEnvironment tEnv = TestTableEnvs.getBatchTableEnv();
    tEnv.getConfig()
        .getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);

    // 4 delta commits (3 inserts + 1 upsert) then async compaction on the 4th commit.
    tEnv.executeSql(createVariantMorTableDdl(tablePath, 4));

    tEnv.executeSql(
            "INSERT INTO variant_table VALUES "
                + "(1, CAST('r1' AS STRING), PARSE_JSON('{\"k\":1}'), CAST(1000 AS BIGINT))")
        .await();
    tEnv.executeSql(
            "INSERT INTO variant_table VALUES "
                + "(2, CAST('r2' AS STRING), PARSE_JSON('{\"k\":2}'), CAST(1000 AS BIGINT))")
        .await();
    tEnv.executeSql(
            "INSERT INTO variant_table VALUES "
                + "(3, CAST('r3' AS STRING), PARSE_JSON('{\"k\":3}'), CAST(1000 AS BIGINT))")
        .await();
    tEnv.executeSql(
            "INSERT INTO variant_table VALUES "
                + "(1, CAST('r1' AS STRING), PARSE_JSON('{\"k\":1,\"merged\":true}'), CAST(2000 AS BIGINT))")
        .await();

    assertTrue(
        TestUtils.waitUntil(
            () -> hasCompletedCompaction(tablePath) && !hasPendingCompaction(tablePath),
            ASYNC_SERVICE_TIMEOUT_SECONDS),
        "Commit-triggered compaction should complete after 4 delta commits");

    List<Row> rows =
        CollectionUtil.iteratorToList(
            tEnv.executeSql("SELECT id, name, v, ts FROM variant_table ORDER BY id").collect());
    assertEquals(3, rows.size(), "Compaction should preserve row count");

    assertRowVariantJson(tEnv, rows.get(0), 1, "r1", 2000L, "merged");
    assertRowVariantJson(tEnv, rows.get(1), 2, "r2", 1000L, "\"k\":2");
    assertRowVariantJson(tEnv, rows.get(2), 3, "r3", 1000L, "\"k\":3");

    tEnv.executeSql("DROP TABLE variant_table");
  }

  @Test
  public void testFlinkCowClusteringPreservesVariant() throws Exception {
    assumeVariantWriteSupport();

    String tablePath = tempDir.resolve("variant_cow_cluster").toString();
    TableEnvironment tEnv = TestTableEnvs.getBatchTableEnv();
    tEnv.getConfig()
        .getConfiguration()
        .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);

    tEnv.executeSql(createVariantCowPartitionedTableDdl(tablePath, 2));

    tEnv.executeSql(
            "INSERT INTO variant_table VALUES "
                + "(1, CAST('n1' AS STRING), PARSE_JSON('{\"part\":\"par1\"}'), CAST(1000 AS BIGINT), "
                + "CAST('par1' AS STRING)), "
                + "(2, CAST('n2' AS STRING), PARSE_JSON('{\"part\":\"par2\"}'), CAST(2000 AS BIGINT), "
                + "CAST('par2' AS STRING))")
        .await();

    List<Row> afterFirstCommit =
        CollectionUtil.iteratorToList(
            tEnv.executeSql("SELECT id, v FROM variant_table ORDER BY id").collect());
    byte[][] metadataBefore = new byte[4][];
    byte[][] valueBefore = new byte[4][];
    for (int i = 0; i < 2; i++) {
      DataTypeAdapterTestUtils.assertAsBinaryVariant(afterFirstCommit.get(i).getField(1));
      metadataBefore[i] = DataTypeAdapter.getVariantMetadata(afterFirstCommit.get(i).getField(1));
      valueBefore[i] = DataTypeAdapter.getVariantValue(afterFirstCommit.get(i).getField(1));
    }

    tEnv.executeSql(
            "INSERT INTO variant_table VALUES "
                + "(3, CAST('n3' AS STRING), PARSE_JSON('{\"part\":\"par3\"}'), CAST(3000 AS BIGINT), "
                + "CAST('par3' AS STRING)), "
                + "(4, CAST('n4' AS STRING), PARSE_JSON('{\"part\":\"par4\"}'), CAST(4000 AS BIGINT), "
                + "CAST('par4' AS STRING))")
        .await();

    List<Row> afterSecondCommit =
        CollectionUtil.iteratorToList(
            tEnv.executeSql("SELECT id, v FROM variant_table WHERE id IN (3, 4) ORDER BY id").collect());
    for (int i = 0; i < 2; i++) {
      DataTypeAdapterTestUtils.assertAsBinaryVariant(afterSecondCommit.get(i).getField(1));
      metadataBefore[i + 2] = DataTypeAdapter.getVariantMetadata(afterSecondCommit.get(i).getField(1));
      valueBefore[i + 2] = DataTypeAdapter.getVariantValue(afterSecondCommit.get(i).getField(1));
    }

    assertTrue(
        TestUtils.waitUntil(
            () -> hasCompletedClustering(tablePath) && !hasPendingClustering(tablePath),
            ASYNC_SERVICE_TIMEOUT_SECONDS),
        "Commit-triggered clustering should complete after 2 insert commits");

    List<Row> afterCluster =
        CollectionUtil.iteratorToList(
            tEnv.executeSql("SELECT id, name, v, ts, `partition` FROM variant_table ORDER BY id")
                .collect());
    assertEquals(4, afterCluster.size(), "Clustering should preserve row count");

    for (int i = 0; i < 4; i++) {
      Row row = afterCluster.get(i);
      assertEquals(i + 1, row.getField(0));
      assertEquals("n" + (i + 1), row.getField(1));
      assertEquals(1000L * (i + 1), row.getField(3));
      assertEquals("par" + (i + 1), row.getField(4));
      DataTypeAdapterTestUtils.assertAsBinaryVariant(row.getField(2));
      assertArrayEquals(metadataBefore[i], DataTypeAdapter.getVariantMetadata(row.getField(2)));
      assertArrayEquals(valueBefore[i], DataTypeAdapter.getVariantValue(row.getField(2)));
    }

    tEnv.executeSql("DROP TABLE variant_table");
  }

  private static String createVariantMorTableDdl(String tablePath, int compactionDeltaCommits) {
    return String.format(
        "CREATE TABLE variant_table ("
            + "  id INT,"
            + "  name STRING,"
            + "  v VARIANT,"
            + "  ts BIGINT,"
            + "  PRIMARY KEY (id) NOT ENFORCED"
            + ") WITH ("
            + "  'connector' = 'hudi',"
            + "  'path' = '%s',"
            + "  'table.type' = 'MERGE_ON_READ',"
            + "  '%s' = 'true',"
            + "  '%s' = 'true',"
            + "  '%s' = '%d',"
            + "  '%s' = '1',"
            + "  '%s' = 'false'"
            + ")",
        tablePath.replace("'", "''"),
        FlinkOptions.COMPACTION_SCHEDULE_ENABLED.key(),
        FlinkOptions.COMPACTION_ASYNC_ENABLED.key(),
        FlinkOptions.COMPACTION_DELTA_COMMITS.key(),
        compactionDeltaCommits,
        FlinkOptions.COMPACTION_TASKS.key(),
        FlinkOptions.METADATA_ENABLED.key());
  }

  private static String createVariantCowPartitionedTableDdl(String tablePath, int clusteringDeltaCommits) {
    return String.format(
        "CREATE TABLE variant_table ("
            + "  id INT,"
            + "  name STRING,"
            + "  v VARIANT,"
            + "  ts BIGINT,"
            + "  `partition` STRING,"
            + "  PRIMARY KEY (id) NOT ENFORCED"
            + ") WITH ("
            + "  'connector' = 'hudi',"
            + "  'path' = '%s',"
            + "  'table.type' = 'COPY_ON_WRITE',"
            + "  '%s' = '%s',"
            + "  '%s' = 'partition',"
            + "  '%s' = 'true',"
            + "  '%s' = 'true',"
            + "  '%s' = '%d',"
            + "  '%s' = '1',"
            + "  '%s' = 'false'"
            + ")",
        tablePath.replace("'", "''"),
        FlinkOptions.OPERATION.key(),
        WriteOperationType.INSERT.value(),
        FlinkOptions.PARTITION_PATH_FIELD.key(),
        FlinkOptions.CLUSTERING_SCHEDULE_ENABLED.key(),
        FlinkOptions.CLUSTERING_ASYNC_ENABLED.key(),
        FlinkOptions.CLUSTERING_DELTA_COMMITS.key(),
        clusteringDeltaCommits,
        FlinkOptions.CLUSTERING_TASKS.key(),
        FlinkOptions.METADATA_ENABLED.key());
  }

  private static void assertRowVariantJson(
      TableEnvironment tEnv,
      Row row,
      int expectedId,
      String expectedName,
      long expectedTs,
      String jsonFragment)
      throws Exception {
    assertEquals(expectedId, row.getField(0));
    assertEquals(expectedName, row.getField(1));
    assertEquals(expectedTs, row.getField(3));
    DataTypeAdapterTestUtils.assertAsBinaryVariant(row.getField(2));

    String json =
        CollectionUtil.iteratorToList(
                tEnv.executeSql(
                        "SELECT CAST(v AS STRING) FROM variant_table WHERE id = " + expectedId)
                    .collect())
            .get(0)
            .getField(0)
            .toString();
    assertTrue(
        json.contains(jsonFragment),
        "Expected VARIANT json to contain " + jsonFragment + "; got: " + json);
  }

  private static HoodieTableMetaClient metaClient(String tablePath) {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.PATH, tablePath);
    return StreamerUtil.createMetaClient(conf);
  }

  private static boolean hasCompletedCompaction(String tablePath) {
    try {
      HoodieTableMetaClient metaClient = metaClient(tablePath);
      metaClient.reloadActiveTimeline();
      for (HoodieInstant instant :
          metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().getInstants()) {
        HoodieCommitMetadata metadata =
            TimelineUtils.getCommitMetadata(instant, metaClient.getActiveTimeline());
        if (metadata.getOperationType() == WriteOperationType.COMPACT) {
          return true;
        }
      }
      return false;
    } catch (Exception e) {
      return false;
    }
  }

  private static boolean hasPendingCompaction(String tablePath) {
    HoodieTableMetaClient metaClient = metaClient(tablePath);
    metaClient.reloadActiveTimeline();
    return metaClient.getActiveTimeline().filterPendingCompactionTimeline().countInstants() > 0;
  }

  private static boolean hasCompletedClustering(String tablePath) {
    HoodieTableMetaClient metaClient = metaClient(tablePath);
    metaClient.reloadActiveTimeline();
    return metaClient.getActiveTimeline().getCompletedReplaceTimeline().countInstants() > 0;
  }

  private static boolean hasPendingClustering(String tablePath) {
    HoodieTableMetaClient metaClient = metaClient(tablePath);
    metaClient.reloadActiveTimeline();
    return metaClient.getActiveTimeline().filterPendingClusteringTimeline().countInstants() > 0;
  }
}
