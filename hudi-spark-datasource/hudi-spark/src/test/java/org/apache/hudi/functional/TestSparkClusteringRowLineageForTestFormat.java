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

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.clustering.run.strategy.SingleSparkJobExecutionStrategy;
import org.apache.hudi.client.clustering.run.strategy.SparkBinaryCopyClusteringExecutionStrategy;
import org.apache.hudi.client.functional.TestSparkCopyOnWriteRowLineageForTestFormat;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.ClusteringGroupInfo;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.table.HoodieSparkTable;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.testutils.RowLineageTestUtils.assertRowLineageChangesWithCommitSeqNo;
import static org.apache.hudi.common.testutils.RowLineageTestUtils.deriveRowLineage;
import static org.apache.hudi.common.testutils.RowLineageTestUtils.readStoredRowLineage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Functional tests for the row lineage columns of an external table format, see
 * {@link org.apache.hudi.common.HoodieTableFormat#getFormatMetaColumns()}, across clustering of a copy-on-write
 * table, which needs the Spark version adapter of the Spark datasource modules.
 */
@Tag("functional")
class TestSparkClusteringRowLineageForTestFormat extends TestSparkCopyOnWriteRowLineageForTestFormat {

  private static final String PARTITION = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
  private static final String ROW_WRITER_ENABLE = "hoodie.datasource.write.row.writer.enable";

  /**
   * Clustering carries the records of several file groups over unchanged into a new base file, so every record keeps
   * its row lineage, whether its base file stores it or not: with the row writer, and with Avro and Spark records,
   * also when the records are sorted along a spatial curve.
   */
  @ParameterizedTest
  @CsvSource({"true,AVRO,false", "false,AVRO,false", "false,SPARK,false", "true,AVRO,true", "false,AVRO,true", "false,SPARK,true"})
  void testClusteringKeepsRowLineage(boolean rowWriter, HoodieRecordType recordType, boolean zOrder) throws IOException {
    HoodieTableMetaClient metaClient = initTable();
    String tableBasePath = metaClient.getBasePath().toString();
    HoodieClusteringConfig.Builder clusteringConfig = HoodieClusteringConfig.newBuilder();
    if (zOrder) {
      clusteringConfig.withClusteringSortColumns("begin_lat,begin_lon")
          .withDataOptimizeStrategy(HoodieClusteringConfig.LayoutOptimizationStrategy.ZORDER.name());
    }
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withProperties(buildConfig(metaClient, recordType, 0, clusteringConfig.build()).getProps())
        .withProps(Collections.singletonMap(ROW_WRITER_ENABLE, String.valueOf(rowWriter))).build();
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION});
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      // Three file groups; the base file of the first one stores the row lineage of some records after an upsert
      List<HoodieRecord> inserts = new ArrayList<>();
      for (String instant : Arrays.asList("001", "002", "003")) {
        List<HoodieRecord> batch = dataGen.generateInsertsForPartition(instant, 5, PARTITION);
        write(client, toRecordType(batch, recordType));
        inserts.addAll(batch);
      }
      write(client, toRecordType(dataGen.generateUpdates("004", Arrays.asList(inserts.get(1), inserts.get(3))), recordType));
      List<HoodieBaseFile> baseFiles = getLatestBaseFiles(metaClient);
      assertEquals(3, baseFiles.size());
      Map<String, Pair<Long, Long>> rowLineageBefore = new HashMap<>();
      for (HoodieBaseFile baseFile : baseFiles) {
        rowLineageBefore.putAll(deriveRowLineage(storageConf(), tableBasePath, baseFile));
      }
      assertEquals(15, new HashSet<>(rowLineageBefore.values()).size());

      Option<String> clusteringInstant = client.scheduleClustering(Option.empty());
      assertTrue(clusteringInstant.isPresent());
      client.cluster(clusteringInstant.get(), true);
      HoodieBaseFile clusteredBaseFile = getOnlyLatestBaseFile(metaClient);
      assertEquals(rowLineageBefore, new HashMap<>(readStoredRowLineage(storageConf(), clusteredBaseFile)));
      for (HoodieBaseFile baseFile : baseFiles) {
        assertRowLineageChangesWithCommitSeqNo(storageConf(), tableBasePath, baseFile, clusteredBaseFile, 5, 0);
      }
    }
  }

  /**
   * Binary copy clustering copies the row groups of the base files, so it cannot add the row lineage columns.
   */
  @Test
  void testBinaryCopyClusteringIsRefused() throws IOException {
    HoodieTableMetaClient metaClient = initTable();
    HoodieWriteConfig config = buildConfig(metaClient, getRecordType(), 0, HoodieClusteringConfig.newBuilder()
        .withClusteringExecutionStrategyClass(SparkBinaryCopyClusteringExecutionStrategy.class.getName()).build());
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {PARTITION});
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      write(client, toRecordType(dataGen.generateInsertsForPartition("001", 10, PARTITION), getRecordType()));
      write(client, toRecordType(dataGen.generateInsertsForPartition("002", 10, PARTITION), getRecordType()));
      Option<String> clusteringInstant = client.scheduleClustering(Option.empty());
      assertTrue(clusteringInstant.isPresent());
      Throwable failure = assertThrows(Exception.class, () -> client.cluster(clusteringInstant.get(), true));
      assertTrue(exceptionChainContains(failure, refusalMessage(SparkBinaryCopyClusteringExecutionStrategy.class)), () -> "Unexpected failure: " + failure);
    }
  }

  /**
   * The strategies that run a single Spark job write the records they read with the schema of the table.
   */
  @Test
  void testSingleSparkJobClusteringIsRefused() throws IOException {
    HoodieTableMetaClient metaClient = initTable();
    HoodieWriteConfig config = buildConfig(metaClient, getRecordType(), 0, HoodieClusteringConfig.newBuilder().build());
    TestSingleSparkJobExecutionStrategy strategy = new TestSingleSparkJobExecutionStrategy(HoodieSparkTable.create(config, context(), metaClient), context(), config);
    Throwable failure = assertThrows(HoodieNotSupportedException.class,
        () -> strategy.performClustering(new HoodieClusteringPlan(), HoodieSchema.parse(config.getSchema()), "001"));
    assertTrue(failure.getMessage().contains(refusalMessage(TestSingleSparkJobExecutionStrategy.class)), () -> "Unexpected failure: " + failure);
  }

  private static class TestSingleSparkJobExecutionStrategy extends SingleSparkJobExecutionStrategy<Object> {
    TestSingleSparkJobExecutionStrategy(HoodieSparkTable<?> table, HoodieEngineContext context, HoodieWriteConfig config) {
      super(table, context, config);
    }

    @Override
    protected List<WriteStatus> performClusteringForGroup(ReaderContextFactory<Object> readerContextFactory, ClusteringGroupInfo clusteringGroup,
                                                          Map<String, String> strategyParams, boolean preserveHoodieMetadata, HoodieSchema schema,
                                                          TaskContextSupplier taskContextSupplier, String instantTime) {
      throw new UnsupportedOperationException();
    }
  }

  private static String refusalMessage(Class<?> strategyClass) {
    return "Table format test-format declares format meta columns, but " + strategyClass.getSimpleName()
        + " does not carry them over to the base files it rewrites";
  }

  private static boolean exceptionChainContains(Throwable throwable, String text) {
    for (Throwable current = throwable; current != null; current = current.getCause()) {
      if (current.getMessage() != null && current.getMessage().contains(text)) {
        return true;
      }
    }
    return false;
  }
}
