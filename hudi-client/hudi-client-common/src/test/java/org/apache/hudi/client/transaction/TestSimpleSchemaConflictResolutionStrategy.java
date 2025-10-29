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

package org.apache.hudi.client.transaction;

import org.apache.hudi.HoodieTestCommitGenerator;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieClusteringStrategy;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieSliceInfo;
import org.apache.hudi.client.utils.TransactionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieSchemaEvolutionConflictException;
import org.apache.hudi.table.TestBaseHoodieTable;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLUSTERING_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.testutils.HoodieCommonTestHarness.incTimestampStrByOne;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.apache.hudi.common.util.CommitUtils.buildMetadata;
import static org.apache.hudi.config.HoodieWriteConfig.ENABLE_SCHEMA_CONFLICT_RESOLUTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestSimpleSchemaConflictResolutionStrategy {

  @Mock
  public static FileSystemViewManager viewManager;
  @Mock
  public static HoodieEngineContext engineContext;
  @Mock
  public static TaskContextSupplier taskContextSupplier;
  public Option<HoodieInstant> lastCompletedTxnOwnerInstant;
  public Option<HoodieInstant> tableCompactionOwnerInstant;
  public Option<HoodieInstant> tableClusteringOwnerInstant;
  public Option<HoodieInstant> tableReplacementOwnerInstant;
  public Option<HoodieInstant> nonTableCompactionInstant;
  @TempDir
  private java.nio.file.Path basePath;
  @Mock
  private HoodieWriteConfig config;
  private HoodieTableMetaClient metaClient;
  private HoodieTestTable dummyInstantGenerator;
  private TestBaseHoodieTable table;
  private SimpleSchemaConflictResolutionStrategy strategy;

  private static final String SCHEMA1 = "{\"type\":\"record\",\"name\":\"MyRecord\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}]}";
  private static final String SCHEMA2 = "{\"type\":\"record\",\"name\":\"MyRecord\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"},{\"name\":\"field2\",\"type\":\"int\"}]}";
  private static final String SCHEMA3 = "{\"type\":\"record\",\"name\":\"MyRecord\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"},{\"name\":\"field3\",\"type\":\"boolean\"}]}";
  private static final String NULL_SCHEMA = "{\"type\":\"null\"}";

  private void setupInstants(String tableSchemaAtTxnStart, String tableSchemaAtTxnValidation,
                             String writerSchemaOfTxn, Boolean enableResolution, boolean setupLegacyClustering) throws Exception {
    metaClient = HoodieTestUtils.getMetaClientBuilder(HoodieTableType.COPY_ON_WRITE, new Properties(), "")
        .initTable(getDefaultStorageConf(), basePath.toString());
    dummyInstantGenerator = HoodieTestTable.of(metaClient);

    lastCompletedTxnOwnerInstant = Option.of(metaClient.createNewInstant(HoodieInstant.State.COMPLETED, COMMIT_ACTION, "0010", incTimestampStrByOne("0010")));
    tableCompactionOwnerInstant = Option.of(metaClient.createNewInstant(HoodieInstant.State.INFLIGHT, COMPACTION_ACTION, "0030", incTimestampStrByOne("0030")));
    tableClusteringOwnerInstant = Option.of(metaClient.createNewInstant(HoodieInstant.State.INFLIGHT, CLUSTERING_ACTION, "0030", incTimestampStrByOne("0030")));
    tableReplacementOwnerInstant = Option.of(metaClient.createNewInstant(HoodieInstant.State.INFLIGHT, REPLACE_COMMIT_ACTION, "0030", incTimestampStrByOne("0030")));
    nonTableCompactionInstant = Option.of(metaClient.createNewInstant(HoodieInstant.State.INFLIGHT, COMMIT_ACTION, "0040", incTimestampStrByOne("0040")));

    dummyInstantGenerator.addCommit("0010", Option.of(incTimestampStrByOne("0010")), Option.of(buildMetadata(
        Collections.emptyList(),
        Collections.emptyMap(),
        Option.empty(),
        WriteOperationType.UNKNOWN,
        tableSchemaAtTxnStart,
        COMMIT_ACTION)));
    dummyInstantGenerator.addCommit("0020", Option.of(incTimestampStrByOne("0020")), Option.of(buildMetadata(
        Collections.emptyList(),
        Collections.emptyMap(),
        Option.empty(),
        WriteOperationType.UNKNOWN,
        tableSchemaAtTxnValidation,
        COMMIT_ACTION)));

    if (setupLegacyClustering) {
      Pair<HoodieRequestedReplaceMetadata, HoodieReplaceCommitMetadata> result = getGetDummyClusteringMetadata();
      dummyInstantGenerator.addReplaceCommit(tableReplacementOwnerInstant.get().requestedTime(), Option.of(result.getLeft()), Option.empty(), result.getRight());
    }
    // Setup config
    TypedProperties typedProperties = new TypedProperties();
    typedProperties.setProperty(ENABLE_SCHEMA_CONFLICT_RESOLUTION.key(),
        enableResolution ? ENABLE_SCHEMA_CONFLICT_RESOLUTION.defaultValue().toString() : "false");
    config = HoodieWriteConfig.newBuilder().withSchema(writerSchemaOfTxn).withPath(basePath.toString()).withProperties(typedProperties).build();

    table = new TestBaseHoodieTable(config, engineContext, viewManager, metaClient, taskContextSupplier);
    strategy = new SimpleSchemaConflictResolutionStrategy();
  }

  @Test
  void testNoConflictFirstCommit() throws Exception {
    setupInstants(null, null, SCHEMA1, true, false);
    Schema result = strategy.resolveConcurrentSchemaEvolution(
        table, config, Option.empty(), nonTableCompactionInstant).get();
    assertEquals(new Schema.Parser().parse(SCHEMA1), result);
  }

  @Test
  void testNullWriterSchema() throws Exception {
    setupInstants(SCHEMA1, SCHEMA1, "", true, false);
    assertFalse(strategy.resolveConcurrentSchemaEvolution(
        table, config, lastCompletedTxnOwnerInstant, nonTableCompactionInstant).isPresent());
  }

  @Test
  void testNullTypeWriterSchema() throws Exception {
    setupInstants(SCHEMA1, SCHEMA1, NULL_SCHEMA, true, false);
    Schema result = strategy.resolveConcurrentSchemaEvolution(
        table, config, lastCompletedTxnOwnerInstant, nonTableCompactionInstant).get();
    assertEquals(new Schema.Parser().parse(SCHEMA1), result);
  }

  @Test
  void testConflictSecondCommitDifferentSchema() throws Exception {
    setupInstants(null, SCHEMA1, SCHEMA2, true, false);
    assertThrows(HoodieSchemaEvolutionConflictException.class,
        () -> strategy.resolveConcurrentSchemaEvolution(table, config, Option.empty(), nonTableCompactionInstant));
  }

  @Test
  void testConflictSecondCommitSameSchema() throws Exception {
    setupInstants(null, SCHEMA1, SCHEMA1, true, false);
    Schema result = strategy.resolveConcurrentSchemaEvolution(
        table, config, Option.empty(), nonTableCompactionInstant).get();
    assertEquals(new Schema.Parser().parse(SCHEMA1), result);
  }

  @Test
  void testNoConflictSameSchema() throws Exception {
    setupInstants(SCHEMA1, SCHEMA1, SCHEMA1, true, false);
    Schema result = strategy.resolveConcurrentSchemaEvolution(
        table, config, lastCompletedTxnOwnerInstant, nonTableCompactionInstant).get();
    assertEquals(new Schema.Parser().parse(SCHEMA1), result);
  }

  @Test
  void testNoConflictBackwardsCompatible1() throws Exception {
    setupInstants(SCHEMA1, SCHEMA2, SCHEMA1, true, false);
    Schema result = strategy.resolveConcurrentSchemaEvolution(
        table, config, lastCompletedTxnOwnerInstant, nonTableCompactionInstant).get();
    assertEquals(new Schema.Parser().parse(SCHEMA2), result);
  }

  @Test
  void testNoConflictBackwardsCompatible2() throws Exception {
    setupInstants(SCHEMA1, SCHEMA1, SCHEMA2, true, false);
    Schema result = strategy.resolveConcurrentSchemaEvolution(
        table, config, lastCompletedTxnOwnerInstant, nonTableCompactionInstant).get();
    assertEquals(new Schema.Parser().parse(SCHEMA2), result);
  }

  @Test
  void testNoConflictConcurrentEvolutionSameSchema() throws Exception {
    setupInstants(SCHEMA1, SCHEMA2, SCHEMA2, true, false);
    Schema result = strategy.resolveConcurrentSchemaEvolution(
        table, config, lastCompletedTxnOwnerInstant, nonTableCompactionInstant).get();
    assertEquals(new Schema.Parser().parse(SCHEMA2), result);
  }

  @Test
  void testCompactionTableServiceSkipSchemaEvolutionCheck() throws Exception {
    setupInstants(SCHEMA1, SCHEMA2, SCHEMA3, true, false);
    boolean hasSchema = strategy.resolveConcurrentSchemaEvolution(
        table, config, lastCompletedTxnOwnerInstant, tableCompactionOwnerInstant).isPresent();
    assertFalse(hasSchema);
  }

  @Test
  void testClusteringInstantSkipsSchemaCheck() throws Exception {
    setupInstants(SCHEMA1, SCHEMA2, SCHEMA3, true, false);
    boolean hasSchema = strategy.resolveConcurrentSchemaEvolution(
        table, config, lastCompletedTxnOwnerInstant, tableClusteringOwnerInstant).isPresent();
    assertFalse(hasSchema);
  }

  @Test
  void testLegacyClusteringInstantSkipsSchemaCheck() throws Exception {
    setupInstants(SCHEMA1, SCHEMA2, SCHEMA3, true, true);
    boolean hasSchema = strategy.resolveConcurrentSchemaEvolution(
        table, config, lastCompletedTxnOwnerInstant, tableReplacementOwnerInstant).isPresent();
    assertFalse(hasSchema);
  }

  @Test
  void testNoCurrentTxnOptionSkipSchemaEvolutionCheck() throws Exception {
    setupInstants(SCHEMA1, SCHEMA2, SCHEMA3, true, false);
    boolean hasResult = strategy.resolveConcurrentSchemaEvolution(
        table, config, lastCompletedTxnOwnerInstant, Option.empty()).isPresent();
    assertFalse(hasResult);
  }

  @Test
  void testSchemaConflictResolutionDisabled() throws Exception {
    setupInstants(SCHEMA1, SCHEMA2, SCHEMA2, false, false);
    boolean hasSchema = TransactionUtils.resolveSchemaConflictIfNeeded(
        table, config, lastCompletedTxnOwnerInstant, nonTableCompactionInstant).isPresent();
    assertFalse(hasSchema);
  }

  private Pair<HoodieRequestedReplaceMetadata, HoodieReplaceCommitMetadata> getGetDummyClusteringMetadata() {
    HoodieRequestedReplaceMetadata requestedReplaceMetadata = new HoodieRequestedReplaceMetadata();
    requestedReplaceMetadata.setOperationType(WriteOperationType.CLUSTER.toString());
    requestedReplaceMetadata.setVersion(1);
    HoodieSliceInfo sliceInfo = HoodieSliceInfo.newBuilder().setFileId("id1").build();
    List<HoodieClusteringGroup> clusteringGroups = new ArrayList<>();
    clusteringGroups.add(HoodieClusteringGroup.newBuilder()
        .setVersion(1).setNumOutputFileGroups(1).setMetrics(Collections.emptyMap())
        .setSlices(Collections.singletonList(sliceInfo)).build());
    requestedReplaceMetadata.setExtraMetadata(Collections.emptyMap());
    requestedReplaceMetadata.setClusteringPlan(HoodieClusteringPlan.newBuilder()
        .setVersion(1).setExtraMetadata(Collections.emptyMap())
        .setStrategy(HoodieClusteringStrategy.newBuilder().setStrategyClassName("").setVersion(1).build())
        .setInputGroups(clusteringGroups).build());

    HoodieReplaceCommitMetadata replaceMetadata = new HoodieReplaceCommitMetadata();
    replaceMetadata.addReplaceFileId("parititon", "replacedFileId");
    replaceMetadata.setOperationType(WriteOperationType.CLUSTER);
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPartitionPath("partition");
    writeStat.setPath("partition" + "/" + HoodieTestCommitGenerator.getBaseFilename(tableReplacementOwnerInstant.get().requestedTime(), "newFileId"));
    writeStat.setFileId("newFileId");
    writeStat.setTotalWriteBytes(1);
    writeStat.setFileSizeInBytes(1);
    replaceMetadata.addWriteStat("partition", writeStat);
    return Pair.of(requestedReplaceMetadata, replaceMetadata);
  }
}