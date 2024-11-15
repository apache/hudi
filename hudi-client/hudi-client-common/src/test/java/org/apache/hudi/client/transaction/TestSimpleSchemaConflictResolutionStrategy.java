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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieIndexCommitMetadata;
import org.apache.hudi.avro.model.HoodieIndexPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRestorePlan;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.client.utils.TransactionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieSchemaEvolutionConflictException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.bootstrap.HoodieBootstrapWriteMetadata;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.apache.hudi.common.util.CommitUtils.buildMetadata;
import static org.apache.hudi.config.HoodieWriteConfig.ENABLE_SCHEMA_CONFLICT_RESOLUTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestSimpleSchemaConflictResolutionStrategy {
  
  public Option<HoodieInstant> lastCompletedTxnOwnerInstant;
  public Option<HoodieInstant> tableCompactionOwnerInstant;
  public Option<HoodieInstant> nonTableCompactionInstant;
  @TempDir
  private java.nio.file.Path basePath;
  @Mock
  private HoodieWriteConfig config;
  private HoodieTableMetaClient metaClient;
  private HoodieTestTable dummyInstantGenerator;
  private TestTable table;
  private SimpleSchemaConflictResolutionStrategy strategy;
  
  private static final String SCHEMA1 = "{\"type\":\"record\",\"name\":\"MyRecord\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}]}";
  private static final String SCHEMA2 = "{\"type\":\"record\",\"name\":\"MyRecord\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"},{\"name\":\"field2\",\"type\":\"int\"}]}";
  private static final String SCHEMA3 = "{\"type\":\"record\",\"name\":\"MyRecord\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"},{\"name\":\"field3\",\"type\":\"boolean\"}]}";
  private static final String NULL_SCHEMA = "{\"type\":\"null\"}";

  private void setupMocks(String tableSchemaAtTxnStart, String tableSchemaAtTxnValidation,
                          String writerSchemaOfTxn, Boolean enableResolution) throws Exception {
    metaClient = HoodieTestUtils.getMetaClientBuilder(HoodieTableType.COPY_ON_WRITE, new Properties(),"")
        .setTableCreateSchema(SCHEMA1)
        .initTable(getDefaultStorageConf(), basePath.toString());
    dummyInstantGenerator = HoodieTestTable.of(metaClient);

    lastCompletedTxnOwnerInstant = Option.of(metaClient.createNewInstant(HoodieInstant.State.COMPLETED, COMMIT_ACTION, "001"));
    tableCompactionOwnerInstant = Option.of(metaClient.createNewInstant(HoodieInstant.State.INFLIGHT, COMPACTION_ACTION, "003"));
    nonTableCompactionInstant = Option.of(metaClient.createNewInstant(HoodieInstant.State.INFLIGHT, COMMIT_ACTION, "003"));

    dummyInstantGenerator.addCommit("001", Option.of(buildMetadata(
        Collections.emptyList(),
        Collections.emptyMap(),
        Option.empty(),
        WriteOperationType.UNKNOWN,
        tableSchemaAtTxnStart,
        COMMIT_ACTION)));
    dummyInstantGenerator.addCommit("002", Option.of(buildMetadata(
        Collections.emptyList(),
        Collections.emptyMap(),
        Option.empty(),
        WriteOperationType.UNKNOWN,
        tableSchemaAtTxnValidation,
        COMMIT_ACTION)));

    // Setup config
    TypedProperties typedProperties = new TypedProperties();
    typedProperties.setProperty(ENABLE_SCHEMA_CONFLICT_RESOLUTION.key(),
        enableResolution ? ENABLE_SCHEMA_CONFLICT_RESOLUTION.defaultValue().toString() : "false");
    config = HoodieWriteConfig.newBuilder().withSchema(writerSchemaOfTxn).withPath(basePath.toString()).withProperties(typedProperties).build();

    table = new TestTable(config, metaClient);
    strategy = new SimpleSchemaConflictResolutionStrategy();
  }

  @Test
  void testNoConflictFirstCommit() throws Exception {
    setupMocks(null, null, SCHEMA1, true);
    Schema result = strategy.resolveConcurrentSchemaEvolution(
        table, config, Option.empty(), nonTableCompactionInstant).get();
    assertEquals(new Schema.Parser().parse(SCHEMA1), result);
  }

  @Test
  void testNullWriterSchema() throws Exception {
    setupMocks(SCHEMA1, SCHEMA1, "", true);
    assertFalse(strategy.resolveConcurrentSchemaEvolution(
        table, config, lastCompletedTxnOwnerInstant, nonTableCompactionInstant).isPresent());
  }

  @Test
  void testNullTypeWriterSchema() throws Exception {
    setupMocks(SCHEMA1, SCHEMA1, NULL_SCHEMA, true);
    Schema result = strategy.resolveConcurrentSchemaEvolution(
        table, config, lastCompletedTxnOwnerInstant, nonTableCompactionInstant).get();
    assertEquals(new Schema.Parser().parse(SCHEMA1), result);
  }

  @Test
  void testConflictSecondCommitDifferentSchema() throws Exception {
    setupMocks(null, SCHEMA1, SCHEMA2, true);
    assertThrows(HoodieSchemaEvolutionConflictException.class,
        () -> strategy.resolveConcurrentSchemaEvolution(table, config, Option.empty(), nonTableCompactionInstant));
  }

  @Test
  void testConflictSecondCommitSameSchema() throws Exception {
    setupMocks(null, SCHEMA1, SCHEMA1, true);
    Schema result = strategy.resolveConcurrentSchemaEvolution(
        table, config, Option.empty(), nonTableCompactionInstant).get();
    assertEquals(new Schema.Parser().parse(SCHEMA1), result);
  }

  @Test
  void testNoConflictSameSchema() throws Exception {
    setupMocks(SCHEMA1, SCHEMA1, SCHEMA1, true);
    Schema result = strategy.resolveConcurrentSchemaEvolution(
        table, config, lastCompletedTxnOwnerInstant, nonTableCompactionInstant).get();
    assertEquals(new Schema.Parser().parse(SCHEMA1), result);
  }

  @Test
  void testNoConflictBackwardsCompatible1() throws Exception {
    setupMocks(SCHEMA1, SCHEMA2, SCHEMA1, true);
    Schema result = strategy.resolveConcurrentSchemaEvolution(
        table, config, lastCompletedTxnOwnerInstant, nonTableCompactionInstant).get();
    assertEquals(new Schema.Parser().parse(SCHEMA2), result);
  }

  @Test
  void testNoConflictBackwardsCompatible2() throws Exception {
    setupMocks(SCHEMA1, SCHEMA1, SCHEMA2, true);
    Schema result = strategy.resolveConcurrentSchemaEvolution(
        table, config, lastCompletedTxnOwnerInstant, nonTableCompactionInstant).get();
    assertEquals(new Schema.Parser().parse(SCHEMA2), result);
  }

  @Test
  void testNoConflictConcurrentEvolutionSameSchema() throws Exception {
    setupMocks(SCHEMA1, SCHEMA2, SCHEMA2, true);
    Schema result = strategy.resolveConcurrentSchemaEvolution(
        table, config, lastCompletedTxnOwnerInstant, nonTableCompactionInstant).get();
    assertEquals(new Schema.Parser().parse(SCHEMA2), result);
  }

  @Test
  void testCompactionTableServiceSkipSchemaEvolutionCheck() throws Exception {
    setupMocks(SCHEMA1, SCHEMA2, SCHEMA3, true);
    boolean hasSchema = strategy.resolveConcurrentSchemaEvolution(
        table, config, lastCompletedTxnOwnerInstant, tableCompactionOwnerInstant).isPresent();
    assertFalse(hasSchema);
  }

  @Test
  void testNoCurrentTxnOptionSkipSchemaEvolutionCheck() throws Exception {
    setupMocks(SCHEMA1, SCHEMA2, SCHEMA3, true);
    boolean hasResult = strategy.resolveConcurrentSchemaEvolution(
        table, config, lastCompletedTxnOwnerInstant, Option.empty()).isPresent();
    assertFalse(hasResult);
  }

  @Test
  void testSchemaConflictResolutionDisabled() throws Exception {
    setupMocks(SCHEMA1, SCHEMA2, SCHEMA2, false);
    boolean hasSchema = TransactionUtils.resolveSchemaConflictIfNeeded(
        table, config, lastCompletedTxnOwnerInstant, nonTableCompactionInstant).isPresent();
    assertFalse(hasSchema);
  }

  @Mock public static FileSystemViewManager viewManager;
  @Mock public static HoodieEngineContext engineContext;
  @Mock public static TaskContextSupplier taskContextSupplier;

  public class TestTable extends HoodieTable {

    protected TestTable(HoodieWriteConfig config, HoodieTableMetaClient metaClient) {
      super(config, engineContext, metaClient, viewManager, TestSimpleSchemaConflictResolutionStrategy.taskContextSupplier);
    }

    @Override
    protected HoodieIndex<?, ?> getIndex(HoodieWriteConfig config, HoodieEngineContext context) {
      return null;
    }

    @Override
    public HoodieWriteMetadata upsert(HoodieEngineContext context, String instantTime, Object records) {
      return null;
    }

    @Override
    public HoodieWriteMetadata insert(HoodieEngineContext context, String instantTime, Object records) {
      return null;
    }

    @Override
    public HoodieWriteMetadata delete(HoodieEngineContext context, String instantTime, Object keys) {
      return null;
    }

    @Override
    public HoodieWriteMetadata deletePrepped(HoodieEngineContext context, String instantTime, Object preppedRecords) {
      return null;
    }

    @Override
    public HoodieWriteMetadata upsertPrepped(HoodieEngineContext context, String instantTime, Object preppedRecords) {
      return null;
    }

    @Override
    public HoodieWriteMetadata insertPrepped(HoodieEngineContext context, String instantTime, Object preppedRecords) {
      return null;
    }

    @Override
    public HoodieWriteMetadata insertOverwrite(HoodieEngineContext context, String instantTime, Object records) {
      return null;
    }

    @Override
    public HoodieWriteMetadata insertOverwriteTable(HoodieEngineContext context, String instantTime, Object records) {
      return null;
    }

    @Override
    public HoodieWriteMetadata managePartitionTTL(HoodieEngineContext context, String instantTime) {
      return null;
    }

    @Override
    public HoodieWriteMetadata compact(HoodieEngineContext context, String compactionInstantTime) {
      return null;
    }

    @Override
    public HoodieWriteMetadata cluster(HoodieEngineContext context, String clusteringInstantTime) {
      return null;
    }

    @Override
    public void rollbackBootstrap(HoodieEngineContext context, String instantTime) {

    }

    @Override
    public HoodieCleanMetadata clean(HoodieEngineContext context, String cleanInstantTime) {
      return null;
    }

    @Override
    public Option<HoodieRollbackPlan> scheduleRollback(HoodieEngineContext context, String instantTime, HoodieInstant instantToRollback, boolean skipTimelinePublish,
                                                       boolean shouldRollbackUsingMarkers, boolean isRestore) {
      return null;
    }

    @Override
    public HoodieRollbackMetadata rollback(HoodieEngineContext context, String rollbackInstantTime, HoodieInstant commitInstant, boolean deleteInstants, boolean skipLocking) {
      return null;
    }

    @Override
    public Option<HoodieIndexCommitMetadata> index(HoodieEngineContext context, String indexInstantTime) {
      return null;
    }

    @Override
    public HoodieSavepointMetadata savepoint(HoodieEngineContext context, String instantToSavepoint, String user, String comment) {
      return null;
    }

    @Override
    public HoodieRestoreMetadata restore(HoodieEngineContext context, String restoreInstantTimestamp, String savepointToRestoreTimestamp) {
      return null;
    }

    @Override
    public Option<HoodieRestorePlan> scheduleRestore(HoodieEngineContext context, String restoreInstantTimestamp, String savepointToRestoreTimestamp) {
      return null;
    }

    @Override
    public Option<HoodieIndexPlan> scheduleIndexing(HoodieEngineContext context, String indexInstantTime, List partitionsToIndex, List partitionPaths) {
      return null;
    }

    @Override
    public Option<HoodieCleanerPlan> scheduleCleaning(HoodieEngineContext context, String instantTime, Option extraMetadata) {
      return null;
    }

    @Override
    public HoodieBootstrapWriteMetadata bootstrap(HoodieEngineContext context, Option extraMetadata) {
      return null;
    }

    @Override
    public Option<HoodieClusteringPlan> scheduleClustering(HoodieEngineContext context, String instantTime, Option extraMetadata) {
      return null;
    }

    @Override
    public Option<HoodieCompactionPlan> scheduleCompaction(HoodieEngineContext context, String instantTime, Option extraMetadata) {
      return null;
    }

    @Override
    public HoodieWriteMetadata bulkInsertPrepped(HoodieEngineContext context, String instantTime, Object preppedRecords, Option bulkInsertPartitioner) {
      return null;
    }

    @Override
    public HoodieWriteMetadata deletePartitions(HoodieEngineContext context, String instantTime, List partitions) {
      return null;
    }

    @Override
    public HoodieWriteMetadata bulkInsert(HoodieEngineContext context, String instantTime, Object records, Option bulkInsertPartitioner) {
      return null;
    }
  }
}