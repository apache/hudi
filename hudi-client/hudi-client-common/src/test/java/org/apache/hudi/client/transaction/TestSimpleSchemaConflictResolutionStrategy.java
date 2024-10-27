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

import org.apache.hudi.client.utils.TransactionUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieSchemaEvolutionConflictException;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.nio.charset.StandardCharsets;

import static org.apache.hudi.config.HoodieWriteConfig.ENABLE_SCHEMA_CONFLICT_RESOLUTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestSimpleSchemaConflictResolutionStrategy extends SimpleSchemaConflictResolutionStrategy {

  public static final Option<HoodieInstant> LAST_COMPLETED_TXN_OWNER_INSTANT =
      Option.of(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "001"));
  public static final Option<HoodieInstant> TABLE_COMPACTION_OWNER_INSTANT =
      Option.of(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "002"));
  public static final Option<HoodieInstant> NON_TABLE_COMPACTION_INSTANT =
      Option.of(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "003"));
  @Mock
  private HoodieTable table;
  @Mock
  private HoodieWriteConfig config;
  @Mock
  private HoodieTableMetaClient metaClient;
  @Mock
  private HoodieActiveTimeline activeTimeline;
  @Mock
  private HoodieTimeline commitsTimeline;
  @Mock
  private TableSchemaResolver schemaResolver;

  private static final String SCHEMA1 = "{\"type\":\"record\",\"name\":\"MyRecord\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}]}";
  private static final String SCHEMA2 = "{\"type\":\"record\",\"name\":\"MyRecord\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"},{\"name\":\"field2\",\"type\":\"int\"}]}";
  private static final String SCHEMA3 = "{\"type\":\"record\",\"name\":\"MyRecord\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"},{\"name\":\"field3\",\"type\":\"boolean\"}]}";

  @BeforeEach
  public void setup() {
    MockitoAnnotations.openMocks(this);

    when(table.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(activeTimeline.getCommitsTimeline()).thenReturn(commitsTimeline);
    when(commitsTimeline.filterCompletedInstants()).thenReturn(commitsTimeline);
  }
  
  @Override
  @NotNull
  TableSchemaResolver getSchemaResolver(HoodieTable table) {
    return schemaResolver;
  }
  
  private void setupMocks(String tableSchemaAtTxnStart, String tableSchemaAtTxnValidation, String writerSchemaOfTxn, Boolean enableResolution)
      throws Exception {
    HoodieInstant instantAtTxnStart = tableSchemaAtTxnStart != null
        ? new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "001") : null;
    HoodieInstant instantAtTxnValidation = tableSchemaAtTxnValidation != null
        ? new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "002") : null;

    when(commitsTimeline.lastInstant()).thenReturn(Option.ofNullable(instantAtTxnStart));

    HoodieTimeline commitsTimeline = mock(HoodieTimeline.class);
    when(metaClient.getCommitsTimeline()).thenReturn(commitsTimeline);
    when(metaClient.getSchemaEvolutionTimeline()).thenReturn(commitsTimeline);
    HoodieTimeline filteredTimeline = mock(HoodieTimeline.class);
    if (tableSchemaAtTxnStart != null) {
      when(commitsTimeline.findInstantsBeforeOrEquals(matches(instantAtTxnStart.getTimestamp())))
          .thenReturn(filteredTimeline);
      when(filteredTimeline.lastInstant()).thenReturn(Option.of(instantAtTxnStart));
    }
    when(commitsTimeline.filterCompletedInstants()).thenReturn(commitsTimeline);
    when(commitsTimeline.lastInstant()).thenReturn(
        instantAtTxnValidation == null ? Option.empty() : Option.of(instantAtTxnValidation));

    when(schemaResolver.getTableAvroSchema(any(HoodieInstant.class), eq(false)))
        .thenAnswer(invocation -> {
          HoodieInstant arg = invocation.getArgument(0);
          if (arg.equals(instantAtTxnValidation)) {
            return new Schema.Parser().parse(tableSchemaAtTxnValidation);
          } else if (arg.equals(instantAtTxnStart)) {
            return new Schema.Parser().parse(tableSchemaAtTxnStart);
          } else {
            throw new IllegalArgumentException("Unexpected instant: " + arg);
          }
        });

    when(config.getBoolean(ENABLE_SCHEMA_CONFLICT_RESOLUTION)).thenReturn(
        enableResolution ? ENABLE_SCHEMA_CONFLICT_RESOLUTION.defaultValue() : false);
    when(config.getWriteSchema()).thenReturn(writerSchemaOfTxn);

    String commitMetadataJson = String.format("{\"" + HoodieCommitMetadata.SCHEMA_KEY + "\":\"%s\"}",
        tableSchemaAtTxnValidation != null ? tableSchemaAtTxnValidation.replace("\"", "\\\"") : "");
    byte[] commitMetadataBytes = commitMetadataJson.getBytes(StandardCharsets.UTF_8);

    when(this.commitsTimeline.getInstantDetails(any(HoodieInstant.class)))
        .thenReturn(Option.of(commitMetadataBytes));

    when(table.getActiveTimeline()).thenReturn(activeTimeline);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
  }

  @Test
  void testNoConflictFirstCommit() throws Exception {
    setupMocks(null, null, SCHEMA1, true);
    Schema result = resolveConcurrentSchemaEvolution(
        table, config, Option.empty(), NON_TABLE_COMPACTION_INSTANT).get();
    assertEquals(new Schema.Parser().parse(SCHEMA1), result);
  }

  @Test
  void testConflictSecondCommitDifferentSchema() throws Exception {
    setupMocks(null, SCHEMA1, SCHEMA2, true);
    assertThrows(HoodieSchemaEvolutionConflictException.class,
        () -> resolveConcurrentSchemaEvolution(table, config, Option.empty(), NON_TABLE_COMPACTION_INSTANT));
  }

  @Test
  void testConflictSecondCommitSameSchema() throws Exception {
    setupMocks(null, SCHEMA1, SCHEMA1, true);
    Schema result = resolveConcurrentSchemaEvolution(
        table, config, Option.empty(), NON_TABLE_COMPACTION_INSTANT).get();
    assertEquals(new Schema.Parser().parse(SCHEMA1), result);
  }

  @Test
  void testNoConflictSameSchema() throws Exception {
    setupMocks(SCHEMA1, SCHEMA1, SCHEMA1, true);
    Schema result = resolveConcurrentSchemaEvolution(
        table, config, LAST_COMPLETED_TXN_OWNER_INSTANT, NON_TABLE_COMPACTION_INSTANT).get();
    assertEquals(new Schema.Parser().parse(SCHEMA1), result);
  }

  @Test
  void testNoConflictBackwardsCompatible1() throws Exception {
    setupMocks(SCHEMA1, SCHEMA2, SCHEMA1, true);
    Schema result = resolveConcurrentSchemaEvolution(
        table, config, LAST_COMPLETED_TXN_OWNER_INSTANT, NON_TABLE_COMPACTION_INSTANT).get();
    assertEquals(new Schema.Parser().parse(SCHEMA2), result);
  }

  @Test
  void testNoConflictBackwardsCompatible2() throws Exception {
    setupMocks(SCHEMA1, SCHEMA1, SCHEMA2, true);
    Schema result = resolveConcurrentSchemaEvolution(
        table, config, LAST_COMPLETED_TXN_OWNER_INSTANT, NON_TABLE_COMPACTION_INSTANT).get();
    assertEquals(new Schema.Parser().parse(SCHEMA2), result);
  }

  @Test
  void testNoConflictConcurrentEvolutionSameSchema() throws Exception {
    setupMocks(SCHEMA1, SCHEMA2, SCHEMA2, true);
    Schema result = resolveConcurrentSchemaEvolution(
        table, config, LAST_COMPLETED_TXN_OWNER_INSTANT, NON_TABLE_COMPACTION_INSTANT).get();
    assertEquals(new Schema.Parser().parse(SCHEMA2), result);
  }

  @Test
  void testConflictConcurrentEvolutionDifferentSchemas() throws Exception {
    setupMocks(SCHEMA1, SCHEMA2, SCHEMA3, true);
    assertThrows(HoodieSchemaEvolutionConflictException.class,
        () -> resolveConcurrentSchemaEvolution(
            table, config, LAST_COMPLETED_TXN_OWNER_INSTANT, NON_TABLE_COMPACTION_INSTANT));
  }

  @Test
  void testCompactionTableServiceSkipSchemaEvolutionCheck() throws Exception {
    setupMocks(SCHEMA1, SCHEMA2, SCHEMA3, true);
    boolean hasSchema = resolveConcurrentSchemaEvolution(
        table, config, LAST_COMPLETED_TXN_OWNER_INSTANT, TABLE_COMPACTION_OWNER_INSTANT).isPresent();
    assertFalse(hasSchema);
  }

  @Test
  void testNoCurrentTxnOptionSkipSchemaEvolutionCheck() throws Exception {
    setupMocks(SCHEMA1, SCHEMA2, SCHEMA3, true);
    boolean hasResult = resolveConcurrentSchemaEvolution(
        table, config, LAST_COMPLETED_TXN_OWNER_INSTANT, Option.empty()).isPresent();
    assertFalse(hasResult);
  }

  @Test
  void testSchemaConflictResolutionDisabled() throws Exception {
    setupMocks(SCHEMA1, SCHEMA2, SCHEMA2, false);
    boolean hasSchema = TransactionUtils.resolveSchemaConflictIfNeeded(
        table, config, LAST_COMPLETED_TXN_OWNER_INSTANT, NON_TABLE_COMPACTION_INSTANT).isPresent();
    assertFalse(hasSchema);
  }
}