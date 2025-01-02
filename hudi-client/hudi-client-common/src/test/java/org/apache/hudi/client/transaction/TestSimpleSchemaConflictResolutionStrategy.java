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

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieClusteringStrategy;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
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

import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.config.HoodieWriteConfig.ENABLE_SCHEMA_CONFLICT_RESOLUTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class TestSimpleSchemaConflictResolutionStrategy extends SimpleSchemaConflictResolutionStrategy {

  public static final Option<HoodieInstant> LAST_COMPLETED_TXN_OWNER_INSTANT =
      Option.of(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "001"));
  public static final Option<HoodieInstant> TABLE_COMPACTION_OWNER_INSTANT =
      Option.of(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, "002"));
  public static final Option<HoodieInstant> CLUSTERING_INSTANT =
      Option.of(new HoodieInstant(HoodieInstant.State.INFLIGHT, REPLACE_COMMIT_ACTION, "002"));
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
  private HoodieTimeline schemaEvolutionTimeline;
  @Mock
  private TableSchemaResolver schemaResolver;

  private static final String SCHEMA1 = "{\"type\":\"record\",\"name\":\"MyRecord\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"}]}";
  private static final String SCHEMA2 = "{\"type\":\"record\",\"name\":\"MyRecord\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"},{\"name\":\"field2\",\"type\":\"int\"}]}";
  private static final String SCHEMA3 = "{\"type\":\"record\",\"name\":\"MyRecord\",\"fields\":[{\"name\":\"field1\",\"type\":\"string\"},{\"name\":\"field3\",\"type\":\"boolean\"}]}";
  private static final String NULL_SCHEMA = "{\"type\":\"null\"}";

  @BeforeEach
  public void setup() {
    MockitoAnnotations.openMocks(this);

    when(table.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(metaClient.getSchemaEvolutionTimelineInReverseOrder()).thenReturn(schemaEvolutionTimeline);
  }

  @Override
  @NotNull
  TableSchemaResolver getSchemaResolver(HoodieTable table) {
    return schemaResolver;
  }

  private void setupMocks(String tableSchemaAtTxnStart, String tableSchemaAtTxnValidation,
                          String writerSchemaOfTxn, Boolean enableResolution,
                          Option<HoodieInstant> clusteringInstant) throws Exception {
    HoodieInstant instantAtTxnStart = tableSchemaAtTxnStart != null
        ? new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "001") : null;
    HoodieInstant instantAtTxnValidation = tableSchemaAtTxnValidation != null
        ? new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "002") : null;

    // Setup schema evolution timeline
    when(schemaEvolutionTimeline.filterCompletedInstants()).thenReturn(schemaEvolutionTimeline);
    when(schemaEvolutionTimeline.firstInstant()).thenReturn(
        instantAtTxnValidation == null ? Option.empty() : Option.of(instantAtTxnValidation));

    // Mock getInstantsAsStream to return new stream each time
    if (instantAtTxnValidation != null) {
      when(schemaEvolutionTimeline.getInstantsAsStream()).thenAnswer(invocation -> {
        if (instantAtTxnStart != null) {
          return Stream.of(instantAtTxnValidation, instantAtTxnStart);
        } else {
          return Stream.of(instantAtTxnValidation);
        }
      });
    }

    // Setup schema resolver
    when(schemaResolver.getTableAvroSchema(any(HoodieInstant.class), eq(false)))
        .thenAnswer(invocation -> {
          HoodieInstant instant = invocation.getArgument(0);
          if (instant.equals(instantAtTxnValidation)) {
            return new Schema.Parser().parse(tableSchemaAtTxnValidation);
          } else if (instant.equals(instantAtTxnStart)) {
            return new Schema.Parser().parse(tableSchemaAtTxnStart);
          }
          return new Schema.Parser().parse(tableSchemaAtTxnValidation); // Default case
        });

    // Setup config
    when(config.getBoolean(ENABLE_SCHEMA_CONFLICT_RESOLUTION)).thenReturn(
        enableResolution ? ENABLE_SCHEMA_CONFLICT_RESOLUTION.defaultValue() : false);
    when(config.getWriteSchema()).thenReturn(writerSchemaOfTxn);

    // Setup commit metadata with schema
    when(schemaEvolutionTimeline.deserializeInstantContent(any(HoodieInstant.class), eq(HoodieCommitMetadata.class)))
        .thenAnswer(invocation -> {
          HoodieInstant instant = invocation.getArgument(0);
          String schema;
          if (instant.equals(instantAtTxnValidation)) {
            schema = tableSchemaAtTxnValidation;
          } else if (instant.equals(instantAtTxnStart)) {
            schema = tableSchemaAtTxnStart;
          } else {
            schema = tableSchemaAtTxnValidation; // Default case
          }

          HoodieCommitMetadata metadata = new HoodieCommitMetadata();
          metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, schema);
          return metadata;
        });

    // Mock the timeline to ensure filtering works correctly
    when(schemaEvolutionTimeline.findInstantsAfter(any(String.class), anyInt())).thenReturn(schemaEvolutionTimeline);
    when(schemaEvolutionTimeline.containsInstant(any(HoodieInstant.class))).thenReturn(true);
    when(schemaEvolutionTimeline.isBeforeTimelineStarts(any(String.class))).thenReturn(false);

    // Setup schema resolver
    if (instantAtTxnValidation != null || instantAtTxnStart != null) {
      when(schemaResolver.getTableAvroSchema(any(HoodieInstant.class), eq(false)))
          .thenAnswer(invocation -> {
            HoodieInstant instant = invocation.getArgument(0);
            if (instant.equals(instantAtTxnValidation)) {
              return new Schema.Parser().parse(tableSchemaAtTxnValidation);
            } else if (instant.equals(instantAtTxnStart)) {
              return new Schema.Parser().parse(tableSchemaAtTxnStart);
            }
            return new Schema.Parser().parse(tableSchemaAtTxnValidation != null
                ? tableSchemaAtTxnValidation : tableSchemaAtTxnStart);
          });
    }
    // Setup create schema as fallback
    when(schemaResolver.getTableCreateSchemaWithMetadata(eq(false)))
        .thenReturn(Option.empty());  // For first commit case, return empty schema

    // Add clustering specific mocks if needed
    if (clusteringInstant.isPresent()) {
      // Create clustering plan metadata
      HoodieRequestedReplaceMetadata requestedReplaceMetadata =
          HoodieRequestedReplaceMetadata.newBuilder()
              .setOperationType("CLUSTER")
              .setClusteringPlan(HoodieClusteringPlan.newBuilder()
                  .setVersion(1)
                  .setStrategy(HoodieClusteringStrategy.newBuilder()
                      .setStrategyClassName("org.apache.hudi.client.clustering.run.strategy.SparkSingleFileSortStrategy")
                      .build())
                  .build())
              .build();

      // Get the corresponding requested instant for the inflight instant
      HoodieInstant requestedInstant = new HoodieInstant(HoodieInstant.State.REQUESTED,
          REPLACE_COMMIT_ACTION, clusteringInstant.get().getTimestamp());
      when(activeTimeline.deserializeInstantContent(requestedInstant, HoodieRequestedReplaceMetadata.class)).thenReturn(requestedReplaceMetadata);
    }
  }

  @Test
  void testNoConflictFirstCommit() throws Exception {
    setupMocks(null, null, SCHEMA1, true, Option.empty());
    Schema result = resolveConcurrentSchemaEvolution(
        table, config, Option.empty(), NON_TABLE_COMPACTION_INSTANT).get();
    assertEquals(new Schema.Parser().parse(SCHEMA1), result);
  }

  @Test
  void testNullWriterSchema() throws Exception {
    setupMocks(SCHEMA1, SCHEMA1, "", true, Option.empty());
    assertFalse(resolveConcurrentSchemaEvolution(
        table, config, LAST_COMPLETED_TXN_OWNER_INSTANT, NON_TABLE_COMPACTION_INSTANT).isPresent());
  }

  @Test
  void testNullTypeWriterSchema() throws Exception {
    setupMocks(SCHEMA1, SCHEMA1, NULL_SCHEMA, true, Option.empty());
    Schema result = resolveConcurrentSchemaEvolution(
        table, config, LAST_COMPLETED_TXN_OWNER_INSTANT, NON_TABLE_COMPACTION_INSTANT).get();
    assertEquals(new Schema.Parser().parse(SCHEMA1), result);
  }

  @Test
  void testClusteringInstantSkipsSchemaCheck() throws Exception {
    setupMocks(SCHEMA1, SCHEMA2, SCHEMA3, true, CLUSTERING_INSTANT);
    boolean hasSchema = resolveConcurrentSchemaEvolution(
        table, config, LAST_COMPLETED_TXN_OWNER_INSTANT, CLUSTERING_INSTANT).isPresent();
    assertFalse(hasSchema);
  }

  @Test
  void testConflictSecondCommitDifferentSchema() throws Exception {
    setupMocks(null, SCHEMA1, SCHEMA2, true, Option.empty());
    assertThrows(HoodieSchemaEvolutionConflictException.class,
        () -> resolveConcurrentSchemaEvolution(table, config, Option.empty(), NON_TABLE_COMPACTION_INSTANT));
  }

  @Test
  void testConflictSecondCommitSameSchema() throws Exception {
    setupMocks(null, SCHEMA1, SCHEMA1, true, Option.empty());
    Schema result = resolveConcurrentSchemaEvolution(
        table, config, Option.empty(), NON_TABLE_COMPACTION_INSTANT).get();
    assertEquals(new Schema.Parser().parse(SCHEMA1), result);
  }

  @Test
  void testNoConflictSameSchema() throws Exception {
    setupMocks(SCHEMA1, SCHEMA1, SCHEMA1, true, Option.empty());
    Schema result = resolveConcurrentSchemaEvolution(
        table, config, LAST_COMPLETED_TXN_OWNER_INSTANT, NON_TABLE_COMPACTION_INSTANT).get();
    assertEquals(new Schema.Parser().parse(SCHEMA1), result);
  }

  @Test
  void testNoConflictBackwardsCompatible1() throws Exception {
    setupMocks(SCHEMA1, SCHEMA2, SCHEMA1, true, Option.empty());
    Schema result = resolveConcurrentSchemaEvolution(
        table, config, LAST_COMPLETED_TXN_OWNER_INSTANT, NON_TABLE_COMPACTION_INSTANT).get();
    assertEquals(new Schema.Parser().parse(SCHEMA2), result);
  }

  @Test
  void testNoConflictBackwardsCompatible2() throws Exception {
    setupMocks(SCHEMA1, SCHEMA1, SCHEMA2, true, Option.empty());
    Schema result = resolveConcurrentSchemaEvolution(
        table, config, LAST_COMPLETED_TXN_OWNER_INSTANT, NON_TABLE_COMPACTION_INSTANT).get();
    assertEquals(new Schema.Parser().parse(SCHEMA2), result);
  }

  @Test
  void testNoConflictConcurrentEvolutionSameSchema() throws Exception {
    setupMocks(SCHEMA1, SCHEMA2, SCHEMA2, true, Option.empty());
    Schema result = resolveConcurrentSchemaEvolution(
        table, config, LAST_COMPLETED_TXN_OWNER_INSTANT, NON_TABLE_COMPACTION_INSTANT).get();
    assertEquals(new Schema.Parser().parse(SCHEMA2), result);
  }

  @Test
  void testCompactionTableServiceSkipSchemaEvolutionCheck() throws Exception {
    setupMocks(SCHEMA1, SCHEMA2, SCHEMA3, true, Option.empty());
    boolean hasSchema = resolveConcurrentSchemaEvolution(
        table, config, LAST_COMPLETED_TXN_OWNER_INSTANT, TABLE_COMPACTION_OWNER_INSTANT).isPresent();
    assertFalse(hasSchema);
  }

  @Test
  void testNoCurrentTxnOptionSkipSchemaEvolutionCheck() throws Exception {
    setupMocks(SCHEMA1, SCHEMA2, SCHEMA3, true, Option.empty());
    boolean hasResult = resolveConcurrentSchemaEvolution(
        table, config, LAST_COMPLETED_TXN_OWNER_INSTANT, Option.empty()).isPresent();
    assertFalse(hasResult);
  }

  @Test
  void testSchemaConflictResolutionDisabled() throws Exception {
    setupMocks(SCHEMA1, SCHEMA2, SCHEMA2, false, Option.empty());
    boolean hasSchema = TransactionUtils.resolveSchemaConflictIfNeeded(
        table, config, LAST_COMPLETED_TXN_OWNER_INSTANT, NON_TABLE_COMPACTION_INSTANT).isPresent();
    assertFalse(hasSchema);
  }
}