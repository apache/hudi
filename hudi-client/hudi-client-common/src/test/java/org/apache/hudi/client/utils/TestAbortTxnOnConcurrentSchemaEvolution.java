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

package org.apache.hudi.client.utils;

import org.apache.avro.Schema;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.table.HoodieTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestAbortTxnOnConcurrentSchemaEvolution {

  public static final Option<HoodieInstant> LAST_COMPLETED_TXN_OWNER_INSTANT = Option.of(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "001"));
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

  private void setupMocks(String schemaAtTxnStart, String schemaAtTxnValidation, String schemaOfTxn) throws Exception {
    HoodieInstant instantAtTxnStart = schemaAtTxnStart != null
        ? new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "001") : null;
    HoodieInstant instantAtTxnValidation = schemaAtTxnValidation != null
        ? new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "002") : null;

    when(commitsTimeline.lastInstant()).thenReturn(Option.ofNullable(instantAtTxnStart));

    HoodieActiveTimeline reloadedActiveTimeline = mock(HoodieActiveTimeline.class);
    HoodieTimeline reloadedCommitsTimeline = mock(HoodieTimeline.class);
    when(metaClient.reloadActiveTimeline()).thenReturn(reloadedActiveTimeline);
    when(reloadedActiveTimeline.getCommitsTimeline()).thenReturn(reloadedCommitsTimeline);
    when(reloadedCommitsTimeline.filterCompletedInstants()).thenReturn(reloadedCommitsTimeline);
    when(reloadedCommitsTimeline.lastInstant()).thenReturn(
        instantAtTxnValidation == null ? Option.empty() : Option.of(instantAtTxnValidation));

    when(schemaResolver.getTableAvroSchema(any(HoodieInstant.class), eq(false)))
        .thenAnswer(invocation -> {
          HoodieInstant arg = invocation.getArgument(0);
          if (arg.equals(instantAtTxnValidation)) {
            return new Schema.Parser().parse(schemaAtTxnValidation);
          } else if (arg.equals(instantAtTxnStart)) {
            return new Schema.Parser().parse(schemaAtTxnStart);
          } else {
            throw new IllegalArgumentException("Unexpected instant: " + arg);
          }
        });

    when(config.getWriteSchema()).thenReturn(schemaOfTxn);

    String commitMetadataJson = String.format("{\"" + HoodieCommitMetadata.SCHEMA_KEY + "\":\"%s\"}",
        schemaAtTxnValidation != null ? schemaAtTxnValidation.replace("\"", "\\\"") : "");
    byte[] commitMetadataBytes = commitMetadataJson.getBytes(StandardCharsets.UTF_8);

    when(commitsTimeline.getInstantDetails(any(HoodieInstant.class)))
        .thenReturn(Option.of(commitMetadataBytes));

    when(table.getActiveTimeline()).thenReturn(activeTimeline);
    when(table.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
  }

  @Test
  public void testNoConflictFirstCommit() throws Exception {
    setupMocks(null, null, SCHEMA1);
    Schema result = assertDoesNotThrow(() -> TransactionUtils.resolveConcurrentSchemaEvolution(table, config, Option.empty(), schemaResolver));
    assertEquals(new Schema.Parser().parse(SCHEMA1), result);
  }

  @Test
  public void testConflictSecondCommitDifferentSchema() throws Exception {
    setupMocks(null, SCHEMA1, SCHEMA2);
    assertThrows(HoodieWriteConflictException.class,
        () -> TransactionUtils.resolveConcurrentSchemaEvolution(table, config, Option.empty(), schemaResolver));
  }

  @Test
  public void testNoConflictSameSchema() throws Exception {
    setupMocks(SCHEMA1, SCHEMA1, SCHEMA1);
    Schema result = assertDoesNotThrow(() -> TransactionUtils.resolveConcurrentSchemaEvolution(table, config, LAST_COMPLETED_TXN_OWNER_INSTANT, schemaResolver));
    assertEquals(new Schema.Parser().parse(SCHEMA1), result);
  }

  @Test
  public void testNoConflictBackwardsCompatible() throws Exception {
    setupMocks(SCHEMA1, SCHEMA2, SCHEMA1);
    Schema result = assertDoesNotThrow(() -> TransactionUtils.resolveConcurrentSchemaEvolution(table, config, LAST_COMPLETED_TXN_OWNER_INSTANT, schemaResolver));
    assertEquals(new Schema.Parser().parse(SCHEMA2), result);
  }

  @Test
  public void testNoConflictConcurrentEvolutionSameSchema() throws Exception {
    setupMocks(SCHEMA1, SCHEMA2, SCHEMA2);
    Schema result = assertDoesNotThrow(() -> TransactionUtils.resolveConcurrentSchemaEvolution(table, config, LAST_COMPLETED_TXN_OWNER_INSTANT, schemaResolver));
    assertEquals(new Schema.Parser().parse(SCHEMA2), result);
  }

  @Test
  public void testConflictConcurrentEvolutionDifferentSchemas() throws Exception {
    setupMocks(SCHEMA1, SCHEMA2, SCHEMA3);
    assertThrows(HoodieWriteConflictException.class,
        () -> TransactionUtils.resolveConcurrentSchemaEvolution(table, config, LAST_COMPLETED_TXN_OWNER_INSTANT, schemaResolver));
  }
}