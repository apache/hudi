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

package org.apache.hudi.common.schema.evolution;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaIdAssigner;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.TIMELINE_FACTORY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HoodieSchemaHistoryStorageManager}: persisting evolution
 * schemas to {@code .hoodie/.schema/}, reading them back, and the residual-
 * file cleanup that runs on each persist. Each persist+read pair also
 * exercises the {@link HoodieSchemaSerDe} JSON layout end-to-end.
 */
public class TestHoodieSchemaHistoryStorageManager extends HoodieCommonTestHarness {

  private HoodieActiveTimeline timeline;

  @BeforeEach
  public void setUp() throws Exception {
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanMetaClient();
  }

  @Test
  public void testPersistAndReadHistorySchemaStr() throws IOException {
    timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient);
    HoodieSchemaHistoryStorageManager fm = new HoodieSchemaHistoryStorageManager(metaClient);

    HoodieSchema currentSchema = simpleSchema(0L);
    fm.persistHistorySchemaStr("0000", HoodieSchemaSerDe.inheritHistory(currentSchema, ""));
    simulateCommit("0000");
    metaClient.reloadActiveTimeline();
    assertSameLogicalSchema(currentSchema, fm.getSchemaByKey("0"));

    HoodieSchema secondSchema = simpleSchema(1L);
    fm.persistHistorySchemaStr("0001", HoodieSchemaSerDe.inheritHistory(secondSchema, fm.getHistorySchemaStr()));
    simulateCommit("0001");
    metaClient.reloadActiveTimeline();
    assertSameLogicalSchema(secondSchema, fm.getSchemaByKey("1"));

    // Persist a schema for "0002" but never simulate the commit. The next persist
    // should treat 0002.schemacommit as a residual file from a failed write and
    // delete it as part of cleanResidualFiles.
    HoodieSchema thirdSchema = simpleSchema(2L);
    fm.persistHistorySchemaStr("0002", HoodieSchemaSerDe.inheritHistory(thirdSchema, fm.getHistorySchemaStr()));

    HoodieSchema lastSchema = simpleSchema(3L);
    fm.persistHistorySchemaStr("0004", HoodieSchemaSerDe.inheritHistory(lastSchema, fm.getHistorySchemaStr()));
    simulateCommit("0004");
    metaClient.reloadActiveTimeline();

    File residual = new File(metaClient.getSchemaFolderName() + File.separator + "0002.schemacommit");
    assertTrue(!residual.exists(), "residual schema file from failed persist should have been cleaned up");

    assertSameLogicalSchema(currentSchema, fm.getSchemaByKey("0"));
    assertSameLogicalSchema(secondSchema, fm.getSchemaByKey("1"));
    assertSameLogicalSchema(lastSchema, fm.getSchemaByKey("3"));
  }

  private void simulateCommit(String commitTime) {
    if (timeline == null) {
      timeline = TIMELINE_FACTORY.createActiveTimeline(metaClient);
    }
    HoodieInstant instant = INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, commitTime);
    timeline.createNewInstant(instant);
    timeline.transitionRequestedToInflight(instant, Option.empty());
    timeline.saveAsComplete(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, instant.getAction(), instant.requestedTime()),
        Option.empty());
  }

  /**
   * Builds a two-field schema (bool, int) with sequential ids assigned and the
   * given schemaId set on the wrapper. Mirrors the legacy test's getSimpleSchema
   * shape so existing on-disk fixtures (if any) parse back the same way.
   */
  private static HoodieSchema simpleSchema(long schemaId) {
    HoodieSchema schema = HoodieSchema.createRecord(
        "Record", null, "ns", false,
        Arrays.asList(
            HoodieSchemaField.of("bool", HoodieSchema.create(HoodieSchemaType.BOOLEAN)),
            HoodieSchemaField.of("int", HoodieSchema.create(HoodieSchemaType.INT))));
    HoodieSchemaIdAssigner.assign(schema, 0);
    schema.setSchemaId(schemaId);
    schema.invalidateIdIndex();
    return schema;
  }

  /**
   * Asserts the round-tripped schema preserves the version id and the
   * (id, name) pairs of the original. Avoids HoodieSchema#equals because
   * it compares full Avro schemas, including names/namespaces that the wire
   * format doesn't carry through the persist→read cycle.
   */
  private static void assertSameLogicalSchema(HoodieSchema expected, Option<HoodieSchema> actualOpt) {
    assertTrue(actualOpt.isPresent(), "expected a schema for the version id");
    HoodieSchema actual = actualOpt.get();
    assertEquals(expected.schemaId(), actual.schemaId());
    for (HoodieSchemaField f : expected.getFields()) {
      Option<HoodieSchemaField> roundTripped = actual.getField(f.name());
      assertTrue(roundTripped.isPresent(), "missing field: " + f.name());
      assertEquals(f.fieldId(), roundTripped.get().fieldId(), "field id mismatch for: " + f.name());
    }
  }
}
