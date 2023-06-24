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

package org.apache.hudi.internal.schema.io;

import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.internal.schema.utils.SerDeHelper;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link FileBasedInternalSchemaStorageManager}.
 */
public class TestFileBasedInternalSchemaStorageManager extends HoodieCommonTestHarness {
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
    timeline = new HoodieActiveTimeline(metaClient);
    FileBasedInternalSchemaStorageManager fm = new FileBasedInternalSchemaStorageManager(metaClient);
    InternalSchema currentSchema = getSimpleSchema();
    currentSchema.setSchemaId(0L);
    // save first schema.
    fm.persistHistorySchemaStr("0000", SerDeHelper.inheritSchemas(currentSchema, ""));
    // Simulate commit.
    simulateCommit("0000");
    metaClient.reloadActiveTimeline();
    // try to read schema
    InternalSchema readSchema = fm.getSchemaByKey("0").get();
    assertEquals(currentSchema, readSchema);
    // save history schema again
    InternalSchema secondSchema = getSimpleSchema();
    secondSchema.setSchemaId(1L);
    fm.persistHistorySchemaStr("0001", SerDeHelper.inheritSchemas(secondSchema, fm.getHistorySchemaStr()));
    // Simulate commit.
    simulateCommit("0001");
    metaClient.reloadActiveTimeline();
    // try to read schema
    assertEquals(secondSchema, fm.getSchemaByKey("1").get());

    // test write failed and residual file clean.
    InternalSchema thirdSchema = getSimpleSchema();
    thirdSchema.setSchemaId(2L);
    fm.persistHistorySchemaStr("0002", SerDeHelper.inheritSchemas(thirdSchema, fm.getHistorySchemaStr()));
    // do not simulate commit "0002", so current save file will be residual files.
    // try 4st persist
    InternalSchema lastSchema = getSimpleSchema();
    lastSchema.setSchemaId(3L);
    fm.persistHistorySchemaStr("0004", SerDeHelper.inheritSchemas(lastSchema, fm.getHistorySchemaStr()));
    simulateCommit("0004");
    metaClient.reloadActiveTimeline();
    // now the residual file created by 3st persist should be removed.
    File f = new File(metaClient.getSchemaFolderName() + File.separator + "0002.schemacommit");
    assertTrue(!f.exists());
    assertEquals(lastSchema, fm.getSchemaByKey("3").get());
  }

  private void simulateCommit(String commitTime) {
    if (timeline == null) {
      timeline = new HoodieActiveTimeline(metaClient);
    }
    HoodieInstant instant = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, commitTime);
    timeline.createNewInstant(instant);
    timeline.transitionRequestedToInflight(instant, Option.empty());
    timeline.saveAsComplete(new HoodieInstant(true, instant.getAction(), instant.getTimestamp()),
        Option.empty());
  }

  private InternalSchema getSimpleSchema() {
    Types.RecordType record = Types.RecordType.get(Arrays.asList(new Types.Field[] {
        Types.Field.get(0, "bool", Types.BooleanType.get()),
        Types.Field.get(1, "int", Types.IntType.get()),
    }));
    return new InternalSchema(record);
  }
}

