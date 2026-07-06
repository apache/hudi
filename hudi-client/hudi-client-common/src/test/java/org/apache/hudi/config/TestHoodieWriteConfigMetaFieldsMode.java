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

package org.apache.hudi.config;

import org.apache.hudi.common.model.HoodieRecord;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates the writer-side accessors and validation guards for the meta-field-population modes
 * on {@link HoodieWriteConfig}. Companion test for the {@link
 * org.apache.hudi.common.table.HoodieTableConfig} accessors lives in {@code TestHoodieMetaFieldsMode};
 * this test covers the writer-builder surface and the cross-flag validation that runs at
 * {@code build()} time.
 */
class TestHoodieWriteConfigMetaFieldsMode {

  private static HoodieWriteConfig.Builder baseBuilder() {
    return HoodieWriteConfig.newBuilder().withPath("file:///tmp/test_hudi_meta_fields_mode");
  }

  @Test
  void defaultsToAllMode() {
    HoodieWriteConfig cfg = baseBuilder().build();
    assertTrue(cfg.populateMetaFields());
    assertTrue(cfg.getMetaFieldsMode().isEmpty(),
        "mode list must be ignored when populate.meta.fields=true");
    assertTrue(cfg.isCommitTimePopulated());
    assertTrue(cfg.isFileNamePopulated());
  }

  @Test
  void explicitNoneModeBuilds() {
    HoodieWriteConfig cfg = baseBuilder().withPopulateMetaFields(false).build();
    assertFalse(cfg.populateMetaFields());
    assertTrue(cfg.getMetaFieldsMode().isEmpty());
    assertFalse(cfg.isCommitTimePopulated());
    assertFalse(cfg.isFileNamePopulated());
  }

  @Test
  void commitTimeOnlyModeBuildsAndIsAdditiveOverNone() {
    HoodieWriteConfig cfg = baseBuilder()
        .withPopulateMetaFields(false)
        .withMetaFieldsMode(HoodieRecord.COMMIT_TIME_METADATA_FIELD)
        .build();
    assertFalse(cfg.populateMetaFields());
    assertTrue(cfg.isCommitTimePopulated(),
        "incremental query semantics depend on _hoodie_commit_time being populated in this mode");
    assertFalse(cfg.isFileNamePopulated());
  }

  @Test
  void fileNameOnlyModeBuilds() {
    HoodieWriteConfig cfg = baseBuilder()
        .withPopulateMetaFields(false)
        .withMetaFieldsMode(HoodieRecord.FILENAME_METADATA_FIELD)
        .build();
    assertFalse(cfg.populateMetaFields());
    assertFalse(cfg.isCommitTimePopulated());
    assertTrue(cfg.isFileNamePopulated());
  }

  @Test
  void commitTimeAndFileNameCombinationBuilds() {
    HoodieWriteConfig cfg = baseBuilder()
        .withPopulateMetaFields(false)
        .withMetaFieldsMode(HoodieRecord.COMMIT_TIME_METADATA_FIELD + "," + HoodieRecord.FILENAME_METADATA_FIELD)
        .build();
    assertFalse(cfg.populateMetaFields());
    assertTrue(cfg.isCommitTimePopulated());
    assertTrue(cfg.isFileNamePopulated());
  }

  @Test
  void rejectsIncompatibleCombination() {
    // populate.meta.fields=true together with a non-empty mode is ambiguous (mode has no effect
    // when all meta fields are already populated); reject loudly.
    HoodieWriteConfig.Builder builder = baseBuilder()
        .withPopulateMetaFields(true)
        .withMetaFieldsMode(HoodieRecord.COMMIT_TIME_METADATA_FIELD);
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, builder::build);
    assertTrue(ex.getMessage().contains("hoodie.meta.fields.mode"),
        "exception must name the mode property: " + ex.getMessage());
    assertTrue(ex.getMessage().contains("hoodie.populate.meta.fields"),
        "exception must name the legacy property too: " + ex.getMessage());
  }

  @Test
  void rejectsUnknownTokenInMode() {
    HoodieWriteConfig.Builder builder = baseBuilder()
        .withPopulateMetaFields(false)
        .withMetaFieldsMode(HoodieRecord.RECORD_KEY_METADATA_FIELD);
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, builder::build);
    assertTrue(ex.getMessage().contains(HoodieRecord.RECORD_KEY_METADATA_FIELD),
        "exception must name the rejected token: " + ex.getMessage());
    assertTrue(ex.getMessage().contains("populate.meta.fields"),
        "exception must recommend populate.meta.fields=true for other columns: " + ex.getMessage());
  }

  @Test
  void noneModeWithEmptyModeExplicitIsStillNone() {
    HoodieWriteConfig cfg = baseBuilder()
        .withPopulateMetaFields(false)
        .withMetaFieldsMode("")
        .build();
    assertFalse(cfg.populateMetaFields());
    assertTrue(cfg.getMetaFieldsMode().isEmpty());
    assertFalse(cfg.isCommitTimePopulated());
    assertFalse(cfg.isFileNamePopulated());
  }
}
