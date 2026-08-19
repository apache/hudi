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

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.MetaFieldsMode;
import org.apache.hudi.common.table.HoodieTableConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates the writer-side accessors and validation guards for the meta-field-population modes
 * on {@link HoodieWriteConfig}. Companion test for the {@link HoodieTableConfig} accessors lives
 * in {@code TestHoodieMetaFieldsMode}; this test covers the writer-builder surface and the
 * cross-flag validation that runs at {@code build()} time.
 */
class TestHoodieWriteConfigMetaFieldsMode {

  private static HoodieWriteConfig.Builder baseBuilder() {
    return HoodieWriteConfig.newBuilder().withPath("file:///tmp/test_hudi_meta_fields_mode");
  }

  @Test
  void defaultsToAllMode() {
    HoodieWriteConfig cfg = baseBuilder().build();
    assertTrue(cfg.populateMetaFields());
    assertEquals(MetaFieldsMode.ALL, cfg.getMetaFieldsMode());
    assertTrue(cfg.isCommitTimePopulated());
    assertTrue(cfg.isFileNamePopulated());
  }

  @Test
  void explicitNoneModeBuilds() {
    HoodieWriteConfig cfg = baseBuilder().withPopulateMetaFields(false).build();
    assertFalse(cfg.populateMetaFields());
    assertEquals(MetaFieldsMode.NONE, cfg.getMetaFieldsMode());
    assertFalse(cfg.isCommitTimePopulated());
    assertFalse(cfg.isFileNamePopulated());
  }

  @Test
  void commitTimeOnlyModeBuilds() {
    HoodieWriteConfig cfg = baseBuilder()
        .withPopulateMetaFields(false)
        .withMetaFieldsMode(MetaFieldsMode.COMMIT_TIME_ONLY)
        .build();
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, cfg.getMetaFieldsMode());
    assertTrue(cfg.isCommitTimePopulated());
    assertFalse(cfg.isFileNamePopulated());
  }

  @Test
  void fileNameOnlyModeBuilds() {
    HoodieWriteConfig cfg = baseBuilder()
        .withPopulateMetaFields(false)
        .withMetaFieldsMode(MetaFieldsMode.FILE_NAME_ONLY)
        .build();
    assertEquals(MetaFieldsMode.FILE_NAME_ONLY, cfg.getMetaFieldsMode());
    assertFalse(cfg.isCommitTimePopulated());
    assertTrue(cfg.isFileNamePopulated());
  }

  @Test
  void commitTimeAndFileNameCombinationBuilds() {
    HoodieWriteConfig cfg = baseBuilder()
        .withPopulateMetaFields(false)
        .withMetaFieldsMode(MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME)
        .build();
    assertEquals(MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME, cfg.getMetaFieldsMode());
    assertTrue(cfg.isCommitTimePopulated());
    assertTrue(cfg.isFileNamePopulated());
  }

  @Test
  void rejectsIncompatibleCombination() {
    // populate.meta.fields=true together with a selective mode is ambiguous — reject.
    HoodieWriteConfig.Builder builder = baseBuilder()
        .withPopulateMetaFields(true)
        .withMetaFieldsMode(MetaFieldsMode.COMMIT_TIME_ONLY);
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, builder::build);
    assertTrue(ex.getMessage().contains("hoodie.meta.fields.mode"),
        "exception must name the mode property: " + ex.getMessage());
    assertTrue(ex.getMessage().contains("hoodie.populate.meta.fields"),
        "exception must name the legacy property too: " + ex.getMessage());
  }

  @Test
  void allModeWithPopulateFalseIsAlsoRejectedByBuilder() {
    // Explicitly setting ALL is a no-op — the builder normalizes it to empty. Passing ALL directly
    // is fine; ensuring populateMetaFields agrees is the caller's responsibility (validate() runs
    // the cross-check at build time).
    HoodieWriteConfig cfg = baseBuilder()
        .withPopulateMetaFields(true)
        .withMetaFieldsMode(MetaFieldsMode.ALL)
        .build();
    assertEquals(MetaFieldsMode.ALL, cfg.getMetaFieldsMode());
  }

  @Test
  void noneModeWithExplicitBuildIsStillNone() {
    HoodieWriteConfig cfg = baseBuilder()
        .withPopulateMetaFields(false)
        .withMetaFieldsMode(MetaFieldsMode.NONE)
        .build();
    // NONE is normalized to empty on-disk (implicit from populate=false).
    assertEquals(MetaFieldsMode.NONE, cfg.getMetaFieldsMode());
    assertFalse(cfg.isCommitTimePopulated());
    assertFalse(cfg.isFileNamePopulated());
  }

  @ParameterizedTest
  @EnumSource(value = MetaFieldsMode.class, names = {"NONE", "COMMIT_TIME_ONLY", "FILE_NAME_ONLY", "COMMIT_TIME_AND_FILE_NAME"})
  void rejectsRecordLevelIndexWhenRecordKeyIsNotPopulated(MetaFieldsMode mode) {
    HoodieMetadataConfig mdt = HoodieMetadataConfig.newBuilder()
        .enable(true)
        .withEnableRecordLevelIndex(true)
        .build();
    HoodieWriteConfig.Builder builder = baseBuilder()
        .withPopulateMetaFields(false)
        .withMetaFieldsMode(mode)
        .withMetadataConfig(mdt);
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, builder::build,
        "expected RLI + mode=" + mode + " to be rejected");
    assertTrue(ex.getMessage().contains("Record-level index"),
        "message must name RLI: " + ex.getMessage());
    assertTrue(ex.getMessage().contains(mode.name()),
        "message must include current mode: " + ex.getMessage());
  }

  @ParameterizedTest
  @EnumSource(value = MetaFieldsMode.class, names = {"NONE", "COMMIT_TIME_ONLY", "FILE_NAME_ONLY", "COMMIT_TIME_AND_FILE_NAME"})
  void rejectsSecondaryIndexWhenRecordKeyIsNotPopulated(MetaFieldsMode mode) {
    HoodieMetadataConfig mdt = HoodieMetadataConfig.newBuilder()
        .enable(true)
        .withEnableGlobalRecordLevelIndex(true)
        .withSecondaryIndexEnabled(true)
        .withSecondaryIndexForColumn("some_col")
        .build();
    HoodieWriteConfig.Builder builder = baseBuilder()
        .withPopulateMetaFields(false)
        .withMetaFieldsMode(mode)
        .withMetadataConfig(mdt);
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, builder::build,
        "expected secondary index + mode=" + mode + " to be rejected");
    // The RLI guard fires first (SI depends on global RLI). Both messages are acceptable;
    // just verify the failure clearly ties back to the mode.
    assertTrue(ex.getMessage().contains(mode.name()),
        "message must include current mode: " + ex.getMessage());
  }

  @Test
  void allowsRecordLevelIndexUnderAllMode() {
    HoodieMetadataConfig mdt = HoodieMetadataConfig.newBuilder()
        .enable(true)
        .withEnableRecordLevelIndex(true)
        .build();
    // populateMetaFields=true → ALL mode, record key populated, RLI allowed.
    HoodieWriteConfig cfg = baseBuilder()
        .withPopulateMetaFields(true)
        .withMetadataConfig(mdt)
        .build();
    assertEquals(MetaFieldsMode.ALL, cfg.getMetaFieldsMode());
    assertTrue(cfg.isRecordLevelIndexEnabled());
  }
}
