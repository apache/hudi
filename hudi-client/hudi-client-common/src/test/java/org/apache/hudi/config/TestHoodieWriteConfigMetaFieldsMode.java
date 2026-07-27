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

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.MetaFieldsMode;
import org.apache.hudi.common.table.HoodieTableConfig;

import org.junit.jupiter.api.Test;

import java.util.Properties;

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

  private static Properties mergeOnReadProps() {
    Properties props = new Properties();
    props.setProperty(HoodieTableConfig.TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    return props;
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
  void modeWinsOverLegacyPopulateMetaFields() {
    // The two properties no longer compose: an explicit mode is the answer regardless of the
    // deprecated boolean, so this combination is accepted rather than rejected.
    HoodieWriteConfig cfg = baseBuilder()
        .withPopulateMetaFields(true)
        .withMetaFieldsMode(MetaFieldsMode.COMMIT_TIME_ONLY)
        .build();
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, cfg.getMetaFieldsMode());
    assertTrue(cfg.isCommitTimePopulated());
    assertFalse(cfg.isFileNamePopulated());
    // populateMetaFields() is derived from the mode — only ALL reports true.
    assertFalse(cfg.populateMetaFields());
    // The raw legacy property is rewritten too, so a config handed to table creation cannot carry
    // a boolean that contradicts the mode.
    assertEquals("false", cfg.getStringOrDefault(HoodieTableConfig.POPULATE_META_FIELDS));
  }

  @Test
  void legacyBooleanSetAfterModeStillResolvesFromMode() {
    // Builder call order must not change the outcome: getMetaFieldsMode() reads the mode property
    // first, so a later withPopulateMetaFields(...) cannot silently widen a selective table.
    HoodieWriteConfig cfg = baseBuilder()
        .withMetaFieldsMode(MetaFieldsMode.COMMIT_TIME_ONLY)
        .withPopulateMetaFields(true)
        .build();
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, cfg.getMetaFieldsMode());
    assertFalse(cfg.populateMetaFields());
  }

  @Test
  void explicitAllModeOverridesLegacyFalse() {
    HoodieWriteConfig cfg = baseBuilder()
        .withPopulateMetaFields(false)
        .withMetaFieldsMode(MetaFieldsMode.ALL)
        .build();
    assertEquals(MetaFieldsMode.ALL, cfg.getMetaFieldsMode());
    assertTrue(cfg.populateMetaFields());
  }

  @Test
  void noneModeWithExplicitBuildIsStillNone() {
    HoodieWriteConfig cfg = baseBuilder()
        .withPopulateMetaFields(false)
        .withMetaFieldsMode(MetaFieldsMode.NONE)
        .build();
    assertEquals(MetaFieldsMode.NONE, cfg.getMetaFieldsMode());
    assertFalse(cfg.isCommitTimePopulated());
    assertFalse(cfg.isFileNamePopulated());
  }

  @Test
  void legacyBooleanIsUsedWhenModeIsAbsent() {
    // Backward compat: tables written before hoodie.meta.fields.mode existed keep their behavior.
    HoodieWriteConfig allCfg = baseBuilder().withPopulateMetaFields(true).build();
    assertEquals(MetaFieldsMode.ALL, allCfg.getMetaFieldsMode());

    HoodieWriteConfig noneCfg = baseBuilder().withPopulateMetaFields(false).build();
    assertEquals(MetaFieldsMode.NONE, noneCfg.getMetaFieldsMode());
  }

  @Test
  void rejectsSelectiveModeOnMergeOnRead() {
    // Selective modes are CoW-only until the MoR log-write path honors them.
    HoodieWriteConfig.Builder builder = baseBuilder()
        .withMetaFieldsMode(MetaFieldsMode.COMMIT_TIME_ONLY)
        .withProps(mergeOnReadProps());
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, builder::build);
    assertTrue(ex.getMessage().contains("hoodie.meta.fields.mode"),
        "exception must name the mode property: " + ex.getMessage());
    assertTrue(ex.getMessage().contains("COPY_ON_WRITE"),
        "exception must explain the CoW-only restriction: " + ex.getMessage());
  }

  @Test
  void allowsAllAndNoneOnMergeOnRead() {
    // Only the selective modes are restricted — the two legacy-equivalent modes stay available.
    assertEquals(MetaFieldsMode.ALL, baseBuilder()
        .withMetaFieldsMode(MetaFieldsMode.ALL)
        .withProps(mergeOnReadProps())
        .build().getMetaFieldsMode());
    assertEquals(MetaFieldsMode.NONE, baseBuilder()
        .withMetaFieldsMode(MetaFieldsMode.NONE)
        .withProps(mergeOnReadProps())
        .build().getMetaFieldsMode());
  }
}
