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

package org.apache.hudi.common.table;

import org.apache.hudi.common.model.MetaFieldsMode;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link HoodieTableConfig} resolution of current and legacy meta-field settings as a
 * {@link MetaFieldsMode}.
 *
 * <p>Deliberately separate from its two siblings, which cover different layers:
 * <ul>
   *   <li>{@code TestMetaFieldsMode} (hudi-common) — the enum's own resolution semantics,
 *       {@code isWiderThan}, {@code toLegacyPopulateMetaFields}. No config involved.</li>
 *   <li>{@code TestHoodieTableConfig#testMetaFieldsModeSurvivesAPropertiesRoundTrip} — the same
 *       resolution against real storage, proving the value survives a {@code hoodie.properties}
 *       write / read.</li>
 * </ul>
 * These cases use an in-memory {@link HoodieTableConfig} instead, so they stay fast and need no
 * storage fixture.
 */
class TestHoodieTableConfigMetaFieldsMode {

  private static HoodieTableConfig configOf(Boolean populate, String mode) {
    HoodieTableConfig config = new HoodieTableConfig();
    if (populate != null) {
      config.setValue(HoodieTableConfig.POPULATE_META_FIELDS, String.valueOf(populate));
    }
    if (mode != null) {
      config.setValue(HoodieTableConfig.META_FIELDS_MODE, mode);
    }
    return config;
  }

  @Test
  void defaultsResolveToAllMode() {
    HoodieTableConfig cfg = configOf(null, null);
    assertTrue(cfg.populateMetaFields(), "populateMetaFields default must remain true");
    assertEquals(MetaFieldsMode.ALL, cfg.getMetaFieldsMode());
    assertFalse(cfg.contains(HoodieTableConfig.META_FIELDS_MODE),
        "resolving a default must not mutate the table config");
    assertTrue(cfg.isCommitTimePopulated());
    assertTrue(cfg.isFileNamePopulated());
    assertTrue(cfg.isRecordKeyPopulated());
  }

  @Test
  void noneModeWhenPopulateFalseAndModeEmpty() {
    HoodieTableConfig cfg = configOf(false, "");
    assertFalse(cfg.populateMetaFields());
    assertEquals(MetaFieldsMode.NONE, cfg.getMetaFieldsMode());
    assertEquals("", cfg.getString(HoodieTableConfig.META_FIELDS_MODE));
    assertFalse(cfg.isCommitTimePopulated());
    assertFalse(cfg.isFileNamePopulated());
    assertFalse(cfg.isRecordKeyPopulated());
  }

  @Test
  void noneModeWhenPopulateFalseAndModeUnset() {
    // Existing populate.meta.fields=false table without the mode property must resolve to NONE.
    HoodieTableConfig cfg = configOf(false, null);
    assertFalse(cfg.populateMetaFields());
    assertEquals(MetaFieldsMode.NONE, cfg.getMetaFieldsMode());
    assertFalse(cfg.contains(HoodieTableConfig.META_FIELDS_MODE),
        "legacy fallback must not mutate the table config");
    assertFalse(cfg.isCommitTimePopulated());
    assertFalse(cfg.isFileNamePopulated());
    assertFalse(cfg.isRecordKeyPopulated());
  }

  @Test
  void commitTimeOnlyMode() {
    HoodieTableConfig cfg = configOf(false, MetaFieldsMode.COMMIT_TIME_ONLY.name());
    assertFalse(cfg.populateMetaFields());
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, cfg.getMetaFieldsMode());
    assertTrue(cfg.isCommitTimePopulated());
    assertFalse(cfg.isFileNamePopulated());
    assertFalse(cfg.isRecordKeyPopulated());
  }

  @Test
  void fileNameOnlyMode() {
    HoodieTableConfig cfg = configOf(false, MetaFieldsMode.FILE_NAME_ONLY.name());
    assertFalse(cfg.populateMetaFields());
    assertEquals(MetaFieldsMode.FILE_NAME_ONLY, cfg.getMetaFieldsMode());
    assertFalse(cfg.isCommitTimePopulated());
    assertTrue(cfg.isFileNamePopulated());
    assertFalse(cfg.isRecordKeyPopulated());
  }

  @Test
  void commitTimeAndFileNameMode() {
    HoodieTableConfig cfg = configOf(false, MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME.name());
    assertFalse(cfg.populateMetaFields());
    assertEquals(MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME, cfg.getMetaFieldsMode());
    assertTrue(cfg.isCommitTimePopulated());
    assertTrue(cfg.isFileNamePopulated());
    assertFalse(cfg.isRecordKeyPopulated());
  }

  @Test
  void modeWinsOverLegacyPopulateMetaFields() {
    // hoodie.meta.fields.mode is the source of truth: when it is set, the deprecated
    // populate.meta.fields boolean is not consulted, whichever way it points.
    HoodieTableConfig cfg = configOf(true, MetaFieldsMode.COMMIT_TIME_ONLY.name());
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, cfg.getMetaFieldsMode());
    assertTrue(cfg.isCommitTimePopulated());
    assertFalse(cfg.isFileNamePopulated());
    // ...and populateMetaFields() is derived from the mode, so legacy call sites agree.
    assertFalse(cfg.populateMetaFields());

    HoodieTableConfig allWithLegacyFalse = configOf(false, MetaFieldsMode.ALL.name());
    assertEquals(MetaFieldsMode.ALL, allWithLegacyFalse.getMetaFieldsMode());
    assertTrue(allWithLegacyFalse.populateMetaFields());
  }

  @Test
  void unknownTokenIsRejected() {
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
        () -> configOf(false, "GARBAGE_VALUE").getMetaFieldsMode());
    assertTrue(ex.getMessage().contains("GARBAGE_VALUE"),
        "message must name the rejected value: " + ex.getMessage());
    assertTrue(ex.getMessage().contains("hoodie.meta.fields.mode"),
        "message must name the property: " + ex.getMessage());
  }

  @Test
  void modeStringIsCaseInsensitiveAndTrimmed() {
    // Whitespace around the value is tolerated, and casing does not have to match the enum.
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, configOf(false, "  COMMIT_TIME_ONLY  ").getMetaFieldsMode());
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, configOf(false, "commit_time_only").getMetaFieldsMode());
    assertEquals(MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME, configOf(false, " Commit_Time_And_File_Name ").getMetaFieldsMode());
    HoodieTableConfig cfg = configOf(false, "CoMmIt_TiMe_OnLy");
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, cfg.getMetaFieldsMode());
  }
}
