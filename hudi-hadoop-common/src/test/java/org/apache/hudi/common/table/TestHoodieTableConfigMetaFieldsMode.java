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
 * {@link HoodieTableConfig} resolution of {@code hoodie.meta.fields.mode} into a
 * {@link MetaFieldsMode}, including the precedence between the mode and the deprecated
 * {@code hoodie.populate.meta.fields} boolean.
 *
 * <p>Deliberately separate from its two siblings, which cover different layers:
 * <ul>
 *   <li>{@code TestMetaFieldsMode} (hudi-common) — the enum's own semantics: {@code parse},
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
    HoodieTableConfig cfg = new HoodieTableConfig();
    if (populate != null) {
      cfg.setValue(HoodieTableConfig.POPULATE_META_FIELDS, String.valueOf(populate));
    }
    if (mode != null) {
      cfg.setValue(HoodieTableConfig.META_FIELDS_MODE, mode);
    }
    return cfg;
  }

  @Test
  void defaultsResolveToAllMode() {
    HoodieTableConfig cfg = configOf(null, null);
    assertTrue(cfg.populateMetaFields(), "populateMetaFields default must remain true");
    assertEquals(MetaFieldsMode.ALL, cfg.getMetaFieldsMode());
    assertTrue(cfg.isCommitTimePopulated());
    assertTrue(cfg.isFileNamePopulated());
    assertTrue(cfg.isRecordKeyPopulated());
  }

  @Test
  void noneModeWhenPopulateFalseAndModeEmpty() {
    HoodieTableConfig cfg = configOf(false, "");
    assertFalse(cfg.populateMetaFields());
    assertEquals(MetaFieldsMode.NONE, cfg.getMetaFieldsMode());
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
    HoodieTableConfig cfg = configOf(false, "GARBAGE_VALUE");
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, cfg::getMetaFieldsMode);
    assertTrue(ex.getMessage().contains("GARBAGE_VALUE"),
        "message must name the rejected value: " + ex.getMessage());
    assertTrue(ex.getMessage().contains("hoodie.meta.fields.mode"),
        "message must name the property: " + ex.getMessage());
  }

  @Test
  void isWiderThanRejectsEveryColumnAddingTransition() {
    // Widening is what the write-client irreversibility guard must reject: any transition that
    // adds a populated meta column, not just the all-or-nothing NONE -> ALL case.
    assertTrue(MetaFieldsMode.COMMIT_TIME_ONLY.isWiderThan(MetaFieldsMode.NONE));
    assertTrue(MetaFieldsMode.FILE_NAME_ONLY.isWiderThan(MetaFieldsMode.NONE));
    assertTrue(MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME.isWiderThan(MetaFieldsMode.COMMIT_TIME_ONLY));
    assertTrue(MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME.isWiderThan(MetaFieldsMode.FILE_NAME_ONLY));
    assertTrue(MetaFieldsMode.ALL.isWiderThan(MetaFieldsMode.NONE));
    assertTrue(MetaFieldsMode.ALL.isWiderThan(MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME));

    // The two selective single-column modes each add a column the other lacks.
    assertTrue(MetaFieldsMode.COMMIT_TIME_ONLY.isWiderThan(MetaFieldsMode.FILE_NAME_ONLY));
    assertTrue(MetaFieldsMode.FILE_NAME_ONLY.isWiderThan(MetaFieldsMode.COMMIT_TIME_ONLY));
  }

  @Test
  void isWiderThanAllowsSameModeAndNarrowing() {
    for (MetaFieldsMode mode : MetaFieldsMode.values()) {
      assertFalse(mode.isWiderThan(mode), mode + " must not be wider than itself");
    }
    // Narrowing drops columns from later commits, which is tolerated.
    assertFalse(MetaFieldsMode.NONE.isWiderThan(MetaFieldsMode.ALL));
    assertFalse(MetaFieldsMode.COMMIT_TIME_ONLY.isWiderThan(MetaFieldsMode.ALL));
    assertFalse(MetaFieldsMode.COMMIT_TIME_ONLY.isWiderThan(MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME));
    // A null on-disk mode carries no information, so nothing is "wider" than it.
    assertFalse(MetaFieldsMode.ALL.isWiderThan(null));
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
