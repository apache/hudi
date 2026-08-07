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
 * Tests the meta-field-population modes exposed by {@link HoodieTableConfig} via the
 * {@code hoodie.meta.fields.mode} property. The mode is materialized as a {@link MetaFieldsMode}
 * enum resolved from both the legacy {@code hoodie.populate.meta.fields} boolean and the on-disk
 * {@code hoodie.meta.fields.mode} property.
 */
class TestHoodieMetaFieldsMode {

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
  void modeIsIgnoredWhenPopulateMetaFieldsIsTrue() {
    // populate.meta.fields=true always resolves to ALL — the raw mode on disk is not consulted.
    // Writer-side validate() rejects this combination up-front, but the accessor must still report
    // ALL semantics defensively.
    HoodieTableConfig cfg = configOf(true, MetaFieldsMode.COMMIT_TIME_ONLY.name());
    assertEquals(MetaFieldsMode.ALL, cfg.getMetaFieldsMode());
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
  void modeStringIsCaseSensitiveAndTrimmed() {
    // Enum-name form is uppercase-only; whitespace around the value is tolerated.
    HoodieTableConfig cfg = configOf(false, "  COMMIT_TIME_ONLY  ");
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, cfg.getMetaFieldsMode());
  }
}
