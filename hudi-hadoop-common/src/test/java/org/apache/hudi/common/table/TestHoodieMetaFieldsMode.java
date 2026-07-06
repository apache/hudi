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

import org.apache.hudi.common.model.HoodieRecord;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the meta-field-population modes exposed by {@link HoodieTableConfig} via the
 * {@code hoodie.meta.fields.mode} property:
 *
 * <ul>
 *   <li>ALL — {@code populate.meta.fields=true} (default). All five meta columns populated.</li>
 *   <li>NONE — {@code populate.meta.fields=false} and mode empty. No meta columns populated.</li>
 *   <li>COMMIT_TIME_ONLY — {@code populate.meta.fields=false},
 *   {@code meta.fields.mode=_hoodie_commit_time}. Only commit-time populated.</li>
 *   <li>FILE_NAME_ONLY — same but with {@code _hoodie_file_name}.</li>
 *   <li>COMMIT_TIME_AND_FILE_NAME — both tokens in the mode list.</li>
 * </ul>
 *
 * <p>Tokens other than the two allowed ones are rejected up-front by the parser. This test
 * exercises {@link HoodieTableConfig} accessors directly without touching the storage layer.
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
    assertTrue(cfg.getMetaFieldsMode().isEmpty(), "mode list defaults to empty");
    assertTrue(cfg.isCommitTimePopulated(), "commit time must be populated in ALL mode");
    assertTrue(cfg.isFileNamePopulated(), "file name must be populated in ALL mode");
    assertTrue(cfg.isRecordKeyPopulated(), "record key must be populated in ALL mode");
  }

  @Test
  void noneModeWhenPopulateFalseAndModeEmpty() {
    HoodieTableConfig cfg = configOf(false, "");
    assertFalse(cfg.populateMetaFields());
    assertTrue(cfg.getMetaFieldsMode().isEmpty());
    assertFalse(cfg.isCommitTimePopulated());
    assertFalse(cfg.isFileNamePopulated());
    assertFalse(cfg.isRecordKeyPopulated());
  }

  @Test
  void noneModeWhenPopulateFalseAndModeUnset() {
    // Existing populate.meta.fields=false table without the mode property must still resolve to NONE.
    HoodieTableConfig cfg = configOf(false, null);
    assertFalse(cfg.populateMetaFields());
    assertTrue(cfg.getMetaFieldsMode().isEmpty());
    assertFalse(cfg.isCommitTimePopulated());
    assertFalse(cfg.isFileNamePopulated());
    assertFalse(cfg.isRecordKeyPopulated());
  }

  @Test
  void commitTimeOnlyMode() {
    HoodieTableConfig cfg = configOf(false, HoodieRecord.COMMIT_TIME_METADATA_FIELD);
    assertFalse(cfg.populateMetaFields());
    assertTrue(cfg.isCommitTimePopulated());
    assertFalse(cfg.isFileNamePopulated());
    assertFalse(cfg.isRecordKeyPopulated());
  }

  @Test
  void fileNameOnlyMode() {
    HoodieTableConfig cfg = configOf(false, HoodieRecord.FILENAME_METADATA_FIELD);
    assertFalse(cfg.populateMetaFields());
    assertFalse(cfg.isCommitTimePopulated());
    assertTrue(cfg.isFileNamePopulated());
    assertFalse(cfg.isRecordKeyPopulated());
  }

  @Test
  void commitTimeAndFileNameMode() {
    HoodieTableConfig cfg = configOf(false,
        HoodieRecord.COMMIT_TIME_METADATA_FIELD + "," + HoodieRecord.FILENAME_METADATA_FIELD);
    assertFalse(cfg.populateMetaFields());
    assertTrue(cfg.isCommitTimePopulated());
    assertTrue(cfg.isFileNamePopulated());
    assertFalse(cfg.isRecordKeyPopulated());
  }

  @Test
  void modeIsIgnoredWhenPopulateMetaFieldsIsTrue() {
    // Note: writer-side validate() rejects this combination, but the accessor must still report
    // ALL semantics defensively if a bad combo ever leaks through (mode has no effect when all
    // meta fields are already populated).
    HoodieTableConfig cfg = configOf(true, HoodieRecord.COMMIT_TIME_METADATA_FIELD);
    assertTrue(cfg.populateMetaFields());
    assertTrue(cfg.isCommitTimePopulated());
    assertTrue(cfg.isFileNamePopulated());
    assertTrue(cfg.isRecordKeyPopulated());
  }

  @Test
  void unknownTokenIsRejected() {
    HoodieTableConfig cfg = configOf(false, HoodieRecord.RECORD_KEY_METADATA_FIELD);
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, cfg::getMetaFieldsMode);
    assertTrue(ex.getMessage().contains(HoodieRecord.RECORD_KEY_METADATA_FIELD),
        "message must name the rejected token: " + ex.getMessage());
    assertTrue(ex.getMessage().contains("populate.meta.fields"),
        "message must recommend populate.meta.fields=true for other columns: " + ex.getMessage());
  }

  @Test
  void whitespaceAndEmptyTokensAreTolerated() {
    HoodieTableConfig cfg = configOf(false, "  " + HoodieRecord.COMMIT_TIME_METADATA_FIELD + " , ");
    assertTrue(cfg.isCommitTimePopulated());
    assertFalse(cfg.isFileNamePopulated());
  }
}
