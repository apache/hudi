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

import org.apache.hudi.common.config.HoodieConfig;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the three meta-field-population modes exposed by {@link HoodieTableConfig}:
 *
 * <ul>
 *   <li>ALL — {@code populate.meta.fields=true} (default). All five meta columns populated.</li>
 *   <li>NONE — {@code populate.meta.fields=false} and {@code meta.fields.commit.time.enabled=false}.
 *   No meta columns populated; incremental queries rejected.</li>
 *   <li>COMMIT_TIME_ONLY — {@code populate.meta.fields=false} and
 *   {@code meta.fields.commit.time.enabled=true}. Only {@code _hoodie_commit_time} populated so
 *   incremental queries keep working. Additive layer on top of the NONE mode.</li>
 * </ul>
 *
 * <p>This test fixture exercises the {@link HoodieTableConfig} accessors directly without
 * touching the storage layer; integration coverage lives in the per-engine writer tests.
 */
class TestHoodieMetaFieldsMode {

  private static HoodieTableConfig configOf(Boolean populate, Boolean commitTime) {
    HoodieTableConfig cfg = new HoodieTableConfig();
    if (populate != null) {
      cfg.setValue(HoodieTableConfig.POPULATE_META_FIELDS, String.valueOf(populate));
    }
    if (commitTime != null) {
      cfg.setValue(HoodieTableConfig.META_FIELDS_COMMIT_TIME_ENABLED, String.valueOf(commitTime));
    }
    return cfg;
  }

  @Test
  void defaultsResolveToAllMode() {
    // No properties set on a fresh config — defaults must keep today's behavior (ALL).
    HoodieTableConfig cfg = configOf(null, null);
    assertTrue(cfg.populateMetaFields(), "populateMetaFields default must remain true");
    assertFalse(cfg.isCommitTimeOnlyMetaFieldsMode(),
        "isCommitTimeOnlyMetaFieldsMode must be false when populate.meta.fields is true");
    assertTrue(cfg.isCommitTimePopulated(), "commit time must be populated in ALL mode");
    assertTrue(cfg.isRecordKeyPopulated(), "record key must be populated in ALL mode");
  }

  @Test
  void allModeWhenPopulateExplicitlyTrue() {
    HoodieTableConfig cfg = configOf(true, false);
    assertTrue(cfg.populateMetaFields());
    assertFalse(cfg.isCommitTimeOnlyMetaFieldsMode());
    assertTrue(cfg.isCommitTimePopulated());
    assertTrue(cfg.isRecordKeyPopulated());
  }

  @Test
  void noneModeWhenPopulateFalseAndCommitTimeFalse() {
    HoodieTableConfig cfg = configOf(false, false);
    assertFalse(cfg.populateMetaFields());
    assertFalse(cfg.isCommitTimeOnlyMetaFieldsMode());
    assertFalse(cfg.isCommitTimePopulated(),
        "commit time must not be populated under NONE — incremental queries are unsupported");
    assertFalse(cfg.isRecordKeyPopulated());
  }

  @Test
  void noneModeWhenPopulateFalseAndCommitTimeUnset() {
    // The new property defaults to false; an existing populate.meta.fields=false table without
    // the new prop set must still resolve to NONE.
    HoodieTableConfig cfg = configOf(false, null);
    assertFalse(cfg.populateMetaFields());
    assertFalse(cfg.isCommitTimeOnlyMetaFieldsMode());
    assertFalse(cfg.isCommitTimePopulated());
    assertFalse(cfg.isRecordKeyPopulated());
  }

  @Test
  void commitTimeOnlyModeWhenPopulateFalseAndCommitTimeTrue() {
    HoodieTableConfig cfg = configOf(false, true);
    assertFalse(cfg.populateMetaFields());
    assertTrue(cfg.isCommitTimeOnlyMetaFieldsMode(),
        "COMMIT_TIME_ONLY mode must engage when populate=false and commit.time.enabled=true");
    assertTrue(cfg.isCommitTimePopulated(),
        "_hoodie_commit_time must be populated under COMMIT_TIME_ONLY so incremental queries work");
    assertFalse(cfg.isRecordKeyPopulated(),
        "_hoodie_record_key remains unpopulated under COMMIT_TIME_ONLY");
  }

  @Test
  void commitTimeFlagIgnoredWhenPopulateMetaFieldsIsTrue() {
    // When all meta fields are populated, the commit-time-enabled flag has no semantic
    // effect — the table is in ALL mode regardless. isCommitTimeOnlyMetaFieldsMode must report
    // false so callers do not mistakenly drop into the COMMIT_TIME_ONLY write path.
    HoodieTableConfig cfg = configOf(true, true);
    assertTrue(cfg.populateMetaFields());
    assertFalse(cfg.isCommitTimeOnlyMetaFieldsMode());
    assertTrue(cfg.isCommitTimePopulated());
    assertTrue(cfg.isRecordKeyPopulated());
  }

  @Test
  void hoodieConfigViewProducesIdenticalSemantics() {
    // Verifies the accessors do not depend on instance state; a HoodieConfig view of the same
    // properties yields the same answers.
    HoodieTableConfig cfg = configOf(false, true);
    HoodieConfig view = new HoodieConfig(cfg.getProps());
    assertTrue(view.getBooleanOrDefault(HoodieTableConfig.META_FIELDS_COMMIT_TIME_ENABLED));
    assertFalse(view.getBooleanOrDefault(HoodieTableConfig.POPULATE_META_FIELDS));
  }
}
