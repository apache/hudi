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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates the writer-side accessors and validation guard for the three meta-field-population
 * modes (ALL / NONE / COMMIT_TIME_ONLY) on {@link HoodieWriteConfig}. The companion test for the
 * {@link org.apache.hudi.common.table.HoodieTableConfig} accessors lives in {@code
 * TestHoodieMetaFieldsMode}; this test covers the writer-builder surface and the cross-flag
 * validation that runs at {@code build()} time.
 */
class TestHoodieWriteConfigMetaFieldsMode {

  private static HoodieWriteConfig.Builder baseBuilder() {
    return HoodieWriteConfig.newBuilder().withPath("file:///tmp/test_hudi_meta_fields_mode");
  }

  @Test
  void defaultsToAllMode() {
    HoodieWriteConfig cfg = baseBuilder().build();
    assertTrue(cfg.populateMetaFields());
    assertFalse(cfg.isCommitTimeOnlyMetaFieldsMode());
    assertTrue(cfg.isCommitTimePopulated());
  }

  @Test
  void explicitNoneModeBuilds() {
    HoodieWriteConfig cfg = baseBuilder().withPopulateMetaFields(false).build();
    assertFalse(cfg.populateMetaFields());
    assertFalse(cfg.isCommitTimeOnlyMetaFieldsMode());
    assertFalse(cfg.isCommitTimePopulated());
  }

  @Test
  void commitTimeOnlyModeBuildsAndIsAdditiveOverNone() {
    HoodieWriteConfig cfg = baseBuilder()
        .withPopulateMetaFields(false)
        .withMetaFieldsCommitTimeEnabled(true)
        .build();
    assertFalse(cfg.populateMetaFields());
    assertTrue(cfg.isCommitTimeOnlyMetaFieldsMode(),
        "COMMIT_TIME_ONLY must engage when populate.meta.fields=false and commit.time.enabled=true");
    assertTrue(cfg.isCommitTimePopulated(),
        "incremental query semantics depend on _hoodie_commit_time being populated in this mode");
  }

  @Test
  void commitTimeFlagIgnoredWhenPopulateMetaFieldsIsTrueByConstruction() {
    // The validation guard rejects the combination at build() time, so the only way to observe
    // both flags being truthy together is to bypass the builder's validate() — i.e., the
    // accessor must still report ALL semantics rather than COMMIT_TIME_ONLY if the bad combo
    // ever leaks through (defensive correctness).
    HoodieWriteConfig cfg = baseBuilder()
        .withPopulateMetaFields(true)
        // commit.time.enabled defaults to false; setting it false is allowed.
        .withMetaFieldsCommitTimeEnabled(false)
        .build();
    assertTrue(cfg.populateMetaFields());
    assertFalse(cfg.isCommitTimeOnlyMetaFieldsMode());
  }

  @Test
  void rejectsIncompatibleCombination() {
    // populate.meta.fields=true together with the COMMIT_TIME_ONLY opt-in is ambiguous (the new
    // flag has no effect when all meta fields are already populated); reject loudly.
    HoodieWriteConfig.Builder builder = baseBuilder()
        .withPopulateMetaFields(true)
        .withMetaFieldsCommitTimeEnabled(true);
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, builder::build);
    assertTrue(ex.getMessage().contains("hoodie.meta.fields.commit.time.enabled"),
        "exception must name the new property: " + ex.getMessage());
    assertTrue(ex.getMessage().contains("hoodie.populate.meta.fields"),
        "exception must name the legacy property too: " + ex.getMessage());
  }

  @Test
  void noneModeWithCommitTimeFlagFalseExplicitIsStillNone() {
    HoodieWriteConfig cfg = baseBuilder()
        .withPopulateMetaFields(false)
        .withMetaFieldsCommitTimeEnabled(false)
        .build();
    assertEquals(false, cfg.populateMetaFields());
    assertEquals(false, cfg.isCommitTimeOnlyMetaFieldsMode());
    assertEquals(false, cfg.isCommitTimePopulated());
  }
}
