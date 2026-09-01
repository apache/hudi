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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.MetaFieldsMode;
import org.apache.hudi.common.table.HoodieTableConfig;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
  void modeOverridesContradictingLegacyBoolean() {
    HoodieWriteConfig config = baseBuilder()
        .withPopulateMetaFields(true)
        .withMetaFieldsMode(MetaFieldsMode.COMMIT_TIME_ONLY)
        .build();

    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, config.getMetaFieldsMode());
    assertFalse(config.populateMetaFields());
  }

  @Test
  void restatingTheDerivedBooleanIsAccepted() {
    // Restating what the mode already implies is a coherent, if redundant, request.
    HoodieWriteConfig selective = baseBuilder()
        .withPopulateMetaFields(false)
        .withMetaFieldsMode(MetaFieldsMode.COMMIT_TIME_ONLY)
        .build();
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, selective.getMetaFieldsMode());
    assertFalse(selective.populateMetaFields());

    HoodieWriteConfig all = baseBuilder()
        .withPopulateMetaFields(true)
        .withMetaFieldsMode(MetaFieldsMode.ALL)
        .build();
    assertEquals(MetaFieldsMode.ALL, all.getMetaFieldsMode());
    assertTrue(all.populateMetaFields());
  }

  @Test
  void modePrecedenceIsIndependentOfBuilderOrder() {
    HoodieWriteConfig modeFirst = baseBuilder()
        .withMetaFieldsMode(MetaFieldsMode.COMMIT_TIME_ONLY)
        .withPopulateMetaFields(true)
        .build();
    HoodieWriteConfig modeLast = baseBuilder()
        .withPopulateMetaFields(true)
        .withMetaFieldsMode(MetaFieldsMode.COMMIT_TIME_ONLY)
        .build();

    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, modeFirst.getMetaFieldsMode());
    assertFalse(modeFirst.populateMetaFields());
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, modeLast.getMetaFieldsMode());
    assertFalse(modeLast.populateMetaFields());
  }

  @Test
  void everyModeOverridesItsContradictingBooleanInBothOrders() {
    // Pinned by construction across all five modes rather than by hand-picked cases.
    for (MetaFieldsMode mode : MetaFieldsMode.values()) {
      boolean contradicting = !mode.toLegacyPopulateMetaFields();

      HoodieWriteConfig modeLast =
          baseBuilder().withPopulateMetaFields(contradicting).withMetaFieldsMode(mode).build();
      HoodieWriteConfig modeFirst =
          baseBuilder().withMetaFieldsMode(mode).withPopulateMetaFields(contradicting).build();
      assertEquals(mode, modeLast.getMetaFieldsMode());
      assertEquals(mode.toLegacyPopulateMetaFields(), modeLast.populateMetaFields());
      assertEquals(mode, modeFirst.getMetaFieldsMode());
      assertEquals(mode.toLegacyPopulateMetaFields(), modeFirst.populateMetaFields());

      // And the derived value is what lands when the boolean is left alone.
      HoodieWriteConfig unstated = baseBuilder().withMetaFieldsMode(mode).build();
      assertEquals(Boolean.toString(mode.toLegacyPopulateMetaFields()),
          unstated.getStringOrDefault(HoodieTableConfig.POPULATE_META_FIELDS),
          "the derived boolean must be persisted for " + mode);
      assertEquals(mode, unstated.getMetaFieldsMode());
    }
  }

  @Test
  void unsetModeLeavesTheLegacyBooleanAlone() {
    // The derivation must only fire when a mode is actually set, or it would clobber the legacy
    // boolean for the many callers that never mention the mode at all.
    HoodieWriteConfig cfg = baseBuilder().withPopulateMetaFields(false).build();
    assertEquals("false", cfg.getStringOrDefault(HoodieTableConfig.POPULATE_META_FIELDS));
    assertEquals(MetaFieldsMode.NONE, cfg.getMetaFieldsMode());

    HoodieWriteConfig dflt = baseBuilder().build();
    assertEquals(MetaFieldsMode.ALL, dflt.getMetaFieldsMode());
  }

  @Test
  void explicitAllModeOverridesLegacyFalse() {
    HoodieWriteConfig config = baseBuilder()
        .withPopulateMetaFields(false)
        .withMetaFieldsMode(MetaFieldsMode.ALL)
        .build();

    assertEquals(MetaFieldsMode.ALL, config.getMetaFieldsMode());
    assertTrue(config.populateMetaFields());
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

  /**
   * The builder treats the mode as authoritative regardless of whether it arrived through inherited
   * properties or a direct setter, and rewrites the legacy boolean instead of rejecting the config.
   */
  @Test
  void inheritedModeDoesNotConflictWithAnExplicitBoolean() {
    Properties inherited = new Properties();
    inherited.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.ALL.name());

    HoodieWriteConfig config = baseBuilder()
        .withProperties(inherited)
        .withPopulateMetaFields(false)
        .build();

    assertEquals(MetaFieldsMode.ALL, config.getMetaFieldsMode());
    assertTrue(config.populateMetaFields());
  }

  /**
   * The mirror case: an inherited boolean must not veto an explicitly-stated mode.
   *
   * <p>An upgraded table keeps {@code hoodie.populate.meta.fields=true} on disk alongside the new
   * mode, so a caller stating {@code NONE} on a config that inherited those props would otherwise be
   * rejected in the other direction.
   */
  @Test
  void inheritedBooleanDoesNotConflictWithAnExplicitMode() {
    Properties inherited = new Properties();
    inherited.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "true");

    HoodieWriteConfig config = baseBuilder()
        .withProperties(inherited)
        .withMetaFieldsMode(MetaFieldsMode.NONE)
        .build();

    assertEquals(MetaFieldsMode.NONE, config.getMetaFieldsMode(),
        "an explicitly-stated mode must override an inherited boolean, not throw");
    assertFalse(config.populateMetaFields());
  }

  /**
   * The shape both bootstrap paths use when building the table.
   *
   * <p>{@code BootstrapExecutor} and {@code BootstrapExecutorUtils} read the deprecated boolean out of
   * the props to hand to {@code TableBuilder}. Using the two-argument {@code getBoolean} there returns
   * a primitive, so an unstated boolean arrives as the {@code true} default and contradicts an explicit
   * selective mode -- every bootstrap with {@code hoodie.meta.fields.mode=COMMIT_TIME_ONLY} or
   * {@code NONE} would be rejected at table creation. Passing null when unstated is what lets the mode
   * stand on its own.
   */
  @Test
  void unstatedBooleanFromPropsDoesNotContradictAnExplicitMode() {
    TypedProperties props = new TypedProperties();
    props.put(HoodieTableConfig.META_FIELDS_MODE.key(), MetaFieldsMode.COMMIT_TIME_ONLY.name());

    // What the bootstrap paths now do: null when the caller never mentioned the boolean.
    Boolean populate = props.containsKey(HoodieTableConfig.POPULATE_META_FIELDS.key())
        ? props.getBoolean(HoodieTableConfig.POPULATE_META_FIELDS.key()) : null;
    assertNull(populate, "an unstated boolean must arrive as null, not the true default");

    HoodieWriteConfig config = baseBuilder()
        .withProperties(props)
        .build();
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY, config.getMetaFieldsMode());
    assertFalse(config.populateMetaFields(),
        "COMMIT_TIME_ONLY does not populate the record key, so the derived boolean is false");
  }
}
