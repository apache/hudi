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

package org.apache.hudi.common.model;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.table.HoodieTableConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.model.MetaFieldsMode.ALL;
import static org.apache.hudi.common.model.MetaFieldsMode.COMMIT_TIME_AND_FILE_NAME;
import static org.apache.hudi.common.model.MetaFieldsMode.COMMIT_TIME_ONLY;
import static org.apache.hudi.common.model.MetaFieldsMode.FILE_NAME_ONLY;
import static org.apache.hudi.common.model.MetaFieldsMode.NONE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the {@link MetaFieldsMode} enum itself — its predicates and the {@link
 * MetaFieldsMode#isWiderThan} relation. Resolution from config properties is covered by
 * {@code TestHoodieMetaFieldsMode}, which exercises the same enum through {@code HoodieTableConfig}.
 */
class TestMetaFieldsMode {

  @Test
  void modePropertyHasNoDefaultValue() {
    assertFalse(HoodieTableConfig.META_FIELDS_MODE.hasDefaultValue());
  }

  @Test
  void resolveDefaultsMissingAndEmptyModesToAll() {
    HoodieConfig config = new HoodieConfig();
    assertEquals(ALL, MetaFieldsMode.resolve(config));

    config.setValue(HoodieTableConfig.META_FIELDS_MODE, "");
    assertEquals(ALL, MetaFieldsMode.resolve(config));
  }

  @Test
  void resolveConfigFallsBackToLegacyBooleanWithoutMutation() {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieTableConfig.POPULATE_META_FIELDS, "false");

    assertEquals(NONE, MetaFieldsMode.resolve(config));
    assertFalse(config.contains(HoodieTableConfig.META_FIELDS_MODE));

    config.setValue(HoodieTableConfig.META_FIELDS_MODE, "file_name_only");
    assertEquals(FILE_NAME_ONLY, MetaFieldsMode.resolve(config));
  }

  @Test
  void resolvePropertiesFallsBackToLegacyBooleanWithoutMutation() {
    Properties props = new Properties();
    props.setProperty(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");

    assertEquals(NONE, MetaFieldsMode.resolve(props));
    assertFalse(props.containsKey(HoodieTableConfig.META_FIELDS_MODE.key()));

    props.setProperty(HoodieTableConfig.META_FIELDS_MODE.key(), "commit_time_only");
    assertEquals(COMMIT_TIME_ONLY, MetaFieldsMode.resolve(props));
  }

  @Test
  void resolveMapFallsBackToLegacyBooleanWithoutMutation() {
    Map<String, String> propsMap = new HashMap<>();
    assertEquals(ALL, MetaFieldsMode.resolve(propsMap));

    propsMap.put(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");

    assertEquals(NONE, MetaFieldsMode.resolve(propsMap));
    assertFalse(propsMap.containsKey(HoodieTableConfig.META_FIELDS_MODE.key()));

    propsMap.put(HoodieTableConfig.META_FIELDS_MODE.key(), "commit_time_and_file_name");
    assertEquals(COMMIT_TIME_AND_FILE_NAME, MetaFieldsMode.resolve(propsMap));
  }

  @Test
  void isSelectiveIsTrueForExactlyTheModesTheLegacyBooleanCannotExpress() {
    // ALL and NONE are the two states hoodie.populate.meta.fields can represent; every other mode
    // is invisible to a boolean-only caller, which is what isSelective() exists to flag.
    assertFalse(ALL.isSelective());
    assertFalse(NONE.isSelective());
    assertTrue(COMMIT_TIME_ONLY.isSelective());
    assertTrue(FILE_NAME_ONLY.isSelective());
    assertTrue(COMMIT_TIME_AND_FILE_NAME.isSelective());
  }

  @ParameterizedTest
  @EnumSource(MetaFieldsMode.class)
  void isSelectiveAgreesWithTheLegacyBooleanRoundTrip(MetaFieldsMode mode) {
    // A mode is non-selective exactly when converting it to the legacy boolean and back is lossless.
    MetaFieldsMode roundTripped = mode.toLegacyPopulateMetaFields() ? ALL : NONE;
    assertEquals(!mode.isSelective(), mode == roundTripped);
  }

  @Test
  void toLegacyPopulateMetaFieldsIsTrueOnlyForAll() {
    assertTrue(ALL.toLegacyPopulateMetaFields());
    assertFalse(NONE.toLegacyPopulateMetaFields());
    assertFalse(COMMIT_TIME_ONLY.toLegacyPopulateMetaFields());
    assertFalse(FILE_NAME_ONLY.toLegacyPopulateMetaFields());
    assertFalse(COMMIT_TIME_AND_FILE_NAME.toLegacyPopulateMetaFields());
  }

  @Test
  void toPersistedPopulateMetaFieldsIsFalseOnlyForNone() {
    // The on-disk projection is deliberately wider than the runtime one: a writer that predates
    // hoodie.meta.fields.mode compares this property against its own `true` default and refuses
    // the table on a mismatch without opening a file, so a mode that does populate meta columns
    // must not advertise itself as populating none.
    assertFalse(NONE.toPersistedPopulateMetaFields());
    assertTrue(ALL.toPersistedPopulateMetaFields());
    assertTrue(COMMIT_TIME_ONLY.toPersistedPopulateMetaFields());
    assertTrue(FILE_NAME_ONLY.toPersistedPopulateMetaFields());
    assertTrue(COMMIT_TIME_AND_FILE_NAME.toPersistedPopulateMetaFields());
  }

  @ParameterizedTest
  @EnumSource(MetaFieldsMode.class)
  void persistedBooleanAgreesWithWhetherAnyMetaColumnIsPopulated(MetaFieldsMode mode) {
    assertEquals(mode.isCommitTimePopulated() || mode.isFileNamePopulated(),
        mode.toPersistedPopulateMetaFields());
  }

  @ParameterizedTest
  @EnumSource(MetaFieldsMode.class)
  void persistedBooleanIsNeverNarrowerThanTheRuntimeOne(MetaFieldsMode mode) {
    // Persisted may over-claim relative to runtime -- that is the compatibility shim. It must
    // never under-claim, or a legacy writer is refused a table the runtime happily writes.
    if (mode.toLegacyPopulateMetaFields()) {
      assertTrue(mode.toPersistedPopulateMetaFields());
    }
  }

  @ParameterizedTest
  @EnumSource(MetaFieldsMode.class)
  void runtimeBooleanImpliesTheRecordKeyIsReadableFromTheMetaColumn(MetaFieldsMode mode) {
    // Callers such as HoodieSparkCopyOnWriteTable.getUpdateHandle build a key generator only when
    // this is false; when true they read _hoodie_record_key directly. It must therefore never be
    // true for a mode that leaves that column null on disk.
    assertEquals(mode.isRecordKeyPopulated(), mode.toLegacyPopulateMetaFields());
  }

  @Test
  void recordKeyIsPopulatedOnlyByAll() {
    assertTrue(ALL.isRecordKeyPopulated());
    assertFalse(COMMIT_TIME_AND_FILE_NAME.isRecordKeyPopulated());
    assertFalse(COMMIT_TIME_ONLY.isRecordKeyPopulated());
    assertFalse(FILE_NAME_ONLY.isRecordKeyPopulated());
    assertFalse(NONE.isRecordKeyPopulated());
  }

  @ParameterizedTest
  @CsvSource({
      // Every ordered pair of the five modes: is the first wider than the second?
      "ALL,                       ALL,                       false",
      "ALL,                       NONE,                      true",
      "ALL,                       COMMIT_TIME_ONLY,          true",
      "ALL,                       FILE_NAME_ONLY,            true",
      // ALL populates the record key on top of the same two columns, so it is still wider.
      "ALL,                       COMMIT_TIME_AND_FILE_NAME, true",
      "NONE,                      ALL,                       false",
      "NONE,                      NONE,                      false",
      "NONE,                      COMMIT_TIME_ONLY,          false",
      "NONE,                      FILE_NAME_ONLY,            false",
      "NONE,                      COMMIT_TIME_AND_FILE_NAME, false",
      "COMMIT_TIME_ONLY,          ALL,                       false",
      "COMMIT_TIME_ONLY,          NONE,                      true",
      "COMMIT_TIME_ONLY,          COMMIT_TIME_ONLY,          false",
      // Not a total order: each of these adds a column the other lacks, so both directions are wider.
      "COMMIT_TIME_ONLY,          FILE_NAME_ONLY,            true",
      "COMMIT_TIME_ONLY,          COMMIT_TIME_AND_FILE_NAME, false",
      "FILE_NAME_ONLY,            ALL,                       false",
      "FILE_NAME_ONLY,            NONE,                      true",
      "FILE_NAME_ONLY,            COMMIT_TIME_ONLY,          true",
      "FILE_NAME_ONLY,            FILE_NAME_ONLY,            false",
      "FILE_NAME_ONLY,            COMMIT_TIME_AND_FILE_NAME, false",
      "COMMIT_TIME_AND_FILE_NAME, ALL,                       false",
      "COMMIT_TIME_AND_FILE_NAME, NONE,                      true",
      "COMMIT_TIME_AND_FILE_NAME, COMMIT_TIME_ONLY,          true",
      "COMMIT_TIME_AND_FILE_NAME, FILE_NAME_ONLY,            true",
      "COMMIT_TIME_AND_FILE_NAME, COMMIT_TIME_AND_FILE_NAME, false"
  })
  void isWiderThanCoversEveryOrderedPair(MetaFieldsMode writer, MetaFieldsMode table, boolean wider) {
    assertEquals(wider, writer.isWiderThan(table));
  }

  @Test
  void commitTimeOnlyAndFileNameOnlyAreMutuallyWider() {
    // Pinned separately from the matrix because it is the case that makes "narrowing is allowed"
    // subtle: neither mode is a subset of the other, so a transition either way is a widening.
    assertTrue(COMMIT_TIME_ONLY.isWiderThan(FILE_NAME_ONLY));
    assertTrue(FILE_NAME_ONLY.isWiderThan(COMMIT_TIME_ONLY));
  }

  @ParameterizedTest
  @EnumSource(MetaFieldsMode.class)
  void noModeIsWiderThanItself(MetaFieldsMode mode) {
    assertFalse(mode.isWiderThan(mode));
  }

  @ParameterizedTest
  @EnumSource(MetaFieldsMode.class)
  void isWiderThanTreatsNullAsNotComparable(MetaFieldsMode mode) {
    // A null table mode means "unknown"; callers must not read that as a widening and reject.
    assertFalse(mode.isWiderThan(null));
  }
}
