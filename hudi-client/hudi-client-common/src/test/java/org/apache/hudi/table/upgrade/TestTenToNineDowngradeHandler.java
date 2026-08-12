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

package org.apache.hudi.table.upgrade;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.MetaFieldsMode;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Version 9 cannot interpret {@code hoodie.meta.fields.mode}, so the downgrade deletes it and writes
 * {@code hoodie.populate.meta.fields} back from it. The write-back is the load-bearing part:
 * {@code POPULATE_META_FIELDS} defaults to {@code true}, so deleting the mode without restating the
 * boolean would silently downgrade the table to {@code ALL}.
 *
 * <p>Every mode downgrades, including the selective ones: they already persist the boolean as
 * {@code false}, so restating it is exactly what version 9 should see. The table under-claims --
 * files keep their populated meta columns while the table advertises none -- which is the safe
 * direction. It is not reversible, and that is asserted here rather than left implicit.
 */
class TestTenToNineDowngradeHandler {

  private static SupportsUpgradeDowngrade helperFor(MetaFieldsMode resolvedMode) {
    HoodieTable table = mock(HoodieTable.class, RETURNS_DEEP_STUBS);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class, RETURNS_DEEP_STUBS);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getMetaFieldsMode()).thenReturn(resolvedMode);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(table.getMetaClient()).thenReturn(metaClient);

    SupportsUpgradeDowngrade helper = mock(SupportsUpgradeDowngrade.class);
    when(helper.getTable(any(HoodieWriteConfig.class), any(HoodieEngineContext.class))).thenReturn(table);
    return helper;
  }

  private static UpgradeDowngrade.TableConfigChangeSet downgradeWith(MetaFieldsMode tableMode) {
    return downgradeWith(tableMode, null);
  }

  /**
   * @param tableMode  what the table config reports
   * @param statedMode what the writer explicitly restated, or null for the ordinary unstated case
   */
  private static UpgradeDowngrade.TableConfigChangeSet downgradeWith(MetaFieldsMode tableMode,
                                                                    MetaFieldsMode statedMode) {
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    when(writeConfig.contains(HoodieTableConfig.META_FIELDS_MODE)).thenReturn(statedMode != null);
    when(writeConfig.getString(HoodieTableConfig.META_FIELDS_MODE))
        .thenReturn(statedMode == null ? null : statedMode.name());
    return new TenToNineDowngradeHandler().downgrade(
        writeConfig, mock(HoodieEngineContext.class), "001", helperFor(tableMode));
  }

  @ParameterizedTest
  @CsvSource({
      // Only ALL populates every meta column, so it is the only mode that maps back to true.
      "ALL,                       true",
      "NONE,                      false",
  })
  void downgradeWritesTheLegacyBooleanDerivedFromTheMode(String modeName, boolean expectedBoolean) {
    UpgradeDowngrade.TableConfigChangeSet changeSet =
        downgradeWith(MetaFieldsMode.valueOf(modeName));

    assertEquals(String.valueOf(expectedBoolean),
        changeSet.propertiesToUpdate().get(HoodieTableConfig.POPULATE_META_FIELDS),
        "version 9 only understands the boolean, so it must be restated from the mode");
    // The mode itself goes away; the boolean must not, or the table resolves to the `true` default.
    assertTrue(changeSet.propertiesToDelete().contains(HoodieTableConfig.META_FIELDS_MODE));
    assertFalse(changeSet.propertiesToDelete().contains(HoodieTableConfig.POPULATE_META_FIELDS));
  }

  @ParameterizedTest
  @CsvSource({"COMMIT_TIME_ONLY", "FILE_NAME_ONLY", "COMMIT_TIME_AND_FILE_NAME"})
  void downgradeRejectsASelectiveModeTheWriterDidNotRestate(String modeName) {
    // The ordinary case: nobody restates meta-field settings, so a selective table would be silently
    // collapsed to NONE -- and irreversibly, since a re-upgrade derives the mode from the boolean.
    // Require the operator to say so rather than making that call for them.
    MetaFieldsMode mode = MetaFieldsMode.valueOf(modeName);

    HoodieUpgradeDowngradeException thrown =
        assertThrows(HoodieUpgradeDowngradeException.class, () -> downgradeWith(mode));

    assertTrue(thrown.getMessage().contains(modeName),
        "the operator needs to know which mode blocked the downgrade, got: " + thrown.getMessage());
    assertTrue(thrown.getMessage().contains(HoodieTableConfig.META_FIELDS_MODE.key()));
  }

  @ParameterizedTest
  @CsvSource({"COMMIT_TIME_ONLY", "FILE_NAME_ONLY", "COMMIT_TIME_AND_FILE_NAME"})
  void downgradeRetainsASelectiveModeTheWriterRestated(String modeName) {
    // Restating the mode is the operator asserting that every reader of this table honors it. The
    // mode is then kept on the downgraded table -- which is also what makes the round trip lossless.
    MetaFieldsMode mode = MetaFieldsMode.valueOf(modeName);

    UpgradeDowngrade.TableConfigChangeSet changeSet = downgradeWith(mode, mode);

    assertFalse(changeSet.propertiesToDelete().contains(HoodieTableConfig.META_FIELDS_MODE),
        "an explicitly restated selective mode must be retained, not dropped");
    assertEquals("false", changeSet.propertiesToUpdate().get(HoodieTableConfig.POPULATE_META_FIELDS),
        "the boolean is still derived from the mode, so a reader ignoring the mode under-claims");
  }

  @Test
  void downgradeRejectsARestatedModeThatDisagreesWithTheTable() {
    // Restating a *different* mode is not consent for this table -- it asserts something untrue
    // about it, so it is treated the same as not restating at all.
    assertThrows(HoodieUpgradeDowngradeException.class,
        () -> downgradeWith(MetaFieldsMode.COMMIT_TIME_ONLY, MetaFieldsMode.FILE_NAME_ONLY));
  }

  @Test
  void retainedSelectiveModeSurvivesAReUpgrade() {
    // The point of retaining it: a later re-upgrade finds the mode intact rather than deriving NONE
    // from the boolean. This is what the earlier drop-and-warn behaviour could not offer.
    UpgradeDowngrade.TableConfigChangeSet down =
        downgradeWith(MetaFieldsMode.COMMIT_TIME_ONLY, MetaFieldsMode.COMMIT_TIME_ONLY);
    assertFalse(down.propertiesToDelete().contains(HoodieTableConfig.META_FIELDS_MODE));

    UpgradeDowngrade.TableConfigChangeSet up = new NineToTenUpgradeHandler().upgrade(
        mock(HoodieWriteConfig.class), mock(HoodieEngineContext.class), "001",
        helperFor(MetaFieldsMode.COMMIT_TIME_ONLY));
    assertEquals(MetaFieldsMode.COMMIT_TIME_ONLY.name(),
        up.propertiesToUpdate().get(HoodieTableConfig.META_FIELDS_MODE),
        "the retained mode must still be COMMIT_TIME_ONLY after upgrading again");
  }

  @Test
  void downgradeRemovesStorageLayoutAndMetaFieldsMode() {
    UpgradeDowngrade.TableConfigChangeSet changeSet = downgradeWith(MetaFieldsMode.ALL);

    assertEquals(2, changeSet.propertiesToDelete().size());
    assertTrue(changeSet.propertiesToDelete().contains(HoodieTableConfig.TABLE_STORAGE_LAYOUT));
    assertTrue(changeSet.propertiesToDelete().contains(HoodieTableConfig.META_FIELDS_MODE));
  }

  @Test
  void downgradeWithoutHelperLeavesTheLegacyBooleanUntouched() {
    // Some callers drive the change set without a helper, so the table config is unreachable and the
    // mode cannot be read. Deleting the mode is still right, but the boolean must not be guessed —
    // writing a value derived from an assumed mode is how a table ends up claiming absent columns.
    UpgradeDowngrade.TableConfigChangeSet changeSet =
        new TenToNineDowngradeHandler().downgrade(null, null, null, null);

    assertTrue(changeSet.propertiesToUpdate().isEmpty());
    assertTrue(changeSet.propertiesToDelete().contains(HoodieTableConfig.META_FIELDS_MODE));
    assertFalse(changeSet.propertiesToDelete().contains(HoodieTableConfig.POPULATE_META_FIELDS));
  }

  @Test
  void tenToNineDowngradeRouteIsSupported() {
    UpgradeDowngrade.TableConfigChangeSet changeSet =
        new UpgradeDowngrade(null, null, null, null)
            .downgrade(HoodieTableVersion.TEN, HoodieTableVersion.NINE, "001");

    assertEquals(2, changeSet.propertiesToDelete().size());
    assertTrue(changeSet.propertiesToDelete().contains(HoodieTableConfig.TABLE_STORAGE_LAYOUT));
    assertTrue(changeSet.propertiesToDelete().contains(HoodieTableConfig.META_FIELDS_MODE));
  }

  @ParameterizedTest
  @CsvSource({"ALL", "NONE"})
  void upgradeDowngradeRoundTripIsLosslessForNonSelectiveModes(String modeName) {
    MetaFieldsMode mode = MetaFieldsMode.valueOf(modeName);

    UpgradeDowngrade.TableConfigChangeSet up = new NineToTenUpgradeHandler().upgrade(
        mock(HoodieWriteConfig.class), mock(HoodieEngineContext.class), "001", helperFor(mode));
    UpgradeDowngrade.TableConfigChangeSet down = downgradeWith(mode);

    // Up records the mode and leaves the boolean; down restores the boolean and drops the mode. A
    // v9 table therefore reads back the same meta-field layout it started with.
    assertEquals(mode.name(), up.propertiesToUpdate().get(HoodieTableConfig.META_FIELDS_MODE));
    assertEquals(String.valueOf(mode.toLegacyPopulateMetaFields()),
        down.propertiesToUpdate().get(HoodieTableConfig.POPULATE_META_FIELDS));
  }
}
