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
    return helperFor(resolvedMode, HoodieTableVersion.SIX);
  }

  private static SupportsUpgradeDowngrade helperFor(MetaFieldsMode resolvedMode,
                                                    HoodieTableVersion initialVersion) {
    HoodieTable table = mock(HoodieTable.class, RETURNS_DEEP_STUBS);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class, RETURNS_DEEP_STUBS);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getMetaFieldsMode()).thenReturn(resolvedMode);
    when(tableConfig.getTableInitialVersion()).thenReturn(initialVersion);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(table.getMetaClient()).thenReturn(metaClient);

    SupportsUpgradeDowngrade helper = mock(SupportsUpgradeDowngrade.class);
    when(helper.getTable(any(HoodieWriteConfig.class), any(HoodieEngineContext.class))).thenReturn(table);
    return helper;
  }

  private static UpgradeDowngrade.TableConfigChangeSet downgradeWith(MetaFieldsMode tableMode) {
    return downgradeWith(tableMode, HoodieTableVersion.SIX, false);
  }

  /**
   * @param tableMode      what the table config reports
   * @param initialVersion the version the table was created at
   * @param allowRetention whether the operator opted in to retaining a selective mode
   */
  private static UpgradeDowngrade.TableConfigChangeSet downgradeWith(MetaFieldsMode tableMode,
                                                                    HoodieTableVersion initialVersion,
                                                                    boolean allowRetention) {
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    when(writeConfig.getBooleanOrDefault(
        HoodieWriteConfig.ALLOW_META_FIELDS_MODE_RETENTION_ON_DOWNGRADE)).thenReturn(allowRetention);
    return new TenToNineDowngradeHandler().downgrade(
        writeConfig, mock(HoodieEngineContext.class), "001", helperFor(tableMode, initialVersion));
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
  void downgradeRejectsASelectiveModeOnATableBornAtTenWithoutOptIn(String modeName) {
    // Created at version 10, so no version that predates the mode has ever read this table. Dropping
    // the mode would collapse it to NONE irreversibly; retaining it leaves a property version 9 does
    // not formally understand. Neither is ours to pick silently.
    MetaFieldsMode mode = MetaFieldsMode.valueOf(modeName);
    HoodieUpgradeDowngradeException ex = assertThrows(HoodieUpgradeDowngradeException.class,
        () -> downgradeWith(mode, HoodieTableVersion.TEN, false));
    assertTrue(ex.getMessage().contains(
        HoodieWriteConfig.ALLOW_META_FIELDS_MODE_RETENTION_ON_DOWNGRADE.key()),
        "the message must name the opt-in flag: " + ex.getMessage());
  }

  @ParameterizedTest
  @CsvSource({"COMMIT_TIME_ONLY", "FILE_NAME_ONLY", "COMMIT_TIME_AND_FILE_NAME"})
  void downgradeRetainsASelectiveModeOnATableThatPredatesTen(String modeName) {
    // Created before version 10, so it existed under a version with no mode property and is simply
    // being returned to a state it has already lived in. No opt-in required.
    MetaFieldsMode mode = MetaFieldsMode.valueOf(modeName);
    UpgradeDowngrade.TableConfigChangeSet changeSet =
        downgradeWith(mode, HoodieTableVersion.SIX, false);

    assertFalse(changeSet.propertiesToDelete().contains(HoodieTableConfig.META_FIELDS_MODE),
        "a table that predates version 10 keeps its mode through a downgrade");
    assertEquals("false",
        changeSet.propertiesToUpdate().get(HoodieTableConfig.POPULATE_META_FIELDS),
        "the legacy boolean is still written so an unaware reader under-claims rather than over-claims");
  }

  @ParameterizedTest
  @CsvSource({"COMMIT_TIME_ONLY", "FILE_NAME_ONLY", "COMMIT_TIME_AND_FILE_NAME"})
  void downgradeRetainsASelectiveModeBornAtTenWhenTheOperatorOptsIn(String modeName) {
    MetaFieldsMode mode = MetaFieldsMode.valueOf(modeName);
    UpgradeDowngrade.TableConfigChangeSet changeSet =
        downgradeWith(mode, HoodieTableVersion.TEN, true);

    assertFalse(changeSet.propertiesToDelete().contains(HoodieTableConfig.META_FIELDS_MODE));
    assertEquals("false",
        changeSet.propertiesToUpdate().get(HoodieTableConfig.POPULATE_META_FIELDS));
  }

  @Test
  void retainedSelectiveModeSurvivesAReUpgrade() {
    // The point of retaining it: a later re-upgrade finds the mode intact rather than deriving NONE
    // from the boolean. This is what the earlier drop-and-warn behaviour could not offer.
    UpgradeDowngrade.TableConfigChangeSet down =
        downgradeWith(MetaFieldsMode.COMMIT_TIME_ONLY, HoodieTableVersion.SIX, false);
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

    // Up records the mode and removes the boolean; down restores the boolean and drops the mode. A
    // v9 table therefore reads back the same meta-field layout it started with.
    assertEquals(mode.name(), up.propertiesToUpdate().get(HoodieTableConfig.META_FIELDS_MODE));
    assertEquals(String.valueOf(mode.toLegacyPopulateMetaFields()),
        down.propertiesToUpdate().get(HoodieTableConfig.POPULATE_META_FIELDS));
  }
}
