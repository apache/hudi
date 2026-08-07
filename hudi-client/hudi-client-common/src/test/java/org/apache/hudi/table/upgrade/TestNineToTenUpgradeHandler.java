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
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Version 9 tables predate {@code hoodie.meta.fields.mode}, so the upgrade records the value
 * derived from the deprecated {@code hoodie.populate.meta.fields} boolean. This makes an upgraded
 * table describe its meta-field layout the same way a freshly created version 10 table does,
 * instead of relying on the legacy fallback at every read.
 */
class TestNineToTenUpgradeHandler {

  private static SupportsUpgradeDowngrade helperFor(MetaFieldsMode resolvedMode) {
    HoodieTable table = mock(HoodieTable.class, RETURNS_DEEP_STUBS);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class, RETURNS_DEEP_STUBS);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(tableConfig.getMetaFieldsMode()).thenReturn(resolvedMode);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(table.getMetaClient()).thenReturn(metaClient);

    SupportsUpgradeDowngrade helper = mock(SupportsUpgradeDowngrade.class);
    when(helper.getTable(org.mockito.ArgumentMatchers.any(HoodieWriteConfig.class),
        org.mockito.ArgumentMatchers.any(HoodieEngineContext.class))).thenReturn(table);
    return helper;
  }

  @ParameterizedTest
  @CsvSource({"ALL", "NONE"})
  void upgradeRecordsTheModeDerivedFromTheLegacyBoolean(String modeName) {
    MetaFieldsMode expected = MetaFieldsMode.valueOf(modeName);
    UpgradeDowngrade.TableConfigChangeSet changeSet = new NineToTenUpgradeHandler().upgrade(
        mock(HoodieWriteConfig.class), mock(HoodieEngineContext.class), "001", helperFor(expected));

    assertTrue(changeSet.propertiesToDelete().isEmpty());
    assertEquals(1, changeSet.propertiesToUpdate().size());
    assertEquals(expected.name(),
        changeSet.propertiesToUpdate().get(HoodieTableConfig.META_FIELDS_MODE));
  }

  @Test
  void upgradeNeitherRewritesNorDeletesTheLegacyBoolean() {
    // Unlike EightToNineUpgradeHandler, which removes the legacy property it translates, the boolean
    // must survive here: POPULATE_META_FIELDS defaults to true, so a table left carrying only the
    // mode resolves to ALL as soon as TenToNineDowngradeHandler deletes it. Assert the absence
    // explicitly on both sides of the change set — the parameterized test above only pins the mode.
    UpgradeDowngrade.TableConfigChangeSet changeSet = new NineToTenUpgradeHandler().upgrade(
        mock(HoodieWriteConfig.class), mock(HoodieEngineContext.class), "001",
        helperFor(MetaFieldsMode.NONE));

    assertFalse(changeSet.propertiesToUpdate().containsKey(HoodieTableConfig.POPULATE_META_FIELDS));
    assertFalse(changeSet.propertiesToDelete().contains(HoodieTableConfig.POPULATE_META_FIELDS));
  }
}
