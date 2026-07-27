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

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestTenToNineDowngradeHandler {

  @Test
  void testDowngradeRemovesStorageLayoutAndMetaFieldsMode() {
    UpgradeDowngrade.TableConfigChangeSet changeSet =
        new TenToNineDowngradeHandler().downgrade(null, null, null, null);

    assertTrue(changeSet.propertiesToUpdate().isEmpty());
    assertEquals(2, changeSet.propertiesToDelete().size());
    assertTrue(changeSet.propertiesToDelete().contains(HoodieTableConfig.TABLE_STORAGE_LAYOUT));
    // Version 9 does not understand hoodie.meta.fields.mode, so it is dropped...
    assertTrue(changeSet.propertiesToDelete().contains(HoodieTableConfig.META_FIELDS_MODE));
    // ...while hoodie.populate.meta.fields is deliberately left in place, so ALL and NONE tables
    // round-trip unchanged — those are exactly the two states the legacy boolean can express.
    assertFalse(changeSet.propertiesToDelete().contains(HoodieTableConfig.POPULATE_META_FIELDS));
    assertFalse(changeSet.propertiesToUpdate().containsKey(HoodieTableConfig.POPULATE_META_FIELDS));
  }

  @Test
  void testTenToNineDowngradeRouteIsSupported() {
    UpgradeDowngrade.TableConfigChangeSet changeSet =
        new UpgradeDowngrade(null, null, null, null)
            .downgrade(HoodieTableVersion.TEN, HoodieTableVersion.NINE, "001");

    assertEquals(2, changeSet.propertiesToDelete().size());
    assertTrue(changeSet.propertiesToDelete().contains(HoodieTableConfig.TABLE_STORAGE_LAYOUT));
    assertTrue(changeSet.propertiesToDelete().contains(HoodieTableConfig.META_FIELDS_MODE));
  }
}
