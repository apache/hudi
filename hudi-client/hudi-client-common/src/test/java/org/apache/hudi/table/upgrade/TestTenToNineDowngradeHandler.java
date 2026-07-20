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
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestTenToNineDowngradeHandler {

  @Test
  void testDowngradeRemovesStorageLayoutOnly() {
    UpgradeDowngrade.TableConfigChangeSet changeSet =
        new TenToNineDowngradeHandler().downgrade(null, null, null, null);

    assertTrue(changeSet.propertiesToUpdate().isEmpty());
    assertEquals(1, changeSet.propertiesToDelete().size());
    assertTrue(changeSet.propertiesToDelete().contains(HoodieTableConfig.TABLE_STORAGE_LAYOUT));
  }

  @Test
  void testTenToNineDowngradeRouteIsSupported() {
    UpgradeDowngrade.TableConfigChangeSet changeSet =
        new UpgradeDowngrade(null, null, null, null)
            .downgrade(HoodieTableVersion.TEN, HoodieTableVersion.NINE, "001");

    assertEquals(1, changeSet.propertiesToDelete().size());
    assertTrue(changeSet.propertiesToDelete().contains(HoodieTableConfig.TABLE_STORAGE_LAYOUT));
  }
}
