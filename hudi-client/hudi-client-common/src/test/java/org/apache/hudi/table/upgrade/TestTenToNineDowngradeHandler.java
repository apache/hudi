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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestTenToNineDowngradeHandler {

  @TempDir
  private Path baseDir;

  @Test
  void testDowngradeRemovesStorageLayoutOnly() {
    UpgradeDowngrade.TableConfigChangeSet changeSet =
        new TenToNineDowngradeHandler().downgrade(null, null, null, null);

    assertTrue(changeSet.propertiesToUpdate().isEmpty());
    assertEquals(1, changeSet.propertiesToDelete().size());
    assertTrue(changeSet.propertiesToDelete().contains(HoodieTableConfig.TABLE_STORAGE_LAYOUT));
  }

  @Test
  void testNativeCdcValidationIgnoresNonCdcNativeLogsAndMetaFolder() throws Exception {
    Files.createDirectories(baseDir.resolve("partition"));
    Files.createDirectories(baseDir.resolve(HoodieTableMetaClient.METAFOLDER_NAME).resolve("partition"));
    Files.createFile(baseDir.resolve("partition").resolve("file-1_1-0-1_001_1.log.parquet"));
    Files.createFile(baseDir.resolve("partition").resolve("file-1_1-0-1_001_1.deletes.parquet"));
    Files.createFile(baseDir.resolve(HoodieTableMetaClient.METAFOLDER_NAME).resolve("partition")
        .resolve("file-1_1-0-1_001_1.cdc.parquet"));

    assertDoesNotThrow(() -> TenToNineDowngradeHandler.validateNoNativeCdcLogs(createMetaClient()));
  }

  @Test
  void testNativeCdcValidationFailsWithOffendingPath() throws Exception {
    Files.createDirectories(baseDir.resolve("partition"));
    Path nativeCdcLog = baseDir.resolve("partition").resolve("file-1_1-0-1_001_1.cdc.parquet");
    Files.createFile(nativeCdcLog);

    HoodieUpgradeDowngradeException exception = assertThrows(
        HoodieUpgradeDowngradeException.class,
        () -> TenToNineDowngradeHandler.validateNoNativeCdcLogs(createMetaClient()));

    assertTrue(exception.getMessage().contains(nativeCdcLog.toString()));
  }

  @Test
  void testTenToNineDowngradeRouteIsSupported() {
    UpgradeDowngrade.TableConfigChangeSet changeSet =
        new UpgradeDowngrade(null, null, null, null)
            .downgrade(HoodieTableVersion.TEN, HoodieTableVersion.NINE, "001");

    assertEquals(1, changeSet.propertiesToDelete().size());
    assertTrue(changeSet.propertiesToDelete().contains(HoodieTableConfig.TABLE_STORAGE_LAYOUT));
  }

  private HoodieTableMetaClient createMetaClient() {
    HoodieStorage storage = HoodieStorageUtils.getStorage(getDefaultStorageConf());
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getStorage()).thenReturn(storage);
    when(metaClient.getBasePath()).thenReturn(new StoragePath(baseDir.toString()));
    return metaClient;
  }
}
