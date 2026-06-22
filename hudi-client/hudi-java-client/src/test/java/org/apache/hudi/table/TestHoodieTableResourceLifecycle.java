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

package org.apache.hudi.table;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that a per-cycle {@link HoodieTable} releases the resources held by its lazily-built
 * {@code FileSystemViewManager}. For a {@code SPILLABLE_DISK} view the file-group store spills to an
 * on-disk {@code BitCaskDiskMap}; {@link HoodieTable#close()} must close the view manager so the disk
 * map is cleaned up immediately instead of lingering until JVM exit.
 */
public class TestHoodieTableResourceLifecycle extends HoodieJavaClientTestHarness {

  private HoodieWriteConfig spillableDiskViewConfig(String spillDir) {
    // maxMemoryForView=0 forces every file-group entry to spill to a BitCaskDiskMap on first put;
    // the metadata table is disabled so the only spillable store is the file-system view's file-group map.
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withEngineType(EngineType.JAVA)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(1, 1)
        .withDeleteParallelism(1)
        .withEmbeddedTimelineServerEnabled(false)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.SPILLABLE_DISK)
            .withBaseStoreDir(spillDir)
            .withMaxMemoryForView(0L)
            .build())
        .build();
  }

  @Test
  public void testCloseReleasesSpillableFileSystemView() throws Exception {
    String spillDir = basePath + "/.view_spill";
    HoodieWriteConfig writeConfig = spillableDiskViewConfig(spillDir);
    try (HoodieJavaWriteClient<?> client = getHoodieWriteClient(writeConfig)) {
      insertFirstBatch(writeConfig, client, "001", "000", 10, HoodieJavaWriteClient::insert, false, true, 10,
          metaClient.getInstantGenerator());
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);

    HoodieTable<?, ?, ?, ?> table = HoodieJavaTable.create(writeConfig, context, metaClient);
    table.getHoodieView().loadAllPartitions();
    assertTrue(countDiskMapDirs(spillDir) > 0,
        "Spilling the file-group view should create an on-disk BitCaskDiskMap directory");

    // The fix: HoodieTable is AutoCloseable and close() releases the view manager -> view -> spillable
    // map -> BitCaskDiskMap, which cleans the on-disk directory immediately.
    table.close();
    assertEquals(0, countDiskMapDirs(spillDir),
        "HoodieTable.close() must release the spillable view's on-disk maps");

    // close() must be idempotent.
    table.close();
    assertEquals(0, countDiskMapDirs(spillDir),
        "Calling HoodieTable.close() twice must be safe");
  }

  private static int countDiskMapDirs(String spillDir) {
    File[] dirs = new File(spillDir).listFiles((dir, name) -> name.startsWith("hudi-"));
    return dirs == null ? 0 : dirs.length;
  }
}
