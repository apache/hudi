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

package org.apache.hudi.common.table.view;

import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadata;

import java.io.IOException;
import java.nio.file.Files;

/**
 * Tests RocksDB based file system view {@link RocksDbBasedFileSystemView}.
 */
public class TestRocksDbBasedFileSystemView extends TestHoodieTableFileSystemView {

  @Override
  protected SyncableFileSystemView getFileSystemView(HoodieTimeline timeline) throws IOException {
    String subdirPath = Files.createTempDirectory(tempDir, null).toAbsolutePath().toString();
    HoodieTableMetadata tableMetadata = new FileSystemBackedTableMetadata(getEngineContext(), metaClient.getTableConfig(), metaClient.getStorage(),
        metaClient.getBasePath().toString());
    return new RocksDbBasedFileSystemView(tableMetadata, metaClient, timeline,
        FileSystemViewStorageConfig.newBuilder().withRocksDBPath(subdirPath).build());
  }
}
