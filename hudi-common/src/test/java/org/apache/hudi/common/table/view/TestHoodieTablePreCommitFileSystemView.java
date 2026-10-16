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

import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link HoodieTablePreCommitFileSystemView}.
 */
class TestHoodieTablePreCommitFileSystemView {

  @Test
  void testWriteStatWithoutFileIsSkipped() {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getBasePath()).thenReturn(new StoragePath("/base"));
    SyncableFileSystemView committedView = mock(SyncableFileSystemView.class);
    when(committedView.getLatestBaseFiles("p1")).thenReturn(Stream.empty());
    // The failed records of a handle that wrote no file are reported with a stat that has no path.
    HoodieWriteStat failedWriteStat = writeStat("f2", null);
    failedWriteStat.setTotalWriteErrors(1);

    HoodieTablePreCommitFileSystemView view = new HoodieTablePreCommitFileSystemView(metaClient, committedView,
        Arrays.asList(writeStat("f1", "p1/f1_0-1-0_100.parquet"), failedWriteStat), Collections.emptyMap(), "100");

    List<HoodieBaseFile> baseFiles = view.getLatestBaseFiles("p1").collect(Collectors.toList());
    assertEquals(1, baseFiles.size());
    assertEquals("f1", baseFiles.get(0).getFileId());
    assertEquals("/base/p1/f1_0-1-0_100.parquet", baseFiles.get(0).getPath());
  }

  private static HoodieWriteStat writeStat(String fileId, String path) {
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPartitionPath("p1");
    writeStat.setFileId(fileId);
    writeStat.setPath(path);
    return writeStat;
  }
}
