/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.marker;

import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.apache.hudi.common.util.MarkerUtils.MARKER_TYPE_FILENAME;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

public class TestMarkerBasedRollbackUtils {

  /**
   * Verifies that a transient IO failure (e.g., HDFS "Server too busy") while listing the
   * DIRECT marker directory propagates as IOException rather than silently falling back to
   * TIMELINE_SERVER_BASED markers. The timeline server uses a different directory and would
   * return 0 marker paths, causing rollback to skip deleting data files and leaving orphans.
   */
  @Test
  public void testGetAllMarkerPathsThrowsIOExceptionOnTransientHdfsFailure() throws IOException {
    String instant = "20260101000000";
    String basePath = "/tmp/test-table";
    String markerDir = basePath + "/.hoodie/.temp/" + instant;

    HoodieTable mockTable = Mockito.mock(HoodieTable.class);
    HoodieTableMetaClient mockMetaClient = Mockito.mock(HoodieTableMetaClient.class);
    HoodieEngineContext mockContext = Mockito.mock(HoodieEngineContext.class);
    HoodieWrapperFileSystem mockFs = Mockito.mock(HoodieWrapperFileSystem.class);

    when(mockTable.getMetaClient()).thenReturn(mockMetaClient);
    when(mockTable.getContext()).thenReturn(mockContext);
    when(mockMetaClient.getFs()).thenReturn(mockFs);
    when(mockMetaClient.getBasePath()).thenReturn(basePath);
    when(mockMetaClient.getMarkerFolderPath(instant)).thenReturn(markerDir);
    when(mockContext.getHadoopConf()).thenReturn(new SerializableConfiguration(new Configuration()));

    // MARKERS.type is absent — simulates a write that was interrupted before writing the type file
    when(mockFs.exists(new Path(markerDir, MARKER_TYPE_FILENAME))).thenReturn(false);
    // The marker directory itself exists (the write had started creating marker files)
    when(mockFs.exists(new Path(markerDir))).thenReturn(true);
    // Listing the marker dir fails transiently (e.g., HDFS namenode throttling)
    when(mockFs.listStatus(new Path(markerDir)))
        .thenThrow(new IOException("Server too busy - disconnecting"));

    // IOException must propagate so the rollback can retry once HDFS recovers.
    // If it were swallowed and fell back to TIMELINE_SERVER_BASED, rollback would return
    // 0 marker paths and leave orphan data files on the table.
    assertThrows(IOException.class,
        () -> MarkerBasedRollbackUtils.getAllMarkerPaths(mockTable, mockContext, instant, 1));
  }
}
