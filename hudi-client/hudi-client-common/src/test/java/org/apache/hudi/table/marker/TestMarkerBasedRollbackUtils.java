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

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;

import static org.apache.hudi.common.util.MarkerUtils.MARKER_TYPE_FILENAME;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link MarkerBasedRollbackUtils}.
 *
 * <p>These tests target the {@code MARKERS.type}-absent code path in
 * {@link MarkerBasedRollbackUtils#getAllMarkerPaths}. That branch attempts to list DIRECT
 * markers first and used to catch {@code IOException | IllegalArgumentException} and fall
 * back to TIMELINE_SERVER_BASED. A transient HDFS failure would therefore be swallowed and
 * the rollback would return zero marker paths, leaving orphan data files on the table.
 *
 * <p>The tests mock the {@link HoodieStorage} seams the production code actually goes
 * through:
 * <ul>
 *   <li>{@code readMarkerType(...)} -&gt; {@code storage.exists(MARKERS.type)} = false
 *   <li>{@code DirectWriteMarkers.doesMarkerDirExist()} -&gt; {@code storage.exists(markerDir)} = true
 *   <li>{@code FSUtils.processFiles} -&gt; {@code storage.listDirectEntries(markerDir)} throws
 * </ul>
 */
public class TestMarkerBasedRollbackUtils {

  private static final String INSTANT = "20260101000000";
  private static final String BASE_PATH = "/tmp/test-table";
  private static final String MARKER_DIR = BASE_PATH + "/.hoodie/.temp/" + INSTANT;

  private HoodieTable mockTable;
  private HoodieTableMetaClient mockMetaClient;
  private HoodieEngineContext mockContext;
  private HoodieStorage mockStorage;

  @BeforeEach
  public void setUp() throws IOException {
    mockTable = mock(HoodieTable.class);
    mockMetaClient = mock(HoodieTableMetaClient.class);
    mockContext = mock(HoodieEngineContext.class);
    mockStorage = mock(HoodieStorage.class);
    HoodieTableConfig mockTableConfig = mock(HoodieTableConfig.class);

    when(mockTable.getMetaClient()).thenReturn(mockMetaClient);
    when(mockTable.getContext()).thenReturn(mockContext);
    when(mockTable.getStorage()).thenReturn(mockStorage);
    when(mockMetaClient.getBasePath()).thenReturn(new StoragePath(BASE_PATH));
    when(mockMetaClient.getMarkerFolderPath(INSTANT)).thenReturn(MARKER_DIR);
    when(mockMetaClient.getTableConfig()).thenReturn(mockTableConfig);
    // Table version 8+ selects DirectWriteMarkers in WriteMarkersFactory.
    when(mockTableConfig.getTableVersion()).thenReturn(HoodieTableVersion.EIGHT);

    StoragePath markerDirPath = new StoragePath(MARKER_DIR);
    StoragePath markerTypeFilePath = new StoragePath(markerDirPath, MARKER_TYPE_FILENAME);
    // MARKERS.type is absent, this drives execution into the fallback branch under test.
    when(mockStorage.exists(markerTypeFilePath)).thenReturn(false);
    // The marker directory itself exists so DirectWriteMarkers.allMarkerFilePaths()
    // proceeds to list entries (rather than early-returning an empty set).
    when(mockStorage.exists(markerDirPath)).thenReturn(true);
  }

  /**
   * A transient IO failure while listing DIRECT markers must propagate as IOException
   * rather than silently falling back to TIMELINE_SERVER_BASED. Falling back would return
   * zero marker paths (timeline server uses a different location) and cause rollback to
   * leave orphan data files on the table.
   */
  @Test
  public void testGetAllMarkerPathsPropagatesIOExceptionOnTransientListingFailure() throws IOException {
    when(mockStorage.listDirectEntries(new StoragePath(MARKER_DIR)))
        .thenThrow(new IOException("Server too busy - disconnecting"));

    IOException thrown = assertThrows(IOException.class,
        () -> MarkerBasedRollbackUtils.getAllMarkerPaths(mockTable, mockContext, INSTANT, 1));
    // Verify the original transient error surfaces to the caller (not a wrapped fallback error).
    assertEquals("Server too busy - disconnecting", thrown.getMessage());
  }

  /**
   * IllegalArgumentException (e.g., marker path format mismatch) must retain the original
   * fallback behavior, read markers via TIMELINE_SERVER_BASED path instead of failing.
   *
   * <p>Both DirectWriteMarkers and the timeline-server-based reader end up calling
   * {@code storage.listDirectEntries(markerDir)}. We stub the first invocation to throw
   * (triggering the fallback under test) and subsequent invocations to return an empty
   * list so the fallback path completes cleanly.
   */
  @Test
  public void testGetAllMarkerPathsFallsBackToTimelineServerOnIllegalArgumentException() throws IOException {
    when(mockStorage.listDirectEntries(new StoragePath(MARKER_DIR)))
        .thenThrow(new IllegalArgumentException("bad marker path"))
        .thenReturn(Collections.emptyList());

    // No exception should surface, the IllegalArgumentException must be handled by the
    // fallback rather than propagating to the caller.
    assertDoesNotThrow(
        () -> MarkerBasedRollbackUtils.getAllMarkerPaths(mockTable, mockContext, INSTANT, 1));
  }
}
