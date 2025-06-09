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

package org.apache.hudi.common.util;

import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.hudi.common.util.MarkerUtils.MARKER_TYPE_FILENAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestMarkerUtils extends HoodieCommonTestHarness {

  private HoodieStorage storage;

  @BeforeEach
  public void setup() {
    initPath();
    storage = HoodieStorageUtils.getStorage(basePath, HoodieTestUtils.getDefaultStorageConfWithDefaults());
  }

  @Test
  public void testReadMarkerType() throws IOException {
    // mock markers file
    String markerDir = this.basePath + "/.hoodie/.temp/testReadMarkerType/";
    if (MarkerUtils.doesMarkerTypeFileExist(storage, new StoragePath(markerDir))) {
      MarkerUtils.deleteMarkerTypeFile(storage, markerDir);
    }

    try {
      // marker file does not exist
      assertEquals(Option.empty(), MarkerUtils.readMarkerType(storage, markerDir),
          "File does not exist, should be empty");

      // HUDI-6440: Fallback to default Marker Type if the content of marker file is empty
      assertTrue(writeEmptyMarkerTypeToFile(storage, markerDir), "Failed to create empty marker type file");
      assertEquals(Option.empty(), MarkerUtils.readMarkerType(storage, markerDir),
          "File exists but empty, should be empty");

      // marker type is DIRECT
      MarkerUtils.deleteMarkerTypeFile(storage, markerDir);
      MarkerUtils.writeMarkerTypeToFile(MarkerType.DIRECT, storage, new StoragePath(markerDir));
      assertEquals(Option.of(MarkerType.DIRECT), MarkerUtils.readMarkerType(storage, markerDir),
          "File exists and contains DIRECT, should be DIRECT");

      // marker type is TIMELINE_SERVER_BASED
      MarkerUtils.deleteMarkerTypeFile(storage, markerDir);
      MarkerUtils.writeMarkerTypeToFile(MarkerType.TIMELINE_SERVER_BASED, storage, new StoragePath(markerDir));
      assertEquals(Option.of(MarkerType.TIMELINE_SERVER_BASED), MarkerUtils.readMarkerType(storage, markerDir),
          "File exists and contains TIMELINE_SERVER_BASED, should be TIMELINE_SERVER_BASED");
    } finally {
      MarkerUtils.deleteMarkerTypeFile(storage, markerDir);
    }
  }

  private boolean writeEmptyMarkerTypeToFile(HoodieStorage storage, String markerDir) {
    StoragePath markerTypeFilePath = new StoragePath(markerDir, MARKER_TYPE_FILENAME);
    try {
      return storage.createNewFile(markerTypeFilePath);
    } catch (IOException e) {
      throw new HoodieException("Failed to create marker type file " + markerTypeFilePath, e);
    }
  }
}
