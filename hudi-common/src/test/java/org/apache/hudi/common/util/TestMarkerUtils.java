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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.exception.HoodieException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.apache.hudi.common.util.MarkerUtils.MARKER_TYPE_FILENAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestMarkerUtils extends HoodieCommonTestHarness {

  private FileSystem fs;

  @BeforeEach
  public void setup() {
    initPath();
    fs = FSUtils.getFs(basePath, new Configuration());
  }

  @Test
  public void testReadMarkerType() throws IOException {
    // mock markers file
    String markerDir = this.basePath + "/.hoodie/.temp/testReadMarkerType/";
    if (MarkerUtils.doesMarkerTypeFileExist(fs, markerDir)) {
      MarkerUtils.deleteMarkerTypeFile(fs, markerDir);
    }

    try {
      // marker file does not exist
      assertEquals(Option.empty(), MarkerUtils.readMarkerType(fs, markerDir),
          "File does not exist, should be empty");

      // HUDI-6440: Fallback to default Marker Type if the content of marker file is empty
      assertTrue(writeEmptyMarkerTypeToFile(fs, markerDir), "Failed to create empty marker type file");
      assertEquals(Option.empty(), MarkerUtils.readMarkerType(fs, markerDir),
          "File exists but empty, should be empty");

      // marker type is DIRECT
      MarkerUtils.deleteMarkerTypeFile(fs, markerDir);
      MarkerUtils.writeMarkerTypeToFile(MarkerType.DIRECT, fs, markerDir);
      assertEquals(Option.of(MarkerType.DIRECT), MarkerUtils.readMarkerType(fs, markerDir),
          "File exists and contains DIRECT, should be DIRECT");

      // marker type is TIMELINE_SERVER_BASED
      MarkerUtils.deleteMarkerTypeFile(fs, markerDir);
      MarkerUtils.writeMarkerTypeToFile(MarkerType.TIMELINE_SERVER_BASED, fs, markerDir);
      assertEquals(Option.of(MarkerType.TIMELINE_SERVER_BASED), MarkerUtils.readMarkerType(fs, markerDir),
          "File exists and contains TIMELINE_SERVER_BASED, should be TIMELINE_SERVER_BASED");
    } finally {
      MarkerUtils.deleteMarkerTypeFile(fs, markerDir);
    }
  }

  private boolean writeEmptyMarkerTypeToFile(FileSystem fileSystem, String markerDir) {
    Path markerTypeFilePath = new Path(markerDir, MARKER_TYPE_FILENAME);
    try {
      return fileSystem.createNewFile(markerTypeFilePath);
    } catch (IOException e) {
      throw new HoodieException("Failed to create marker type file " + markerTypeFilePath, e);
    }
  }
}
