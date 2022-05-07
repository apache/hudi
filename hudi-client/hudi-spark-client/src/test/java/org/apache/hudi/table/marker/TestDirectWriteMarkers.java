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

package org.apache.hudi.table.marker;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.testutils.FileSystemTestUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.testutils.HoodieClientTestUtils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;

public class TestDirectWriteMarkers extends TestWriteMarkersBase {

  @BeforeEach
  public void setup() throws IOException {
    initPath();
    initMetaClient();
    this.jsc = new JavaSparkContext(
        HoodieClientTestUtils.getSparkConfForTest(TestDirectWriteMarkers.class.getName()));
    this.context = new HoodieSparkEngineContext(jsc);
    this.fs = FSUtils.getFs(metaClient.getBasePath(), metaClient.getHadoopConf());
    this.markerFolderPath =  new Path(metaClient.getMarkerFolderPath("000"));
    this.writeMarkers = new DirectWriteMarkers(
        fs, metaClient.getBasePath(), markerFolderPath.toString(), "000");
  }

  @AfterEach
  public void cleanup() {
    jsc.stop();
    context = null;
  }

  @Override
  void verifyMarkersInFileSystem(boolean isTablePartitioned) throws IOException {
    List<FileStatus> markerFiles = FileSystemTestUtils.listRecursive(fs, markerFolderPath)
        .stream().filter(status -> status.getPath().getName().contains(".marker"))
        .sorted().collect(Collectors.toList());
    assertEquals(3, markerFiles.size());
    assertIterableEquals(CollectionUtils.createImmutableList(
            "file:" + markerFolderPath.toString()
                + (isTablePartitioned ? "/2020/06/01" : "") + "/file1.marker.MERGE",
            "file:" + markerFolderPath.toString()
                + (isTablePartitioned ? "/2020/06/02" : "") + "/file2.marker.APPEND",
            "file:" + markerFolderPath.toString()
                + (isTablePartitioned ? "/2020/06/03" : "") + "/file3.marker.CREATE"),
        markerFiles.stream().map(m -> m.getPath().toString()).collect(Collectors.toList())
    );
  }
}
