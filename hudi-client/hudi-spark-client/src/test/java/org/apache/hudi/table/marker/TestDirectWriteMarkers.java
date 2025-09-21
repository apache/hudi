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
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestUtils;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDirectWriteMarkers extends TestWriteMarkersBase {

  @BeforeEach
  public void setup() throws IOException {
    initPath();
    initMetaClient();
    this.jsc = new JavaSparkContext(
        HoodieClientTestUtils.getSparkConfForTest(TestDirectWriteMarkers.class.getName()));
    this.context = new HoodieSparkEngineContext(jsc);
    this.storage = metaClient.getStorage();
    this.markerFolderPath = new StoragePath(Paths.get(metaClient.getMarkerFolderPath("000")).toUri());
    this.writeMarkers = new DirectWriteMarkers(
        storage, metaClient.getBasePath().toString(), markerFolderPath.toString(), "000");
  }

  @AfterEach
  public void cleanup() {
    jsc.stop();
    context = null;
  }

  @Override
  void verifyMarkersInFileSystem(boolean isTablePartitioned) throws IOException {
    List<StoragePathInfo> markerFiles = HoodieTestTable.listRecursive(storage, markerFolderPath)
        .stream().filter(status -> status.getPath().getName().contains(".marker"))
        .sorted().collect(Collectors.toList());
    List<String> expectedList = getRelativeMarkerPathList(isTablePartitioned)
        .stream().map(e -> markerFolderPath.toString() + "/" + e)
        .collect(Collectors.toList());
    assertIterableEquals(
        expectedList,
        markerFiles.stream().map(m -> m.getPath().toString()).collect(Collectors.toList())
    );
  }

  @Test
  public void testMarkerReconciliation() throws IOException {
    // create couple of files which exists only in markers, but not on storage.
    initMetaClient();

    // create marker files
    createSomeMarkers(true);
    // add 2 data files, out of which 1 is expected to be deleted during reconciliation.
    String fileName1 = "file5.parquet";
    String partitionPathToTest = "2020/06/01";
    StoragePath dataFile1 = createDataFile("2020/06/01", fileName1);
    writeMarkers.create("2020/06/01", fileName1, IOType.CREATE);

    String fileName2 = "file6.parquet";
    StoragePath dataFile2 = createDataFile("2020/06/01", fileName2);
    writeMarkers.create("2020/06/01", fileName2, IOType.CREATE);

    // create HoodieWriteStats
    List<String> expectedMarkerPaths = new ArrayList<>(getRelativeMarkerPathList(true));
    List<String> expectedDataPaths = new ArrayList<>(expectedMarkerPaths.stream().map(entry ->
        entry.substring(0, entry.indexOf(".marker"))).collect(Collectors.toList()));
    // only add file1 and skip file2. Hence we expect file2 to be deleted during reconciliation.
    expectedDataPaths.add(partitionPathToTest + "/" + fileName1);

    List<HoodieWriteStat> writeStatList = new ArrayList<>();
    expectedDataPaths.forEach(entry -> {
      String fullPath = entry;
      String fileName = fullPath.substring(fullPath.lastIndexOf("/") + 1);
      String partitionPath = fullPath.substring(0, fullPath.lastIndexOf("/"));
      HoodieWriteStat writeStat = new HoodieWriteStat();
      writeStat.setPath(partitionPath + "/" + fileName);
      writeStatList.add(writeStat);
    });

    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath(metaClient.getBasePath()).withMarkersType(MarkerType.DIRECT.name()).build();

    HoodieTable hoodieTable = HoodieSparkTable.createForReads(writeConfig, context, metaClient);
    hoodieTable.finalizeWrite(context, "000", writeStatList); // data file 2 should have been deleted.
    assertTrue(storage.exists(dataFile1));
    // file 2 is expected to be deleted.
    assertTrue(!storage.exists(dataFile2));
  }

  @Test
  public void testFailureToDeleteDuringReconciliation() {

  }
}
