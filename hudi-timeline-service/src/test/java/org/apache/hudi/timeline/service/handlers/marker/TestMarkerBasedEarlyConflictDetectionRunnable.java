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

package org.apache.hudi.timeline.service.handlers.marker;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.timeline.service.handlers.MarkerHandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link MarkerBasedEarlyConflictDetectionRunnable}.
 */
@Slf4j
public class TestMarkerBasedEarlyConflictDetectionRunnable extends HoodieCommonTestHarness {

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
  }

  @AfterEach
  public void tearDown() throws Exception {
    Path path = new Path(basePath);
    FileSystem fs = path.getFileSystem(new Configuration());
    fs.delete(path, true);
  }

  /**
   * Prepare dataset :
   * $base_path/.hoodie/.temp/2016/001/MARKERS0 => 2016/b21adfa2-7013-4452-a565-4cc39fea5b73-0_4-17-21_001.parquet.marker.CREATE (same)
   * 2016/4a266542-c7d5-426f-8fb8-fb85a2e88448-0_3-17-20_001.parquet.marker.CREATE
   * /002/MARKERS0 => 2016/b21adfa2-7013-4452-a565-4cc39fea5b73-0_40-170-210_002.parquet.marker.MERGE (same)
   * => 2016/1228caeb-4188-4e19-a18d-848e6f9b0448-0_55-55-425_002.parquet.marker.MERGE
   * <p>
   * <p>
   * Run MarkerBasedEarlyConflictDetectionRunnable and find there is a conflict 2016/b21adfa2-7013-4452-a565-4cc39fea5b73-0
   */
  @Test
  public void testMarkerConflictDetectionRunnable() throws IOException, InterruptedException {

    AtomicBoolean hasConflict = new AtomicBoolean(false);
    HoodieStorage storage = HoodieStorageUtils.getStorage(basePath, getDefaultStorageConf());
    MarkerHandler markerHandler = mock(MarkerHandler.class);
    String rootBaseMarkerDir = basePath + "/.hoodie/.temp";
    String partition = "2016";
    metaClient = HoodieTestUtils.init(
        HoodieTestUtils.getDefaultStorageConf(), basePath, HoodieTableType.COPY_ON_WRITE);

    String oldInstant = "001";
    Set<String> oldMarkers = Stream.of(partition + "/b21adfa2-7013-4452-a565-4cc39fea5b73-0_4-17-21_001.parquet.marker.CREATE",
        partition + "/4a266542-c7d5-426f-8fb8-fb85a2e88448-0_3-17-20_001.parquet.marker.CREATE").collect(Collectors.toSet());
    prepareFiles(rootBaseMarkerDir, oldInstant, oldMarkers, storage);

    // here current markers and old markers have a common fileID b21adfa2-7013-4452-a565-4cc39fea5b73-0
    String currentInstantTime = "002";
    String currentMarkerDir = rootBaseMarkerDir + "/" + currentInstantTime;
    Set<String> currentMarkers = Stream.of(partition + "/b21adfa2-7013-4452-a565-4cc39fea5b73-0_40-170-210_002.parquet.marker.MERGE",
        partition + "/1228caeb-4188-4e19-a18d-848e6f9b0448-0_55-55-425_002.parquet.marker.MERGE").collect(Collectors.toSet());
    prepareFiles(rootBaseMarkerDir, currentInstantTime, currentMarkers, storage);

    HashSet<HoodieInstant> oldInstants = new HashSet<>();
    oldInstants.add(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.COMPLETED, "commit", oldInstant));
    when(markerHandler.getAllMarkers(currentMarkerDir)).thenReturn(currentMarkers);

    ScheduledExecutorService detectorExecutor = Executors.newSingleThreadScheduledExecutor();
    detectorExecutor.submit(new MarkerBasedEarlyConflictDetectionRunnable(hasConflict, markerHandler, currentMarkerDir,
        basePath, storage, Long.MAX_VALUE, oldInstants, true));

    detectorExecutor.shutdown();
    detectorExecutor.awaitTermination(60, TimeUnit.SECONDS);

    assertTrue(hasConflict.get());
  }

  private void prepareFiles(String baseMarkerDir, String instant, Set<String> markers, HoodieStorage storage) throws IOException {
    storage.create(new StoragePath(basePath + "/.hoodie/timeline/" + instant + ".commit"), true);
    String markerDir = baseMarkerDir + "/" + instant;
    storage.createDirectory(new StoragePath(markerDir));
    BufferedWriter out = new BufferedWriter(new FileWriter(markerDir + "/MARKERS0"));
    markers.forEach(ele -> {
      try {
        out.write(ele);
        out.write("\n");
      } catch (IOException e) {
        // ignore here.
      }
    });

    out.close();
  }
}
