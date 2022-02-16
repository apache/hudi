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

package org.apache.hudi.utilities.functional;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.testutils.FunctionalTestHarness;
import org.apache.hudi.utilities.HoodieSnapshotCopier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Disabled("Disable due to flakiness and feature deprecation.")
@Tag("functional")
public class TestHoodieSnapshotCopier extends FunctionalTestHarness {

  private static final String TEST_WRITE_TOKEN = "1-0-1";

  private String basePath;
  private String outputPath;
  private FileSystem fs;

  @BeforeEach
  public void init() throws IOException {
    // Prepare directories
    String rootPath = "file://" + tempDir.toString();
    basePath = rootPath + "/" + HoodieTestUtils.RAW_TRIPS_TEST_NAME;
    outputPath = rootPath + "/output";

    final Configuration hadoopConf = HoodieTestUtils.getDefaultHadoopConf();
    fs = FSUtils.getFs(basePath, hadoopConf);
    HoodieTestUtils.init(hadoopConf, basePath);
  }

  @Test
  public void testEmptySnapshotCopy() throws IOException {
    // There is no real data (only .hoodie directory)
    assertEquals(fs.listStatus(new Path(basePath)).length, 1);
    assertFalse(fs.exists(new Path(outputPath)));

    // Do the snapshot
    HoodieSnapshotCopier copier = new HoodieSnapshotCopier();
    copier.snapshot(jsc(), basePath, outputPath, true,
        HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS);

    // Nothing changed; we just bail out
    assertEquals(fs.listStatus(new Path(basePath)).length, 1);
    assertFalse(fs.exists(new Path(outputPath + "/_SUCCESS")));
  }

  // TODO - uncomment this after fixing test failures
  // @Test
  public void testSnapshotCopy() throws Exception {
    // Generate some commits and corresponding parquets
    String commitTime1 = "20160501010101";
    String commitTime2 = "20160502020601";
    String commitTime3 = "20160506030611";
    new File(basePath + "/.hoodie").mkdirs();
    new File(basePath + "/.hoodie/hoodie.properties").createNewFile();
    // Only first two have commit files
    new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime3 + ".inflight").createNewFile();

    // Some parquet files
    new File(basePath + "/2016/05/01/").mkdirs();
    new File(basePath + "/2016/05/02/").mkdirs();
    new File(basePath + "/2016/05/06/").mkdirs();
    HoodieTestDataGenerator.writePartitionMetadata(fs, new String[] {"2016/05/01", "2016/05/02", "2016/05/06"},
        basePath);
    // Make commit1
    File file11 = new File(basePath + "/2016/05/01/" + FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, "id11"));
    file11.createNewFile();
    File file12 = new File(basePath + "/2016/05/02/" + FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, "id12"));
    file12.createNewFile();
    File file13 = new File(basePath + "/2016/05/06/" + FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, "id13"));
    file13.createNewFile();

    // Make commit2
    File file21 = new File(basePath + "/2016/05/01/" + FSUtils.makeDataFileName(commitTime2, TEST_WRITE_TOKEN, "id21"));
    file21.createNewFile();
    File file22 = new File(basePath + "/2016/05/02/" + FSUtils.makeDataFileName(commitTime2, TEST_WRITE_TOKEN, "id22"));
    file22.createNewFile();
    File file23 = new File(basePath + "/2016/05/06/" + FSUtils.makeDataFileName(commitTime2, TEST_WRITE_TOKEN, "id23"));
    file23.createNewFile();

    // Make commit3
    File file31 = new File(basePath + "/2016/05/01/" + FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, "id31"));
    file31.createNewFile();
    File file32 = new File(basePath + "/2016/05/02/" + FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, "id32"));
    file32.createNewFile();
    File file33 = new File(basePath + "/2016/05/06/" + FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, "id33"));
    file33.createNewFile();

    // Do a snapshot copy
    HoodieSnapshotCopier copier = new HoodieSnapshotCopier();
    copier.snapshot(jsc(), basePath, outputPath, false, HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS);

    // Check results
    assertTrue(fs.exists(new Path(outputPath + "/2016/05/01/" + file11.getName())));
    assertTrue(fs.exists(new Path(outputPath + "/2016/05/02/" + file12.getName())));
    assertTrue(fs.exists(new Path(outputPath + "/2016/05/06/" + file13.getName())));
    assertTrue(fs.exists(new Path(outputPath + "/2016/05/01/" + file21.getName())));
    assertTrue(fs.exists(new Path(outputPath + "/2016/05/02/" + file22.getName())));
    assertTrue(fs.exists(new Path(outputPath + "/2016/05/06/" + file23.getName())));
    assertFalse(fs.exists(new Path(outputPath + "/2016/05/01/" + file31.getName())));
    assertFalse(fs.exists(new Path(outputPath + "/2016/05/02/" + file32.getName())));
    assertFalse(fs.exists(new Path(outputPath + "/2016/05/06/" + file33.getName())));

    assertTrue(fs.exists(new Path(outputPath + "/.hoodie/" + commitTime1 + ".commit")));
    assertTrue(fs.exists(new Path(outputPath + "/.hoodie/" + commitTime2 + ".commit")));
    assertFalse(fs.exists(new Path(outputPath + "/.hoodie/" + commitTime3 + ".commit")));
    assertFalse(fs.exists(new Path(outputPath + "/.hoodie/" + commitTime3 + ".inflight")));
    assertTrue(fs.exists(new Path(outputPath + "/.hoodie/hoodie.properties")));

    assertTrue(fs.exists(new Path(outputPath + "/_SUCCESS")));
  }
}
