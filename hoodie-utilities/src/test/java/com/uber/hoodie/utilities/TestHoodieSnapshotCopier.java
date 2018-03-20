/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.utilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.util.FSUtils;
import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestHoodieSnapshotCopier {

  private String rootPath = null;
  private String basePath = null;
  private String outputPath = null;
  private FileSystem fs = null;
  private JavaSparkContext jsc = null;

  @Before
  public void init() throws IOException {
    try {
      // Prepare directories
      TemporaryFolder folder = new TemporaryFolder();
      folder.create();
      rootPath = "file://" + folder.getRoot().getAbsolutePath();
      basePath = rootPath + "/" + HoodieTestUtils.RAW_TRIPS_TEST_NAME;
      outputPath = rootPath + "/output";

      final Configuration hadoopConf = HoodieTestUtils.getDefaultHadoopConf();
      fs = FSUtils.getFs(basePath, hadoopConf);
      HoodieTestUtils.init(hadoopConf, basePath);
      // Start a local Spark job
      SparkConf conf = new SparkConf().setAppName("snapshot-test-job").setMaster("local[2]");
      jsc = new JavaSparkContext(conf);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testEmptySnapshotCopy() throws IOException {
    // There is no real data (only .hoodie directory)
    assertEquals(fs.listStatus(new Path(basePath)).length, 1);
    assertFalse(fs.exists(new Path(outputPath)));

    // Do the snapshot
    HoodieSnapshotCopier copier = new HoodieSnapshotCopier();
    copier.snapshot(jsc, basePath, outputPath, true);

    // Nothing changed; we just bail out
    assertEquals(fs.listStatus(new Path(basePath)).length, 1);
    assertFalse(fs.exists(new Path(outputPath + "/_SUCCESS")));
  }

  //TODO - uncomment this after fixing test failures
  //@Test
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
    HoodieTestDataGenerator
        .writePartitionMetadata(fs, new String[] {"2016/05/01", "2016/05/02", "2016/05/06"},
            basePath);
    // Make commit1
    File file11 = new File(
        basePath + "/2016/05/01/" + FSUtils.makeDataFileName(commitTime1, 1, "id11"));
    file11.createNewFile();
    File file12 = new File(
        basePath + "/2016/05/02/" + FSUtils.makeDataFileName(commitTime1, 1, "id12"));
    file12.createNewFile();
    File file13 = new File(
        basePath + "/2016/05/06/" + FSUtils.makeDataFileName(commitTime1, 1, "id13"));
    file13.createNewFile();

    // Make commit2
    File file21 = new File(
        basePath + "/2016/05/01/" + FSUtils.makeDataFileName(commitTime2, 1, "id21"));
    file21.createNewFile();
    File file22 = new File(
        basePath + "/2016/05/02/" + FSUtils.makeDataFileName(commitTime2, 1, "id22"));
    file22.createNewFile();
    File file23 = new File(
        basePath + "/2016/05/06/" + FSUtils.makeDataFileName(commitTime2, 1, "id23"));
    file23.createNewFile();

    // Make commit3
    File file31 = new File(
        basePath + "/2016/05/01/" + FSUtils.makeDataFileName(commitTime3, 1, "id31"));
    file31.createNewFile();
    File file32 = new File(
        basePath + "/2016/05/02/" + FSUtils.makeDataFileName(commitTime3, 1, "id32"));
    file32.createNewFile();
    File file33 = new File(
        basePath + "/2016/05/06/" + FSUtils.makeDataFileName(commitTime3, 1, "id33"));
    file33.createNewFile();

    // Do a snapshot copy
    HoodieSnapshotCopier copier = new HoodieSnapshotCopier();
    copier.snapshot(jsc, basePath, outputPath, false);

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

  @After
  public void cleanup() {
    if (rootPath != null) {
      new File(rootPath).delete();
    }
    if (jsc != null) {
      jsc.stop();
    }
  }
}
