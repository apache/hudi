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

package org.apache.hudi.utilities.checkpointing;

import org.apache.hudi.common.HoodieCommonTestHarness;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class TestKafkaConnectHdfsProvider extends HoodieCommonTestHarness {
  private FileSystem fs = null;
  private String topicPath = null;

  @Before
  public void init() {
    // Prepare directories
    initPath();
    final Configuration hadoopConf = HoodieTestUtils.getDefaultHadoopConf();
    fs = FSUtils.getFs(basePath, hadoopConf);
  }

  @Test
  public void testValidKafkaConnectPath() throws Exception {
    // a standard format(time based partition) of the files managed by kafka connect is:
    // topic/year=xxx/month=xxx/day=xxx/topic+partition+lowerOffset+upperOffset.file
    topicPath = basePath + "/topic1";
    new File(topicPath).mkdirs();
    // create regular kafka connect hdfs dirs
    new File(topicPath + "/year=2016/month=05/day=01/").mkdirs();
    new File(topicPath + "/year=2016/month=05/day=02/").mkdirs();
    // kafka connect tmp folder
    new File(topicPath + "/TMP").mkdirs();
    // tmp file that being written
    new File(topicPath + "/TMP/" + "topic1+0+301+400.parquet").createNewFile();
    // regular parquet files
    new File(topicPath + "/year=2016/month=05/day=01/"
        + "topic1+0+100+200.parquet").createNewFile();
    new File(topicPath + "/year=2016/month=05/day=01/"
        + "topic1+1+100+200.parquet").createNewFile();
    new File(topicPath + "/year=2016/month=05/day=02/"
        + "topic1+0+201+300.parquet").createNewFile();
    // noise parquet file
    new File(topicPath + "/year=2016/month=05/day=01/"
        + "random_snappy_1.parquet").createNewFile();
    new File(topicPath + "/year=2016/month=05/day=02/"
        + "random_snappy_2.parquet").createNewFile();
    InitialCheckPointProvider provider = new KafkaConnectHdfsProvider(new Path(topicPath), fs);
    assertEquals(provider.getCheckpoint(), "topic1,0:300,1:200");
  }

  @Test(expected = HoodieException.class)
  public void testMissingPartition() throws Exception {
    topicPath = basePath + "/topic2";
    new File(topicPath).mkdirs();
    // create regular kafka connect hdfs dirs
    new File(topicPath + "/year=2016/month=05/day=01/").mkdirs();
    new File(topicPath + "/year=2016/month=05/day=02/").mkdirs();
    // parquet files with missing partition
    new File(topicPath + "/year=2016/month=05/day=01/"
        + "topic1+0+100+200.parquet").createNewFile();
    new File(topicPath + "/year=2016/month=05/day=01/"
        + "topic1+2+100+200.parquet").createNewFile();
    new File(topicPath + "/year=2016/month=05/day=02/"
        + "topic1+0+201+300.parquet").createNewFile();
    InitialCheckPointProvider provider = new KafkaConnectHdfsProvider(new Path(topicPath), fs);
    provider.getCheckpoint();
  }
}
