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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestKafkaConnectHdfsProvider extends HoodieCommonTestHarness {
  private static final String BASE_FILE_EXTENSION = HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().getFileExtension();

  @Test
  public void testValidKafkaConnectPath() throws Exception {
    // a standard format(time based partition) of the files managed by kafka connect is:
    // topic/year=xxx/month=xxx/day=xxx/topic+partition+lowerOffset+upperOffset.file
    Path topicPath = tempDir.resolve("topic1");
    Files.createDirectories(topicPath);
    // create regular kafka connect hdfs dirs
    new File(topicPath + "/year=2016/month=05/day=01/").mkdirs();
    new File(topicPath + "/year=2016/month=05/day=02/").mkdirs();
    // kafka connect tmp folder
    new File(topicPath + "/TMP").mkdirs();
    // tmp file that being written
    new File(topicPath + "/TMP/" + "topic1+0+301+400" + BASE_FILE_EXTENSION).createNewFile();
    // regular base files
    new File(topicPath + "/year=2016/month=05/day=01/"
        + "topic1+0+100+200" + BASE_FILE_EXTENSION).createNewFile();
    new File(topicPath + "/year=2016/month=05/day=01/"
        + "topic1+1+100+200" + BASE_FILE_EXTENSION).createNewFile();
    new File(topicPath + "/year=2016/month=05/day=02/"
        + "topic1+0+201+300" + BASE_FILE_EXTENSION).createNewFile();
    // noise base file
    new File(topicPath + "/year=2016/month=05/day=01/"
        + "random_snappy_1" + BASE_FILE_EXTENSION).createNewFile();
    new File(topicPath + "/year=2016/month=05/day=02/"
        + "random_snappy_2" + BASE_FILE_EXTENSION).createNewFile();
    final TypedProperties props = new TypedProperties();
    props.put("hoodie.deltastreamer.checkpoint.provider.path", topicPath.toString());
    final InitialCheckPointProvider provider = new KafkaConnectHdfsProvider(props);
    provider.init(HoodieTestUtils.getDefaultHadoopConf());
    assertEquals("topic1,0:300,1:200", provider.getCheckpoint());
  }

  @Test
  public void testMissingPartition() throws Exception {
    Path topicPath = tempDir.resolve("topic2");
    Files.createDirectories(topicPath);
    // create regular kafka connect hdfs dirs
    new File(topicPath + "/year=2016/month=05/day=01/").mkdirs();
    new File(topicPath + "/year=2016/month=05/day=02/").mkdirs();
    // base files with missing partition
    new File(topicPath + "/year=2016/month=05/day=01/"
        + "topic1+0+100+200" + BASE_FILE_EXTENSION).createNewFile();
    new File(topicPath + "/year=2016/month=05/day=01/"
        + "topic1+2+100+200" + BASE_FILE_EXTENSION).createNewFile();
    new File(topicPath + "/year=2016/month=05/day=02/"
        + "topic1+0+201+300" + BASE_FILE_EXTENSION).createNewFile();
    final TypedProperties props = new TypedProperties();
    props.put("hoodie.deltastreamer.checkpoint.provider.path", topicPath.toString());
    final InitialCheckPointProvider provider = new KafkaConnectHdfsProvider(props);
    provider.init(HoodieTestUtils.getDefaultHadoopConf());
    assertThrows(HoodieException.class, provider::getCheckpoint);
  }
}
