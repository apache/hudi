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

package org.apache.hudi.connect;

import org.apache.hudi.connect.utils.KafkaConnectUtils;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hudi.connect.writers.KafkaConnectConfigs;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;

public class TestHdfsConfiguration {

  private boolean checkFiles(List<Path> paths) {
    paths.removeIf(p -> {
      String fileName = p.toFile().getName();
      return fileName.equals("core-site.xml") || fileName.equals("hdfs-site.xml");
    });
    return paths.isEmpty();
  }

  @Test
  public void testHadoopConfigEnvs() throws Exception {
    List<Path> paths = KafkaConnectUtils.getHadoopConfigFiles(
        "src/test/resources/hadoop_conf", "");
    assertEquals(paths.size(), 2);
    assertTrue(checkFiles(paths));
  }

  @Test
  public void testHadoopHomeEnvs() throws Exception {
    List<Path> paths = KafkaConnectUtils.getHadoopConfigFiles(
        "","src/test/resources/hadoop_home");
    assertEquals(paths.size(), 2);
    assertTrue(checkFiles(paths));
  }

  @Test
  public void testKafkaConfig() throws Exception {
    KafkaConnectConfigs connectConfigs = KafkaConnectConfigs.newBuilder()
        .withHadoopHome("src/test/resources/hadoop_home")
        .build();
    List<Path> paths = KafkaConnectUtils.getHadoopConfigFiles(
        connectConfigs.getHadoopConfDir(),
        connectConfigs.getHadoopConfHome()
    );
    assertEquals(paths.size(), 2);
    assertTrue(checkFiles(paths));
  }
}
