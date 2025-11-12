/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utils;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.ViewStorageProperties;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test cases for {@link ViewStorageProperties}.
 */
public class TestViewStorageProperties {
  @TempDir
  File tempFile;

  @ParameterizedTest
  @ValueSource(strings = {"", "1"})
  void testReadWriteProperties(String uniqueId) throws IOException {
    String basePath = tempFile.getAbsolutePath();
    FileSystemViewStorageConfig config = FileSystemViewStorageConfig.newBuilder()
        .withStorageType(FileSystemViewStorageType.SPILLABLE_DISK)
        .withRemoteServerHost("host1")
        .withRemoteServerPort(1234).build();
    Configuration flinkConfig = new Configuration();
    flinkConfig.set(FlinkOptions.WRITE_CLIENT_ID, uniqueId);
    ViewStorageProperties.createProperties(basePath, config, flinkConfig);
    ViewStorageProperties.createProperties(basePath, config, flinkConfig);
    ViewStorageProperties.createProperties(basePath, config, flinkConfig);

    FileSystemViewStorageConfig readConfig = ViewStorageProperties.loadFromProperties(basePath, flinkConfig);
    assertThat(readConfig.getStorageType(), is(FileSystemViewStorageType.SPILLABLE_DISK));
    assertThat(readConfig.getRemoteViewServerHost(), is("host1"));
    assertThat(readConfig.getRemoteViewServerPort(), is(1234));
  }

  @Test
  void testDumpRemoteViewStorageConfig() throws IOException {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    try (HoodieFlinkWriteClient<?> writeClient = FlinkWriteClients.createWriteClient(conf)) {
      FileSystemViewStorageConfig storageConfig = ViewStorageProperties.loadFromProperties(conf.get(FlinkOptions.PATH), new Configuration());
      assertThat(storageConfig.getStorageType(), is(FileSystemViewStorageType.REMOTE_FIRST));
    }
  }
}
