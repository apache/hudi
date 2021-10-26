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

import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.util.ViewStorageProperties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

  @Test
  void testReadWriteProperties() throws IOException {
    String basePath = tempFile.getAbsolutePath();
    FileSystemViewStorageConfig config = FileSystemViewStorageConfig.newBuilder()
        .withStorageType(FileSystemViewStorageType.SPILLABLE_DISK)
        .withRemoteServerHost("host1")
        .withRemoteServerPort(1234).build();
    ViewStorageProperties.createProperties(basePath, config);
    ViewStorageProperties.createProperties(basePath, config);
    ViewStorageProperties.createProperties(basePath, config);

    FileSystemViewStorageConfig readConfig = ViewStorageProperties.loadFromProperties(basePath);
    assertThat(readConfig.getStorageType(), is(FileSystemViewStorageType.SPILLABLE_DISK));
    assertThat(readConfig.getRemoteViewServerHost(), is("host1"));
    assertThat(readConfig.getRemoteViewServerPort(), is(1234));
  }
}
