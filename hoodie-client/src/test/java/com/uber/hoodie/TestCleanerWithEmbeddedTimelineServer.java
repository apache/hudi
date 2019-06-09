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

package com.uber.hoodie;

import com.uber.hoodie.common.table.view.FileSystemViewStorageConfig;
import com.uber.hoodie.common.table.view.FileSystemViewStorageType;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieException;
import java.io.IOException;
import org.junit.After;

public class TestCleanerWithEmbeddedTimelineServer extends TestCleaner {

  @Override
  HoodieWriteConfig.Builder getConfigBuilder() {
    HoodieWriteConfig.Builder builder = super.getConfigBuilder();
    try {
      builder.withEmbeddedTimelineServerEnabled(true).withFileSystemViewConfig(
          FileSystemViewStorageConfig.newBuilder().withStorageType(FileSystemViewStorageType.EMBEDDED_KV_STORE)
              .build());
    } catch (Exception e) {
      throw new HoodieException(e);
    }
    return builder;
  }

  @After
  public void tearDown() throws IOException {
    super.tearDown();
  }
}
