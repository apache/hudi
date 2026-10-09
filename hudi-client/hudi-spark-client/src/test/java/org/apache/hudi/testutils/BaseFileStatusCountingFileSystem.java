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

package org.apache.hudi.testutils;

import org.apache.hudi.storage.StorageConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Local file system that counts the file status lookups of parquet files. Checksums are not verified, so reading a
 * file looks up no status besides the ones the caller makes.
 */
public class BaseFileStatusCountingFileSystem extends LocalFileSystem {

  private static final AtomicLong PARQUET_FILE_STATUS_CALLS = new AtomicLong();

  /**
   * Returns a copy of the configuration that uses this file system for local paths.
   */
  public static StorageConfiguration<?> withCountingFileSystem(StorageConfiguration<?> storageConf) {
    StorageConfiguration<?> conf = storageConf.newInstance();
    conf.set("fs.file.impl", BaseFileStatusCountingFileSystem.class.getName());
    conf.set("fs.file.impl.disable.cache", "true");
    return conf;
  }

  public static long getParquetFileStatusCalls() {
    return PARQUET_FILE_STATUS_CALLS.get();
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);
    setVerifyChecksum(false);
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    if (f.getName().endsWith(".parquet")) {
      PARQUET_FILE_STATUS_CALLS.incrementAndGet();
    }
    return super.getFileStatus(f);
  }
}
