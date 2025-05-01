/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.storage.hadoop;

import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.io.storage.TestHoodieStorageBase;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests {@link HoodieHadoopStorage}.
 */
public class TestHoodieHadoopStorage extends TestHoodieStorageBase {
  private static final String CONF_KEY = "hudi.testing.key";
  private static final String CONF_VALUE = "value";

  @Override
  protected HoodieStorage getStorage(Object fs, Object conf) {
    return new HoodieHadoopStorage((FileSystem) fs);
  }

  @Override
  protected Object getFileSystem(Object conf) {
    return HadoopFSUtils.getFs(getTempDir(), (Configuration) conf, true);
  }

  @Override
  protected Object getConf() {
    Configuration conf = new Configuration();
    conf.set(CONF_KEY, CONF_VALUE);
    return conf;
  }

  @Test
  void testClose() throws IOException {
    Configuration conf = new Configuration();
    String path = getTempDir();
    FileSystem fileSystem = HadoopFSUtils.getFs(path, conf, true);
    HoodieStorage storage = new HoodieHadoopStorage(fileSystem);
    storage.close();
    // This validates that HoodieHadoopStorage#close does not close the underlying FileSystem
    // object. If the underlying FileSystem object is closed, the cache of the object based on
    // the path is closed and removed, which causes problems if it is reused elsewhere. Fetching
    // the FileSystem object on the same path again in this case returns a different object,
    // which can be caught here.
    assertSame(fileSystem, storage.getFileSystem());
    assertSame(fileSystem, HadoopFSUtils.getFs(getTempDir(), conf, true));
  }
}
