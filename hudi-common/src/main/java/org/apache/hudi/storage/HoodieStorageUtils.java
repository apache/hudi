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

package org.apache.hudi.storage;

import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hadoop.fs.HoodieWrapperFileSystem;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class HoodieStorageUtils {
  public static final String DEFAULT_URI = "file:///";

  public static HoodieStorage getStorage(Configuration conf) {
    return getStorage(DEFAULT_URI, conf);
  }

  public static HoodieStorage getStorage(FileSystem fs) {
    return new HoodieHadoopStorage(fs);
  }

  public static HoodieStorage getStorage(String basePath, Configuration conf) {
    return getStorage(HadoopFSUtils.getFs(basePath, conf));
  }

  public static HoodieStorage getStorage(StoragePath path, Configuration conf) {
    return getStorage(HadoopFSUtils.getFs(path, conf));
  }

  public static HoodieStorage getRawStorage(HoodieStorage storage) {
    FileSystem fs = (FileSystem) storage.getFileSystem();
    if (fs instanceof HoodieWrapperFileSystem) {
      return getStorage(((HoodieWrapperFileSystem) fs).getFileSystem());
    }
    return storage;
  }
}
