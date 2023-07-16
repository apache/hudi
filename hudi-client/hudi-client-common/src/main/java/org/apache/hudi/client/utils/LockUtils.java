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

package org.apache.hudi.client.utils;

import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider;
import org.apache.hudi.client.transaction.lock.LockManager;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Properties;

/**
 * Utility class to create instance of {@link LockManager} via lock provider.
 */
public class LockUtils {

  // default lock directory of file system lock manager
  private static final String LOCK_DIRECTORY_NAME = ".lock";

  /**
   * Gets a file system lock manager with {@link FileSystemBasedLockProvider} and given file system lock path.
   *
   * @param writeConfig The {@link HoodieWriteConfig} with given lock config.
   * @param fs          The {@link FileSystem} instance.
   * @return A file system {@link LockManager} with lock path .lock.
   */
  public static LockManager getFileSystemLockManager(HoodieWriteConfig writeConfig, FileSystem fs) {
    Properties properties = new Properties();
    properties.putAll(writeConfig.getProps());
    properties.setProperty(
        HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(),
        FileSystemBasedLockProvider.class.getName());
    properties.setProperty(
        HoodieLockConfig.FILESYSTEM_LOCK_PATH.key(),
        (writeConfig.contains(HoodieLockConfig.FILESYSTEM_LOCK_PATH)
            ? writeConfig.getString(HoodieLockConfig.FILESYSTEM_LOCK_PATH)
            : writeConfig.getBasePath() + Path.SEPARATOR + HoodieTableMetaClient.AUXILIARYFOLDER_NAME)
            + Path.SEPARATOR + LOCK_DIRECTORY_NAME);
    return new LockManager(HoodieWriteConfig.newBuilder().withEngineType(writeConfig.getEngineType()).withProps(properties).build(), fs);
  }
}
