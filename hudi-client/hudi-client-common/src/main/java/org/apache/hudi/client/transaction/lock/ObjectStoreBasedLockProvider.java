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

package org.apache.hudi.client.transaction.lock;

import org.apache.hudi.common.config.LockConfiguration;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A ObjectStore FileSystem based lock.
 * NOTE: This works not only for DFS with atomic create/delete operation, but also
 * ObjectStores that has no atomic semantic guarantee.
 */
public class ObjectStoreBasedLockProvider extends FileSystemBasedLockProvider {

  private static final Logger LOG = LogManager.getLogger(ObjectStoreBasedLockProvider.class);

  private static final int RANDOM_CONTENT_LENGTH = 8192;

  private byte[] randomContent;

  public ObjectStoreBasedLockProvider(final LockConfiguration lockConfiguration, final Configuration conf) {
    super(lockConfiguration, conf);

    randomContent = new byte[RANDOM_CONTENT_LENGTH];
    ThreadLocalRandom.current().nextBytes(randomContent);
  }

  @Override
  protected void acquireLock() {
    try (FSDataOutputStream out = fs.create(this.lockFile, false)) {
      out.write(randomContent);
    } catch (FileAlreadyExistsException e) {
      throw new HoodieIOException("Failed to acquire lock since the file already exists");
    } catch (IOException e) {
      throw new HoodieIOException("Failed to acquire lock", e);
    }

    try (FSDataInputStream in = fs.open(this.lockFile)) {
      byte[] buffer = new byte[RANDOM_CONTENT_LENGTH];
      int length = in.read(buffer);
      if (length == RANDOM_CONTENT_LENGTH && Arrays.equals(randomContent, buffer)) {
        // lock acquired
        return;
      }
      throw new HoodieIOException("Failed to acquire lock since the file content does not match");
    } catch (IOException e) {
      throw new HoodieIOException("Failed to acquire lock", e);
    }
  }

  @Override
  protected void releaseLock() {
    try (FSDataInputStream in = fs.open(this.lockFile)) {
      byte[] buffer = new byte[RANDOM_CONTENT_LENGTH];
      int length = in.read(buffer);
      if (length == RANDOM_CONTENT_LENGTH && Arrays.equals(randomContent, buffer)) {
        super.releaseLock();
        return;
      }
      throw new HoodieIOException("Failed to release lock since the file content does not match");
    } catch (IOException e) {
      throw new HoodieIOException("Failed to release lock", e);
    }
  }
}
