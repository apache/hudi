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

package org.apache.hudi.common.testutils;

import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link HoodieHadoopStorage} that counts file opens and file status lookups per path. Counters are shared by all
 * instances, including the ones created by {@link #newInstance(StoragePath, StorageConfiguration)}.
 */
public class CountingHoodieStorage extends HoodieHadoopStorage {

  private static final Map<StoragePath, AtomicInteger> OPEN_COUNTS = new ConcurrentHashMap<>();
  private static final Map<StoragePath, AtomicInteger> PATH_INFO_COUNTS = new ConcurrentHashMap<>();

  public CountingHoodieStorage(StoragePath path, StorageConfiguration<?> conf) {
    super(path, conf);
  }

  public static void resetCounts() {
    OPEN_COUNTS.clear();
    PATH_INFO_COUNTS.clear();
  }

  public static int getOpenCount(StoragePath path) {
    return getCount(OPEN_COUNTS, path);
  }

  public static int getPathInfoCount(StoragePath path) {
    return getCount(PATH_INFO_COUNTS, path);
  }

  @Override
  public HoodieStorage newInstance(StoragePath path, StorageConfiguration<?> storageConf) {
    return new CountingHoodieStorage(path, storageConf);
  }

  @Override
  public InputStream open(StoragePath path) throws IOException {
    increment(OPEN_COUNTS, path);
    return super.open(path);
  }

  @Override
  public SeekableDataInputStream openSeekable(StoragePath path, int bufferSize, boolean wrapStream) throws IOException {
    increment(OPEN_COUNTS, path);
    return super.openSeekable(path, bufferSize, wrapStream);
  }

  @Override
  public StoragePathInfo getPathInfo(StoragePath path) throws IOException {
    increment(PATH_INFO_COUNTS, path);
    return super.getPathInfo(path);
  }

  private static void increment(Map<StoragePath, AtomicInteger> counts, StoragePath path) {
    counts.computeIfAbsent(normalize(path), p -> new AtomicInteger()).incrementAndGet();
  }

  private static int getCount(Map<StoragePath, AtomicInteger> counts, StoragePath path) {
    AtomicInteger count = counts.get(normalize(path));
    return count == null ? 0 : count.get();
  }

  private static StoragePath normalize(StoragePath path) {
    return new StoragePath(path.toUri().getPath());
  }
}
