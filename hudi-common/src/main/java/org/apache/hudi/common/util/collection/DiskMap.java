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

package org.apache.hudi.common.util.collection;

import org.apache.hudi.common.util.FileIOUtils;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * This interface provides the map interface for storing records in disk after they
 * spill over from memory. Used by {@link ExternalSpillableMap}.
 *
 * @param <T> The generic type of the keys
 * @param <R> The generic type of the values
 */
public abstract class DiskMap<T extends Serializable, R extends Serializable> implements Map<T, R>, Iterable<R> {

  private static final Logger LOG = LoggerFactory.getLogger(DiskMap.class);
  private static final String SUBFOLDER_PREFIX = "hudi";
  private final File diskMapPathFile;
  private transient Thread shutdownThread = null;

  // Base path for the write file
  protected final String diskMapPath;

  public DiskMap(String basePath, String prefix) throws IOException {
    this.diskMapPath =
        String.format("%s/%s-%s-%s", basePath, SUBFOLDER_PREFIX, prefix, UUID.randomUUID().toString());
    diskMapPathFile = new File(diskMapPath);
    FileIOUtils.deleteDirectory(diskMapPathFile);
    FileIOUtils.mkdir(diskMapPathFile);
    // Make sure the folder is deleted when JVM exits
    diskMapPathFile.deleteOnExit();
    addShutDownHook();
  }

  // Only used for the EmptyDiskMap implementation
  private DiskMap() {
    this.diskMapPath = null;
    this.diskMapPathFile = null;
  }

  /**
   * Register shutdown hook to force flush contents of the data written to FileOutputStream from OS page cache
   * (typically 4 KB) to disk.
   */
  private void addShutDownHook() {
    shutdownThread = new Thread(() -> {
      LOG.warn("Failed to properly close DiskMap in application");
      cleanup();
    });
    Runtime.getRuntime().addShutdownHook(shutdownThread);
  }

  /**
   * @returns a stream of the values stored in the disk.
   */
  abstract Stream<R> valueStream();

  /**
   * Number of bytes spilled to disk.
   */
  abstract long sizeOfFileOnDiskInBytes();

  /**
   * Close and cleanup the Map.
   */
  public void close() {
    cleanup(false);
  }

  /**
   * Cleanup all resources, files and folders
   * triggered by shutdownhook.
   */
  private void cleanup() {
    cleanup(true);
  }

  /**
   * Cleanup all resources, files and folders.
   */
  private void cleanup(boolean isTriggeredFromShutdownHook) {
    try {
      FileIOUtils.deleteDirectory(diskMapPathFile);
    } catch (IOException exception) {
      LOG.warn("Error while deleting the disk map directory=" + diskMapPath, exception);
    }
    if (!isTriggeredFromShutdownHook && shutdownThread != null) {
      Runtime.getRuntime().removeShutdownHook(shutdownThread);
    }
  }

  private static final DiskMap EMPTY = new EmptyDiskMap<>();

  public static <T extends Serializable, R extends Serializable> DiskMap<T, R> empty() {
    return (DiskMap<T, R>) EMPTY;
  }

  static class EmptyDiskMap<T extends Serializable, R extends Serializable> extends DiskMap<T, R> {

    public EmptyDiskMap() {
      super();
    }

    @Override
    public Stream<R> valueStream() {
      return Stream.empty();
    }

    @Override
    public long sizeOfFileOnDiskInBytes() {
      return 0;
    }

    @Override
    public int size() {
      return 0;
    }

    @Override
    public boolean isEmpty() {
      return true;
    }

    @Override
    public boolean containsKey(Object key) {
      return false;
    }

    @Override
    public boolean containsValue(Object value) {
      return false;
    }

    @Override
    public R get(Object key) {
      throw new IllegalStateException("Cannot get from an empty map");
    }

    @Nullable
    @Override
    public R put(T key, R value) {
      throw new IllegalStateException("Cannot put into an empty map");
    }

    @Override
    public R remove(Object key) {
      throw new IllegalStateException("Cannot remove from an empty map");
    }

    @Override
    public void putAll(@NotNull Map<? extends T, ? extends R> m) {
      throw new IllegalStateException("Cannot putAll on an empty map");
    }

    @Override
    public void clear() {

    }

    @NotNull
    @Override
    public Set<T> keySet() {
      return Collections.emptySet();
    }

    @NotNull
    @Override
    public Collection<R> values() {
      return Collections.emptyList();
    }

    @NotNull
    @Override
    public Set<Entry<T, R>> entrySet() {
      return Collections.emptySet();
    }

    @Override
    public void close() {
      // Do nothing
    }

    @NotNull
    @Override
    public Iterator<R> iterator() {
      return Collections.<R>emptyList().iterator();
    }
  }
}
