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

import org.apache.hudi.common.serialization.CustomSerializer;
import org.apache.hudi.common.util.BufferedRandomAccessFile;
import org.apache.hudi.exception.HoodieException;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Iterable to lazily fetch values spilled to disk. This class uses BufferedRandomAccessFile to randomly access the position of
 * the latest value for a key spilled to disk and returns the result.
 */
@Slf4j
public class LazyFileIterable<T, R> implements Iterable<R> {

  // Used to access the value written at a specific position in the file
  private final String filePath;
  // Pre-built snapshot of key→metadata entries in disk-offset order. Taking the snapshot at
  // construction time (under the BitCaskDiskMap lock) ensures thread safety when
  // SpillableMapBasedFileSystemView has concurrent puts and reads under its readLock.
  private final List<Map.Entry<T, BitCaskDiskMap.ValueMetadata>> snapshot;
  // Was compressions enabled for the values when inserted into the file/ map
  private final boolean isCompressionEnabled;
  private final CustomSerializer<R> serializer;

  private transient Thread shutdownThread = null;

  /**
   * Constructs a LazyFileIterable with a pre-built snapshot of metadata entries.
   * Callers (e.g. {@link BitCaskDiskMap#iterator()}) must take the snapshot under the
   * BitCaskDiskMap lock before invoking this constructor.
   */
  LazyFileIterable(String filePath, List<Map.Entry<T, BitCaskDiskMap.ValueMetadata>> snapshot,
                   CustomSerializer<R> serializer, boolean isCompressionEnabled) {
    this.filePath = filePath;
    this.snapshot = snapshot;
    this.serializer = serializer;
    this.isCompressionEnabled = isCompressionEnabled;
  }

  /**
   * Constructs a LazyFileIterable from a live metadata map. The snapshot is taken immediately
   * at construction time, but WITHOUT any lock — concurrent writers can cause
   * {@link java.util.ConcurrentModificationException}. Prefer the list-based constructor when
   * the caller already holds a lock that must cover the snapshot (e.g. in
   * {@link BitCaskDiskMap#iterator()}).
   *
   * @deprecated The unsynchronized snapshot is unsafe under the new {@code BitCaskDiskMap}
   *     locking model (ENG-43078). Callers should take a snapshot under the appropriate lock
   *     and use the {@link #LazyFileIterable(String, List, CustomSerializer, boolean) list-based
   *     constructor}. Retained only for binary compatibility with any external callers.
   */
  @Deprecated
  public LazyFileIterable(String filePath, Map<T, BitCaskDiskMap.ValueMetadata> map,
                          CustomSerializer<R> serializer, boolean isCompressionEnabled) {
    this.filePath = filePath;
    this.snapshot = new ArrayList<>(map.entrySet());
    this.serializer = serializer;
    this.isCompressionEnabled = isCompressionEnabled;
  }

  @Override
  public ClosableIterator<R> iterator() {
    try {
      return new LazyFileIterator<>(filePath, snapshot, serializer);
    } catch (IOException io) {
      throw new HoodieException("Unable to initialize iterator for file on disk", io);
    }
  }

  /**
   * Iterator implementation for the iterable defined above.
   */
  public class LazyFileIterator<R> implements ClosableIterator<R> {

    private final String filePath;
    private BufferedRandomAccessFile readOnlyFileHandle;
    private final Iterator<Map.Entry<T, BitCaskDiskMap.ValueMetadata>> metadataIterator;
    private final CustomSerializer<R> serializer;

    /**
     * Constructs the iterator from a pre-built snapshot list.
     * The snapshot must have been taken by the caller (e.g. under the BitCaskDiskMap lock)
     * to ensure no entries are missed and no ConcurrentModificationException occurs.
     * ENG-43078: BitCaskDiskMap.valueMetadataMap is a LinkedHashMap whose insertion order
     * equals disk offset order, so no sort is needed.
     */
    public LazyFileIterator(String filePath, List<Map.Entry<T, BitCaskDiskMap.ValueMetadata>> snapshot,
                            CustomSerializer<R> serializer) throws IOException {
      this.filePath = filePath;
      this.readOnlyFileHandle = new BufferedRandomAccessFile(filePath, "r", BitCaskDiskMap.BUFFER_SIZE);
      this.serializer = serializer;
      readOnlyFileHandle.seek(0);
      this.metadataIterator = snapshot.iterator();
      this.addShutdownHook();
    }

    @Override
    public boolean hasNext() {
      boolean available = this.metadataIterator.hasNext();
      if (!available) {
        close();
      }
      return available;
    }

    @Override
    public R next() {
      if (!hasNext()) {
        throw new IllegalStateException("next() called on EOF'ed stream. File :" + filePath);
      }
      Map.Entry<T, BitCaskDiskMap.ValueMetadata> entry = this.metadataIterator.next();
      return BitCaskDiskMap.get(entry.getValue(), readOnlyFileHandle, serializer, isCompressionEnabled);
    }

    @Override
    public void remove() {
      this.metadataIterator.remove();
    }

    @Override
    public void forEachRemaining(Consumer<? super R> action) {
      action.accept(next());
    }

    public void close() {
      closeHandle();
      Runtime.getRuntime().removeShutdownHook(shutdownThread);
    }

    private void closeHandle() {
      if (readOnlyFileHandle != null) {
        try {
          readOnlyFileHandle.close();
          readOnlyFileHandle = null;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    private void addShutdownHook() {
      shutdownThread = new Thread(() -> {
        log.debug("Failed to properly close LazyFileIterable in application.");
        this.closeHandle();
      });
      Runtime.getRuntime().addShutdownHook(shutdownThread);
    }
  }
}
