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

import org.apache.hudi.common.util.BufferedRandomAccessFile;
import org.apache.hudi.exception.HoodieException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Iterable to lazily fetch values spilled to disk. This class uses BufferedRandomAccessFile to randomly access the position of
 * the latest value for a key spilled to disk and returns the result.
 */
public class LazyFileIterable<T, R> implements Iterable<R> {
  private static final Logger LOG = LoggerFactory.getLogger(LazyFileIterable.class);

  // Used to access the value written at a specific position in the file
  private final String filePath;
  // Stores the key and corresponding value's latest metadata spilled to disk
  private final Map<T, BitCaskDiskMap.ValueMetadata> inMemoryMetadataOfSpilledData;
  // Was compressions enabled for the values when inserted into the file/ map
  private final boolean isCompressionEnabled;

  private transient Thread shutdownThread = null;

  public LazyFileIterable(String filePath, Map<T, BitCaskDiskMap.ValueMetadata> map) {
    this(filePath, map, false);
  }

  public LazyFileIterable(String filePath, Map<T, BitCaskDiskMap.ValueMetadata> map, boolean isCompressionEnabled) {
    this.filePath = filePath;
    this.inMemoryMetadataOfSpilledData = map;
    this.isCompressionEnabled = isCompressionEnabled;
  }

  @Override
  public ClosableIterator<R> iterator() {
    try {
      return new LazyFileIterator<>(filePath, inMemoryMetadataOfSpilledData);
    } catch (IOException io) {
      throw new HoodieException("Unable to initialize iterator for file on disk", io);
    }
  }

  /**
   * Iterator implementation for the iterable defined above.
   */
  public class LazyFileIterator<T, R> implements ClosableIterator<R> {

    private final String filePath;
    private BufferedRandomAccessFile readOnlyFileHandle;
    private final Iterator<Map.Entry<T, BitCaskDiskMap.ValueMetadata>> metadataIterator;

    public LazyFileIterator(String filePath, Map<T, BitCaskDiskMap.ValueMetadata> map) throws IOException {
      this.filePath = filePath;
      this.readOnlyFileHandle = new BufferedRandomAccessFile(filePath, "r", BitCaskDiskMap.BUFFER_SIZE);
      readOnlyFileHandle.seek(0);

      // sort the map in increasing order of offset of value so disk seek is only in one(forward) direction
      this.metadataIterator = map.entrySet().stream()
          .sorted((Map.Entry<T, BitCaskDiskMap.ValueMetadata> o1, Map.Entry<T, BitCaskDiskMap.ValueMetadata> o2) -> o1
              .getValue().getOffsetOfValue().compareTo(o2.getValue().getOffsetOfValue()))
          .collect(Collectors.toList()).iterator();
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
      return BitCaskDiskMap.get(entry.getValue(), readOnlyFileHandle, isCompressionEnabled);
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
        LOG.warn("Failed to properly close LazyFileIterable in application.");
        this.closeHandle();
      });
      Runtime.getRuntime().addShutdownHook(shutdownThread);
    }
  }
}
