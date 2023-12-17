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

import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.exception.HoodieIOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * An external map that spills content to disk when there is insufficient space for it to grow.
 * <p>
 * This map holds 2 types of data structures :
 * <p>
 * (1) Key-Value pairs in a in-memory map (2) Key-ValueMetadata pairs in an in-memory map which keeps a marker to the
 * values spilled to disk
 * <p>
 * NOTE : Values are only appended to disk. If a remove() is called, the entry is marked removed from the in-memory
 * key-valueMetadata map but it's values will be lying around in the temp file on disk until the file is cleaned.
 * <p>
 * The setting of the spill threshold faces the following trade-off: If the spill threshold is too high, the in-memory
 * map may occupy more memory than is available, resulting in OOM. However, if the spill threshold is too low, we spill
 * frequently and incur unnecessary disk writes.
 */
@NotThreadSafe
public class ExternalSpillableMap<T extends Serializable, R extends Serializable> implements Map<T, R>, Serializable, Closeable {

  // Find the actual estimated payload size after inserting N records
  private static final int NUMBER_OF_RECORDS_TO_ESTIMATE_PAYLOAD_SIZE = 100;
  private static final Logger LOG = LoggerFactory.getLogger(ExternalSpillableMap.class);
  // maximum space allowed in-memory for this map
  private final long maxInMemorySizeInBytes;
  // Map to store key-values in memory until it hits maxInMemorySizeInBytes
  private final Map<T, R> inMemoryMap;
  // Map to store key-values on disk or db after it spilled over the memory
  private transient volatile DiskMap<T, R> diskBasedMap;
  // TODO(na) : a dynamic sizing factor to ensure we have space for other objects in memory and
  // incorrect payload estimation
  private final Double sizingFactorForInMemoryMap = 0.8;
  // Size Estimator for key type
  private final SizeEstimator<T> keySizeEstimator;
  // Size Estimator for key types
  private final SizeEstimator<R> valueSizeEstimator;
  // Type of the disk map
  private final DiskMapType diskMapType;
  // Enables compression of values stored in disc
  private final boolean isCompressionEnabled;
  // current space occupied by this map in-memory
  private Long currentInMemoryMapSize;
  // An estimate of the size of each payload written to this map
  private volatile long estimatedPayloadSize = 0;
  // Base File Path
  private final String baseFilePath;

  public ExternalSpillableMap(Long maxInMemorySizeInBytes, String baseFilePath, SizeEstimator<T> keySizeEstimator,
                              SizeEstimator<R> valueSizeEstimator) throws IOException {
    this(maxInMemorySizeInBytes, baseFilePath, keySizeEstimator, valueSizeEstimator, DiskMapType.BITCASK);
  }

  public ExternalSpillableMap(Long maxInMemorySizeInBytes, String baseFilePath, SizeEstimator<T> keySizeEstimator,
                              SizeEstimator<R> valueSizeEstimator, DiskMapType diskMapType) throws IOException {
    this(maxInMemorySizeInBytes, baseFilePath, keySizeEstimator, valueSizeEstimator, diskMapType, false);
  }

  public ExternalSpillableMap(Long maxInMemorySizeInBytes, String baseFilePath, SizeEstimator<T> keySizeEstimator,
                              SizeEstimator<R> valueSizeEstimator, DiskMapType diskMapType, boolean isCompressionEnabled) throws IOException {
    this.inMemoryMap = new HashMap<>();
    this.baseFilePath = baseFilePath;
    this.maxInMemorySizeInBytes = (long) Math.floor(maxInMemorySizeInBytes * sizingFactorForInMemoryMap);
    this.currentInMemoryMapSize = 0L;
    this.keySizeEstimator = keySizeEstimator;
    this.valueSizeEstimator = valueSizeEstimator;
    this.diskMapType = diskMapType;
    this.isCompressionEnabled = isCompressionEnabled;
  }

  private DiskMap<T, R> getDiskBasedMap(boolean forceInitialization) {
    if (null == diskBasedMap) {
      if (!forceInitialization) {
        return DiskMap.empty();
      }
      synchronized (this) {
        if (null == diskBasedMap) {
          try {
            switch (diskMapType) {
              case ROCKS_DB:
                diskBasedMap = new RocksDbDiskMap<>(baseFilePath);
                break;
              case BITCASK:
              default:
                diskBasedMap = new BitCaskDiskMap<>(baseFilePath, isCompressionEnabled);
            }
          } catch (IOException e) {
            throw new HoodieIOException(e.getMessage(), e);
          }
        }
      }
    }
    return diskBasedMap;
  }

  /**
   * A custom iterator to wrap over iterating in-memory + disk spilled data.
   */
  public Iterator<R> iterator() {
    return new IteratorWrapper<>(inMemoryMap.values().iterator(), getDiskBasedMap(false).iterator());
  }

  /**
   * Number of entries in BitCaskDiskMap.
   */
  public int getDiskBasedMapNumEntries() {
    return getDiskBasedMap(false).size();
  }

  /**
   * Number of bytes spilled to disk.
   */
  public long getSizeOfFileOnDiskInBytes() {
    return getDiskBasedMap(false).sizeOfFileOnDiskInBytes();
  }

  /**
   * Number of entries in InMemoryMap.
   */
  public int getInMemoryMapNumEntries() {
    return inMemoryMap.size();
  }

  /**
   * Approximate memory footprint of the in-memory map.
   */
  public long getCurrentInMemoryMapSize() {
    return currentInMemoryMapSize;
  }

  @Override
  public int size() {
    return inMemoryMap.size() + getDiskBasedMap(false).size();
  }

  @Override
  public boolean isEmpty() {
    return inMemoryMap.isEmpty() && getDiskBasedMap(false).isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return inMemoryMap.containsKey(key) || getDiskBasedMap(false).containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return inMemoryMap.containsValue(value) || getDiskBasedMap(false).containsValue(value);
  }

  public boolean inMemoryContainsKey(Object key) {
    return inMemoryMap.containsKey(key);
  }

  public boolean inDiskContainsKey(Object key) {
    return getDiskBasedMap(false).containsKey(key);
  }

  @Override
  public R get(Object key) {
    if (inMemoryMap.containsKey(key)) {
      return inMemoryMap.get(key);
    } else if (getDiskBasedMap(false).containsKey(key)) {
      return getDiskBasedMap(false).get(key);
    }
    return null;
  }

  @Override
  public R put(T key, R value) {
    if (this.estimatedPayloadSize == 0) {
      // At first, use the sizeEstimate of a record being inserted into the spillable map.
      // Note, the converter may over-estimate the size of a record in the JVM
      this.estimatedPayloadSize = keySizeEstimator.sizeEstimate(key) + valueSizeEstimator.sizeEstimate(value);
    } else if (this.inMemoryMap.size() % NUMBER_OF_RECORDS_TO_ESTIMATE_PAYLOAD_SIZE == 0) {
      this.estimatedPayloadSize = (long) (this.estimatedPayloadSize * 0.9 + (keySizeEstimator.sizeEstimate(key) + valueSizeEstimator.sizeEstimate(value)) * 0.1);
      this.currentInMemoryMapSize = this.inMemoryMap.size() * this.estimatedPayloadSize;
    }

    if (this.inMemoryMap.containsKey(key)) {
      this.inMemoryMap.put(key, value);
    } else if (this.currentInMemoryMapSize < this.maxInMemorySizeInBytes) {
      this.currentInMemoryMapSize += this.estimatedPayloadSize;
      // Remove the old version of the record from disk first to avoid data duplication.
      if (inDiskContainsKey(key)) {
        getDiskBasedMap(true).remove(key);
      }
      this.inMemoryMap.put(key, value);
    } else {
      getDiskBasedMap(true).put(key, value);
    }
    return value;
  }

  @Override
  public R remove(Object key) {
    // NOTE : getDiskBasedMap().remove does not delete the data from disk
    if (inMemoryMap.containsKey(key)) {
      currentInMemoryMapSize -= estimatedPayloadSize;
      return inMemoryMap.remove(key);
    } else if (getDiskBasedMap(false).containsKey(key)) {
      return getDiskBasedMap(false).remove(key);
    }
    return null;
  }

  @Override
  public void putAll(Map<? extends T, ? extends R> m) {
    for (Map.Entry<? extends T, ? extends R> entry : m.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void clear() {
    inMemoryMap.clear();
    getDiskBasedMap(false).clear();
    currentInMemoryMapSize = 0L;
  }

  public void close() {
    inMemoryMap.clear();
    getDiskBasedMap(false).close();
    currentInMemoryMapSize = 0L;
  }

  @Override
  public Set<T> keySet() {
    Set<T> keySet = new HashSet<T>();
    keySet.addAll(inMemoryMap.keySet());
    keySet.addAll(getDiskBasedMap(false).keySet());
    return keySet;
  }

  @Override
  public Collection<R> values() {
    if (getDiskBasedMap(false).isEmpty()) {
      return inMemoryMap.values();
    }
    List<R> result = new ArrayList<>(inMemoryMap.values());
    result.addAll(getDiskBasedMap(false).values());
    return result;
  }

  public Stream<R> valueStream() {
    return Stream.concat(inMemoryMap.values().stream(), getDiskBasedMap(false).valueStream());
  }

  @Override
  public Set<Entry<T, R>> entrySet() {
    Set<Entry<T, R>> inMemory = inMemoryMap.entrySet();
    Set<Entry<T, R>> onDisk = getDiskBasedMap(false).entrySet();

    Set<Entry<T, R>> entrySet = new HashSet<>(inMemory.size() + onDisk.size());
    entrySet.addAll(inMemory);
    entrySet.addAll(onDisk);
    return entrySet;
  }

  /**
   * The type of map to use for storing the Key, values on disk after it spills
   * from memory in the {@link ExternalSpillableMap}.
   */
  public enum DiskMapType {
    BITCASK,
    ROCKS_DB,
    UNKNOWN
  }

  /**
   * Iterator that wraps iterating over all the values for this map 1) inMemoryIterator - Iterates over all the data
   * in-memory map 2) diskLazyFileIterator - Iterates over all the data spilled to disk.
   */
  private class IteratorWrapper<R> implements Iterator<R> {

    private final Iterator<R> inMemoryIterator;
    private final Iterator<R> diskLazyFileIterator;

    public IteratorWrapper(Iterator<R> inMemoryIterator, Iterator<R> diskLazyFileIterator) {
      this.inMemoryIterator = inMemoryIterator;
      this.diskLazyFileIterator = diskLazyFileIterator;
    }

    @Override
    public boolean hasNext() {
      if (inMemoryIterator.hasNext()) {
        return true;
      }
      return diskLazyFileIterator.hasNext();
    }

    @Override
    public R next() {
      if (inMemoryIterator.hasNext()) {
        return inMemoryIterator.next();
      }
      return diskLazyFileIterator.next();
    }
  }
}
