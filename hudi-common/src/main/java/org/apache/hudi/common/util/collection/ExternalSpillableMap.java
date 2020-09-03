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

import org.apache.hudi.common.util.ObjectSizeCalculator;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

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
public class ExternalSpillableMap<K extends Serializable, V extends Serializable> implements Map<K, V> {

  // Find the actual estimated payload size after inserting N records
  private static final int NUMBER_OF_RECORDS_TO_ESTIMATE_PAYLOAD_SIZE = 100;
  private static final Logger LOG = LogManager.getLogger(ExternalSpillableMap.class);
  // maximum space allowed in-memory for this map
  private final long maxInMemorySizeInBytes;
  // Map to store key-values in memory until it hits maxInMemorySizeInBytes
  private final Map<K, V> inMemoryMap;
  // Map to store key-value metadata important to find the values spilled to disk
  private transient volatile DiskBasedMap<K, V> diskBasedMap;
  // TODO(na) : a dynamic sizing factor to ensure we have space for other objects in memory and
  // incorrect payload estimation
  private final Double sizingFactorForInMemoryMap = 0.8;
  // Size Estimator for key type
  private final SizeEstimator<K> keySizeEstimator;
  // Size Estimator for value type
  private final SizeEstimator<V> valueSizeEstimator;
  // current space occupied by this map in-memory
  private Long currentInMemoryMapSize;
  // An estimate of the size of each payload written to this map
  private volatile long estimatedPayloadSize = 0;
  // Flag to determine whether to stop re-estimating payload size
  private boolean shouldEstimatePayloadSize = true;
  // Base File Path
  private final String baseFilePath;

  public ExternalSpillableMap(Long maxInMemorySizeInBytes, String baseFilePath, SizeEstimator<K> keySizeEstimator,
      SizeEstimator<V> valueSizeEstimator) throws IOException {
    this.inMemoryMap = new HashMap<>();
    this.baseFilePath = baseFilePath;
    this.diskBasedMap = new DiskBasedMap<>(baseFilePath);
    this.maxInMemorySizeInBytes = (long) Math.floor(maxInMemorySizeInBytes * sizingFactorForInMemoryMap);
    this.currentInMemoryMapSize = 0L;
    this.keySizeEstimator = keySizeEstimator;
    this.valueSizeEstimator = valueSizeEstimator;
  }

  private DiskBasedMap<K, V> getDiskBasedMap() {
    if (null == diskBasedMap) {
      synchronized (this) {
        if (null == diskBasedMap) {
          try {
            diskBasedMap = new DiskBasedMap<>(baseFilePath);
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
  public Iterator<V> iterator() {
    return new IteratorWrapper<>(inMemoryMap.values().iterator(), getDiskBasedMap().iterator());
  }

  /**
   * Number of entries in DiskBasedMap.
   */
  public int getDiskBasedMapNumEntries() {
    return getDiskBasedMap().size();
  }

  /**
   * Number of bytes spilled to disk.
   */
  public long getSizeOfFileOnDiskInBytes() {
    return getDiskBasedMap().sizeOfFileOnDiskInBytes();
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
    return inMemoryMap.size() + getDiskBasedMap().size();
  }

  @Override
  public boolean isEmpty() {
    return inMemoryMap.isEmpty() && getDiskBasedMap().isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return inMemoryMap.containsKey(key) || getDiskBasedMap().containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return inMemoryMap.containsValue(value) || getDiskBasedMap().containsValue(value);
  }

  @Override
  public V get(Object key) {
    if (inMemoryMap.containsKey(key)) {
      return inMemoryMap.get(key);
    } else if (getDiskBasedMap().containsKey(key)) {
      return getDiskBasedMap().get(key);
    }
    return null;
  }

  @Override
  public V put(K key, V value) {
    if (currentInMemoryMapSize < maxInMemorySizeInBytes || inMemoryMap.containsKey(key)) {
      // put to inMemoryMap
      if (shouldEstimatePayloadSize && estimatedPayloadSize == 0) {
        // At first, use the sizeEstimate of a record being inserted into the spillable map.
        // Note, the converter may over estimate the size of a record in the JVM
        estimatedPayloadSize = keySizeEstimator.sizeEstimate(key) + valueSizeEstimator.sizeEstimate(value);
        LOG.debug("Estimated Payload size => " + estimatedPayloadSize);
      } else if (shouldEstimatePayloadSize && inMemoryMap.size() % NUMBER_OF_RECORDS_TO_ESTIMATE_PAYLOAD_SIZE == 0) {
        // Re-estimate the size of a record by calculating the size of the entire map containing
        // N entries and then dividing by the number of entries present (N). This helps to get a
        // correct estimation of the size of each record in the JVM.
        long totalMapSize = ObjectSizeCalculator.getObjectSize(inMemoryMap);
        currentInMemoryMapSize = totalMapSize;
        estimatedPayloadSize = totalMapSize / inMemoryMap.size();
        shouldEstimatePayloadSize = false;
        LOG.debug("New Estimated Payload size => " + estimatedPayloadSize);
      }
      if (!inMemoryMap.containsKey(key)) {
        // TODO : Add support for adjusting payloadSize for updates to the same key
        currentInMemoryMapSize += estimatedPayloadSize;
      }
      inMemoryMap.put(key, value);
    } else {
      // put to DiskBasedMap
      getDiskBasedMap().put(key, value);
    }
    return value;
  }

  @Override
  public V remove(Object key) {
    // NOTE : getDiskBasedMap().remove does not delete the data from disk
    if (inMemoryMap.containsKey(key)) {
      currentInMemoryMapSize -= estimatedPayloadSize;
      return inMemoryMap.remove(key);
    } else if (getDiskBasedMap().containsKey(key)) {
      return getDiskBasedMap().remove(key);
    }
    return null;
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void clear() {
    inMemoryMap.clear();
    getDiskBasedMap().clear();
    currentInMemoryMapSize = 0L;
  }

  @Override
  public Set<K> keySet() {
    Set<K> keySet = new HashSet<K>();
    keySet.addAll(inMemoryMap.keySet());
    keySet.addAll(getDiskBasedMap().keySet());
    return keySet;
  }

  @Override
  public Collection<V> values() {
    if (getDiskBasedMap().isEmpty()) {
      return inMemoryMap.values();
    }
    List<V> result = new ArrayList<>(inMemoryMap.values());
    result.addAll(getDiskBasedMap().values());
    return result;
  }

  public Stream<V> valueStream() {
    return Stream.concat(inMemoryMap.values().stream(), getDiskBasedMap().valueStream());
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    Set<Entry<K, V>> entrySet = new HashSet<>();
    entrySet.addAll(inMemoryMap.entrySet());
    entrySet.addAll(getDiskBasedMap().entrySet());
    return entrySet;
  }

  /**
   * Iterator that wraps iterating over all the values for this map 1) inMemoryIterator - Iterates over all the data
   * in-memory map 2) diskLazyFileIterator - Iterates over all the data spilled to disk.
   */
  private class IteratorWrapper<R> implements Iterator<R> {

    private Iterator<R> inMemoryIterator;
    private Iterator<R> diskLazyFileIterator;

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
