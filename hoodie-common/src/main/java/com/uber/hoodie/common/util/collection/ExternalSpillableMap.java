/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.util.collection;

import com.twitter.common.objectsize.ObjectSizeCalculator;
import com.uber.hoodie.common.util.collection.converter.Converter;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.HoodieNotSupportedException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * An external map that spills content to disk when there is insufficient space for it to grow. <p>
 * This map holds 2 types of data structures : <p> (1) Key-Value pairs in a in-memory map (2)
 * Key-ValueMetadata pairs in an in-memory map which keeps a marker to the values spilled to disk
 * <p> NOTE : Values are only appended to disk. If a remove() is called, the entry is marked removed
 * from the in-memory key-valueMetadata map but it's values will be lying around in the temp file on
 * disk until the file is cleaned. <p> The setting of the spill threshold faces the following
 * trade-off: If the spill threshold is too high, the in-memory map may occupy more memory than is
 * available, resulting in OOM. However, if the spill threshold is too low, we spill frequently and
 * incur unnecessary disk writes.
 */
public class ExternalSpillableMap<T, R> implements Map<T, R> {

  // Find the actual estimated payload size after inserting N records
  final private static int NUMBER_OF_RECORDS_TO_ESTIMATE_PAYLOAD_SIZE = 100;
  // maximum space allowed in-memory for this map
  final private long maxInMemorySizeInBytes;
  // current space occupied by this map in-memory
  private Long currentInMemoryMapSize;
  // Map to store key-values in memory until it hits maxInMemorySizeInBytes
  final private Map<T, R> inMemoryMap;
  // Map to store key-valuemetadata important to find the values spilled to disk
  final private DiskBasedMap<T, R> diskBasedMap;
  // An estimate of the size of each payload written to this map
  private volatile long estimatedPayloadSize = 0;
  // TODO(na) : a dynamic sizing factor to ensure we have space for other objects in memory and incorrect payload estimation
  final private Double sizingFactorForInMemoryMap = 0.8;
  // Key converter to convert key type to bytes
  final private Converter<T> keyConverter;
  // Value converter to convert value type to bytes
  final private Converter<R> valueConverter;
  // Flag to determine whether to stop re-estimating payload size
  private boolean shouldEstimatePayloadSize = true;

  private static Logger log = LogManager.getLogger(ExternalSpillableMap.class);

  public ExternalSpillableMap(Long maxInMemorySizeInBytes, Optional<String> baseFilePath,
      Converter<T> keyConverter, Converter<R> valueConverter) throws IOException {
    this.inMemoryMap = new HashMap<>();
    this.diskBasedMap = new DiskBasedMap<>(baseFilePath, keyConverter, valueConverter);
    this.maxInMemorySizeInBytes = (long) Math
        .floor(maxInMemorySizeInBytes * sizingFactorForInMemoryMap);
    this.currentInMemoryMapSize = 0L;
    this.keyConverter = keyConverter;
    this.valueConverter = valueConverter;
  }

  /**
   * A custom iterator to wrap over iterating in-memory + disk spilled data
   */
  public Iterator<R> iterator() {
    return new IteratorWrapper<>(inMemoryMap.values().iterator(), diskBasedMap.iterator());
  }

  /**
   * Number of entries in DiskBasedMap
   */
  public int getDiskBasedMapNumEntries() {
    return diskBasedMap.size();
  }

  /**
   * Number of bytes spilled to disk
   */
  public long getSizeOfFileOnDiskInBytes() {
    return diskBasedMap.sizeOfFileOnDiskInBytes();
  }

  /**
   * Number of entries in InMemoryMap
   */
  public int getInMemoryMapNumEntries() {
    return inMemoryMap.size();
  }

  /**
   * Approximate memory footprint of the in-memory map
   */
  public long getCurrentInMemoryMapSize() {
    return currentInMemoryMapSize;
  }

  @Override
  public int size() {
    return inMemoryMap.size() + diskBasedMap.size();
  }

  @Override
  public boolean isEmpty() {
    return inMemoryMap.isEmpty() && diskBasedMap.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return inMemoryMap.containsKey(key) || diskBasedMap.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return inMemoryMap.containsValue(value) || diskBasedMap.containsValue(value);
  }

  @Override
  public R get(Object key) {
    if (inMemoryMap.containsKey(key)) {
      return inMemoryMap.get(key);
    } else if (diskBasedMap.containsKey(key)) {
      return diskBasedMap.get(key);
    }
    return null;
  }

  @Override
  public R put(T key, R value) {
    if (this.currentInMemoryMapSize < maxInMemorySizeInBytes || inMemoryMap.containsKey(key)) {
      if (shouldEstimatePayloadSize && estimatedPayloadSize == 0) {
        // At first, use the sizeEstimate of a record being inserted into the spillable map.
        // Note, the converter may over estimate the size of a record in the JVM
        this.estimatedPayloadSize =
            keyConverter.sizeEstimate(key) + valueConverter.sizeEstimate(value);
        log.info("Estimated Payload size => " + estimatedPayloadSize);
      }
      else if(shouldEstimatePayloadSize &&
          inMemoryMap.size() % NUMBER_OF_RECORDS_TO_ESTIMATE_PAYLOAD_SIZE == 0) {
        // Re-estimate the size of a record by calculating the size of the entire map containing
        // N entries and then dividing by the number of entries present (N). This helps to get a
        // correct estimation of the size of each record in the JVM.
        long totalMapSize = ObjectSizeCalculator.getObjectSize(inMemoryMap);
        this.currentInMemoryMapSize = totalMapSize;
        this.estimatedPayloadSize = totalMapSize/inMemoryMap.size();
        shouldEstimatePayloadSize = false;
        log.info("New Estimated Payload size => " + this.estimatedPayloadSize);
      }
      if (!inMemoryMap.containsKey(key)) {
        // TODO : Add support for adjusting payloadSize for updates to the same key
        currentInMemoryMapSize += this.estimatedPayloadSize;
      }
      inMemoryMap.put(key, value);
    } else {
      diskBasedMap.put(key, value);
    }
    return value;
  }

  @Override
  public R remove(Object key) {
    // NOTE : diskBasedMap.remove does not delete the data from disk
    if (inMemoryMap.containsKey(key)) {
      currentInMemoryMapSize -= estimatedPayloadSize;
      return inMemoryMap.remove(key);
    } else if (diskBasedMap.containsKey(key)) {
      return diskBasedMap.remove(key);
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
    diskBasedMap.clear();
    currentInMemoryMapSize = 0L;
  }

  @Override
  public Set<T> keySet() {
    Set<T> keySet = new HashSet<T>();
    keySet.addAll(inMemoryMap.keySet());
    keySet.addAll(diskBasedMap.keySet());
    return keySet;
  }

  @Override
  public Collection<R> values() {
    if (diskBasedMap.isEmpty()) {
      return inMemoryMap.values();
    }
    throw new HoodieNotSupportedException("Cannot return all values in memory");
  }

  @Override
  public Set<Entry<T, R>> entrySet() {
    Set<Entry<T, R>> entrySet = new HashSet<>();
    entrySet.addAll(inMemoryMap.entrySet());
    entrySet.addAll(diskBasedMap.entrySet());
    return entrySet;
  }

  /**
   * Iterator that wraps iterating over all the values for this map
   * 1) inMemoryIterator - Iterates over all the data in-memory map
   * 2) diskLazyFileIterator - Iterates over all the data spilled to disk
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