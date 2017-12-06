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

import com.uber.hoodie.common.util.SpillableMapUtils;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.exception.HoodieNotSupportedException;
import org.apache.avro.Schema;
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
 * An external map that spills content to disk when there is insufficient space for it
 * to grow.
 *
 * This map holds 2 types of data structures :
 *
 *   (1) Key-Value pairs in a in-memory map
 *   (2) Key-ValueMetadata pairs in an in-memory map which keeps a marker to the values spilled to disk
 *
 * NOTE : Values are only appended to disk. If a remove() is called, the entry is marked removed from the in-memory
 * key-valueMetadata map but it's values will be lying around in the temp file on disk until the file is cleaned.
 *
 * The setting of the spill threshold faces the following trade-off: If the spill threshold is
 * too high, the in-memory map may occupy more memory than is available, resulting in OOM.
 * However, if the spill threshold is too low, we spill frequently and incur unnecessary disk
 * writes.
 * @param <T>
 * @param <R>
 */
public class ExternalSpillableMap<T,R> implements Map<T,R> {

  // maximum space allowed in-memory for this map
  final private long maxInMemorySizeInBytes;
  // current space occupied by this map in-memory
  private Long currentInMemoryMapSize;
  // Map to store key-values in memory until it hits maxInMemorySizeInBytes
  final private Map<T,R> inMemoryMap;
  // Map to store key-valuemetadata important to find the values spilled to disk
  final private DiskBasedMap<T,R> diskBasedMap;
  // Schema used to de-serialize and readFromDisk the records written to disk
  final private Schema schema;
  // An estimate of the size of each payload written to this map
  private volatile long estimatedPayloadSize = 0;
  // TODO(na) : a dynamic sizing factor to ensure we have space for other objects in memory and incorrect payload estimation
  final private Double sizingFactorForInMemoryMap = 0.8;

  private static Logger log = LogManager.getLogger(ExternalSpillableMap.class);


  public ExternalSpillableMap(Long maxInMemorySizeInBytes, Schema schema,
                              String payloadClazz, Optional<String> baseFilePath) throws IOException {
    this.inMemoryMap = new HashMap<>();
    this.diskBasedMap = new DiskBasedMap<>(schema, payloadClazz, baseFilePath);
    this.maxInMemorySizeInBytes = (long) Math.floor(maxInMemorySizeInBytes*sizingFactorForInMemoryMap);
    this.schema = schema;
    this.currentInMemoryMapSize = 0L;
  }

  /**
   * A custom iterator to wrap over iterating in-memory + disk spilled data
   * @return
   */
  public Iterator<R> iterator() {
    return new IteratorWrapper<>(inMemoryMap.values().iterator(), diskBasedMap.iterator());
  }

  /**
   * Number of entries in DiskBasedMap
   * @return
   */
  public int getDiskBasedMapNumEntries() {
    return diskBasedMap.size();
  }

  /**
   * Number of bytes spilled to disk
   * @return
   */
  public long getSizeOfFileOnDiskInBytes() {
    return diskBasedMap.sizeOfFileOnDiskInBytes();
  }

  /**
   * Number of entries in InMemoryMap
   * @return
   */
  public int getInMemoryMapNumEntries() {
    return inMemoryMap.size();
  }

  /**
   * Approximate memory footprint of the in-memory map
   * @return
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
    if(inMemoryMap.containsKey(key)) {
      return inMemoryMap.get(key);
    } else if(diskBasedMap.containsKey(key)) {
      return diskBasedMap.get(key);
    }
    return null;
  }

  @Override
  public R put(T key, R value) {
    try {
      if (this.currentInMemoryMapSize < maxInMemorySizeInBytes || inMemoryMap.containsKey(key)) {
        // Naive approach for now
        if (estimatedPayloadSize == 0) {
          this.estimatedPayloadSize = SpillableMapUtils.computePayloadSize(value, schema);
          log.info("Estimated Payload size => " + estimatedPayloadSize);
        }
        if(!inMemoryMap.containsKey(key)) {
          currentInMemoryMapSize += this.estimatedPayloadSize;
        }
        inMemoryMap.put(key, value);
      } else {
        diskBasedMap.put(key, value);
      }
      return value;
    } catch(IOException io) {
      throw new HoodieIOException("Unable to estimate size of payload", io);
    }
  }

  @Override
  public R remove(Object key) {
    // NOTE : diskBasedMap.remove does not delete the data from disk
    if(inMemoryMap.containsKey(key)) {
      currentInMemoryMapSize -= estimatedPayloadSize;
      return inMemoryMap.remove(key);
    } else if(diskBasedMap.containsKey(key)) {
      return diskBasedMap.remove(key);
    }
    return null;
  }

  @Override
  public void putAll(Map<? extends T, ? extends R> m) {
    for(Map.Entry<? extends T, ? extends R> entry: m.entrySet()) {
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
    if(diskBasedMap.isEmpty()) {
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
   * @param <R>
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
      if(inMemoryIterator.hasNext()) {
        return true;
      }
      return diskLazyFileIterator.hasNext();
    }

    @Override
    public R next() {
      if(inMemoryIterator.hasNext()) {
        return inMemoryIterator.next();
      }
      return diskLazyFileIterator.next();
    }
  }
}