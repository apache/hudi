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
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.HoodieIOException;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Iterable to lazily fetch values spilled to disk.
 * This class uses RandomAccessFile to randomly access the position of
 * the latest value for a key spilled to disk and returns the result.
 * @param <T>
 */
public class LazyFileIterable<T> implements Iterable<T> {

  // Used to access the value written at a specific position in the file
  private RandomAccessFile readOnlyFileHandle;
  // Stores the key and corresponding value's latest metadata spilled to disk
  private Map<T, DiskBasedMap.ValueMetadata> inMemoryMetadataOfSpilledData;
  // Schema used to de-serialize payload written to disk
  private Schema schema;
  // Class used to de-serialize/realize payload written to disk
  private String payloadClazz;

  public LazyFileIterable(RandomAccessFile file, Map<T, DiskBasedMap.ValueMetadata> map,
                          Schema schema, String payloadClazz) {
    this.readOnlyFileHandle = file;
    this.inMemoryMetadataOfSpilledData = map;
    this.schema = schema;
    this.payloadClazz = payloadClazz;
  }
  @Override
  public Iterator<T> iterator() {
    try {
      return new LazyFileIterator<>(readOnlyFileHandle, inMemoryMetadataOfSpilledData, schema, payloadClazz);
    } catch(IOException io) {
      throw new HoodieException("Unable to initialize iterator for file on disk", io);
    }
  }

  /**
   * Iterator implementation for the iterable defined above.
   * @param <T>
   */
  public class LazyFileIterator<T> implements Iterator<T> {

    private RandomAccessFile readOnlyFileHandle;
    private Schema schema;
    private String payloadClazz;
    private Iterator<Map.Entry<T, DiskBasedMap.ValueMetadata>> metadataIterator;

    public LazyFileIterator(RandomAccessFile file, Map<T, DiskBasedMap.ValueMetadata> map,
                            Schema schema, String payloadClazz) throws IOException {
      this.readOnlyFileHandle = file;
      this.schema = schema;
      this.payloadClazz = payloadClazz;
      // sort the map in increasing order of offset of value so disk seek is only in one(forward) direction
      this.metadataIterator = map
          .entrySet()
          .stream()
          .sorted((Map.Entry<T, DiskBasedMap.ValueMetadata> o1, Map.Entry<T, DiskBasedMap.ValueMetadata> o2) ->
              o1.getValue().getOffsetOfValue().compareTo(o2.getValue().getOffsetOfValue()))
          .collect(Collectors.toList()).iterator();
    }

    @Override
    public boolean hasNext() {
      return this.metadataIterator.hasNext();
    }

    @Override
    public T next() {
      Map.Entry<T, DiskBasedMap.ValueMetadata> entry = this.metadataIterator.next();
      try {
        return SpillableMapUtils.readFromDisk(readOnlyFileHandle, schema,
            payloadClazz, entry.getValue().getOffsetOfValue(), entry.getValue().getSizeOfValue());
      } catch(IOException e) {
        throw new HoodieIOException("Unable to read hoodie record from value spilled to disk", e);
      }
    }

    @Override
    public void remove() {
      this.metadataIterator.remove();
    }

    @Override
    public void forEachRemaining(Consumer<? super T> action) {
      action.accept(next());
    }
  }
}