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

import com.uber.hoodie.common.util.SerializationUtils;
import com.uber.hoodie.common.util.SpillableMapUtils;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.HoodieIOException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Iterable to lazily fetch values spilled to disk. This class uses RandomAccessFile to randomly access the position of
 * the latest value for a key spilled to disk and returns the result.
 */
public class LazyFileIterable<T, R> implements Iterable<R> {

  // Used to access the value written at a specific position in the file
  private final RandomAccessFile readOnlyFileHandle;
  // Stores the key and corresponding value's latest metadata spilled to disk
  private final Map<T, DiskBasedMap.ValueMetadata> inMemoryMetadataOfSpilledData;

  public LazyFileIterable(RandomAccessFile file, Map<T, DiskBasedMap.ValueMetadata> map) {
    this.readOnlyFileHandle = file;
    this.inMemoryMetadataOfSpilledData = map;
  }

  @Override
  public Iterator<R> iterator() {
    try {
      return new LazyFileIterator<>(readOnlyFileHandle, inMemoryMetadataOfSpilledData);
    } catch (IOException io) {
      throw new HoodieException("Unable to initialize iterator for file on disk", io);
    }
  }

  /**
   * Iterator implementation for the iterable defined above.
   */
  public class LazyFileIterator<T, R> implements Iterator<R> {

    private RandomAccessFile readOnlyFileHandle;
    private Iterator<Map.Entry<T, DiskBasedMap.ValueMetadata>> metadataIterator;

    public LazyFileIterator(RandomAccessFile file, Map<T, DiskBasedMap.ValueMetadata> map) throws IOException {
      this.readOnlyFileHandle = file;
      // sort the map in increasing order of offset of value so disk seek is only in one(forward) direction
      this.metadataIterator = map
          .entrySet()
          .stream()
          .sorted(
              (Map.Entry<T, DiskBasedMap.ValueMetadata> o1, Map.Entry<T, DiskBasedMap.ValueMetadata> o2) ->
                  o1.getValue().getOffsetOfValue().compareTo(o2.getValue().getOffsetOfValue()))
          .collect(Collectors.toList()).iterator();
    }

    @Override
    public boolean hasNext() {
      return this.metadataIterator.hasNext();
    }

    @Override
    public R next() {
      Map.Entry<T, DiskBasedMap.ValueMetadata> entry = this.metadataIterator.next();
      try {
        return SerializationUtils.deserialize(SpillableMapUtils.readBytesFromDisk(readOnlyFileHandle,
            entry.getValue().getOffsetOfValue(), entry.getValue().getSizeOfValue()));
      } catch (IOException e) {
        throw new HoodieIOException("Unable to read hoodie record from value spilled to disk", e);
      }
    }

    @Override
    public void remove() {
      this.metadataIterator.remove();
    }

    @Override
    public void forEachRemaining(Consumer<? super R> action) {
      action.accept(next());
    }
  }
}