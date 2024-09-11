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

package org.apache.hudi.hadoop;

import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieException;

import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Provides Iterator Interface to iterate value entries read from record reader.
 *
 * @param <K> Key Type
 * @param <V> Value Type
 */
public class RecordReaderValueIterator<K, V> implements ClosableIterator<V> {

  private static final Logger LOG = LoggerFactory.getLogger(RecordReaderValueIterator.class);

  private final RecordReader<K, V> reader;
  private V nextVal = null;

  /**
   * Construct RecordReaderValueIterator.
   *
   * @param reader reader
   */
  public RecordReaderValueIterator(RecordReader<K, V> reader) {
    this.reader = reader;
  }

  @Override
  public boolean hasNext() {
    if (nextVal == null) {
      K key = reader.createKey();
      V val = reader.createValue();
      try {
        boolean notDone = reader.next(key, val);
        if (!notDone) {
          return false;
        }
        this.nextVal = val;
      } catch (IOException e) {
        LOG.error("Got error reading next record from record reader");
        throw new HoodieException(e);
      }
    }
    return true;
  }

  @Override
  public V next() {
    if (!hasNext()) {
      throw new NoSuchElementException("Make sure you are following iterator contract.");
    }
    V retVal = this.nextVal;
    this.nextVal = null;
    return retVal;
  }

  @Override
  public void close() {
    try {
      this.reader.close();
    } catch (IOException e) {
      throw new RuntimeException("Could not close reader", e);
    }
  }
}
