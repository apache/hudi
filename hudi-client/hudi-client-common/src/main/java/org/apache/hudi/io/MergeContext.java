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

package org.apache.hudi.io;

import org.apache.hudi.common.model.HoodieRecord;

import java.util.Iterator;

/**
 * Context for merge handle creation, carrying incoming data to merge and its characteristics.
 */
public class MergeContext<T> {

  /**
   * Sentinel value indicating that the number of update records is unknown.
   */
  public static final long UNKNOWN_NUM_UPDATES = -1L;

  /**
   * The number of incoming update and delete records tagged to the file group based on
   * workload profiling, or {@link #UNKNOWN_NUM_UPDATES} if unknown.
   *
   * <p>This is a lower bound of the record count of {@link #recordIterator}, not its exact
   * size: with small-file handling, inserts routed to the same file group also flow through
   * the iterator but are not counted here. Do not use this value to pre-size a data structure
   * that must hold all incoming records.
   */
  private final long numUpdates;

  /**
   * Iterator over the incoming records to be merged.
   */
  private final Iterator<HoodieRecord<T>> recordIterator;

  private MergeContext(long numUpdates, Iterator<HoodieRecord<T>> recordIterator) {
    this.numUpdates = numUpdates;
    this.recordIterator = recordIterator;
  }

  public static <T> MergeContext<T> create(long numUpdates, Iterator<HoodieRecord<T>> recordIterator) {
    return new MergeContext<>(numUpdates, recordIterator);
  }

  public static <T> MergeContext<T> create(Iterator<HoodieRecord<T>> recordIterator) {
    return new MergeContext<>(UNKNOWN_NUM_UPDATES, recordIterator);
  }

  public long getNumUpdates() {
    return numUpdates;
  }

  public Iterator<HoodieRecord<T>> getRecordIterator() {
    return recordIterator;
  }
}
