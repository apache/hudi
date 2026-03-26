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
 * Context for merge handle creation, carrying incoming data and characteristics
 */
public class MergeContext<T> {

  /**
   * The estimated number of incoming update and delete records to merge
   * A value of -1 means unknown.
   */
  private final long numIncomingUpdates;

  /**
   * Iterator over the incoming records to be merged.
   */
  private final Iterator<HoodieRecord<T>> recordItr;

  private MergeContext(long numIncomingUpdates, Iterator<HoodieRecord<T>> recordItr) {
    this.numIncomingUpdates = numIncomingUpdates;
    this.recordItr = recordItr;
  }

  public static <T> MergeContext<T> create(long numIncomingUpdates, Iterator<HoodieRecord<T>> recordItr) {
    return new MergeContext<>(numIncomingUpdates, recordItr);
  }

  public static <T> MergeContext<T> create(Iterator<HoodieRecord<T>> recordItr) {
    return new MergeContext<>(-1L, recordItr);
  }

  public long getNumIncomingUpdates() {
    return numIncomingUpdates;
  }

  public Iterator<HoodieRecord<T>> getRecordItr() {
    return recordItr;
  }
}
