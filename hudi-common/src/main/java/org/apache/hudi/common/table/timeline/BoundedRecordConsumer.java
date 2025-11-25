/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
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

package org.apache.hudi.common.table.timeline;

import org.apache.avro.generic.GenericRecord;

import java.util.function.BiConsumer;

/**
 * Marker interface for BiConsumers that support early termination during timeline loading.
 * 
 * <p>When a consumer implements this interface, the timeline loader can optimize by:
 * <ul>
 *   <li>Stopping file reading when {@link #shouldStop()} returns true</li>
 *   <li>Sorting files in reverse chronological order if {@link #needsReverseOrder()} returns true</li>
 * </ul>
 * 
 * <p>This is particularly useful for limit-based queries where we only need the N most recent instants.
 * The loader will check {@link #shouldStop()} between files and stop reading once the limit is reached,
 * avoiding unnecessary I/O and decoding of records.
 */
public interface BoundedRecordConsumer extends BiConsumer<String, GenericRecord> {
  /**
   * Returns true if the consumer has reached its limit and no more records should be processed.
   * The loader should check this between files and stop reading if true.
   * 
   * @return true if loading should stop, false otherwise
   */
  boolean shouldStop();
  
  /**
   * Returns true if files should be sorted in reverse chronological order (newest first).
   * This is typically true when loading with a limit to get the latest N instants.
   * 
   * @return true if files should be sorted in reverse chronological order, false otherwise
   */
  default boolean needsReverseOrder() {
    return true; // Default to true for limit-based queries
  }
}

