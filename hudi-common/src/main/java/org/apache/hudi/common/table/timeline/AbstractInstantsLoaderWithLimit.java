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

import java.util.List;

/**
 * Abstract base class for loading instants with a limit.
 * This class handles the common logic for limit-based loading, while allowing
 * subclasses to override the differences in how instants are read and stored.
 */
public abstract class AbstractInstantsLoaderWithLimit implements BoundedRecordConsumer {
  protected final int limit;
  protected volatile int loadedCount = 0;

  protected AbstractInstantsLoaderWithLimit(int limit) {
    this.limit = limit;
  }

  @Override
  public boolean shouldStop() {
    return loadedCount >= limit;
  }

  @Override
  public void accept(String instantTime, GenericRecord record) {
    if (shouldStop()) {
      return;
    }
    HoodieInstant instant = readCommit(instantTime, record);
    if (instant != null) {
      synchronized (this) {
        if (loadedCount < limit) {
          addInstant(instantTime, instant);
          loadedCount++;
        }
      }
    }
  }

  /**
   * Reads a commit from the given record. Subclasses should implement this
   * to handle version-specific reading logic.
   * 
   * @param instantTime the instant time
   * @param record the generic record
   * @return the HoodieInstant, or null if the instant should not be included
   */
  protected abstract HoodieInstant readCommit(String instantTime, GenericRecord record);

  /**
   * Adds an instant to the collection. Subclasses should implement this
   * to handle version-specific storage (e.g., Map<String, HoodieInstant> vs Map<String, List<HoodieInstant>>).
   * 
   * @param instantTime the instant time
   * @param instant the HoodieInstant to add
   */
  protected abstract void addInstant(String instantTime, HoodieInstant instant);

  /**
   * Returns the collected instants as a sorted list. Subclasses should implement this
   * to handle version-specific collection logic (e.g., flattening lists in V1).
   * 
   * @return a sorted list of collected instants
   */
  public abstract List<HoodieInstant> getCollectedInstants();
}

