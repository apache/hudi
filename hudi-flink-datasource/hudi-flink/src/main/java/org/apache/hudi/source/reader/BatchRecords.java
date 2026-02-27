/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.source.reader;

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;

import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

/**
 * Implementation of RecordsWithSplitIds with a list record inside.
 *
 * Type parameters: <T> – record type
 */
public class BatchRecords<T> implements RecordsWithSplitIds<HoodieRecordWithPosition<T>> {
  private String splitId;
  private String nextSprintId;
  private final ClosableIterator<T> recordIterator;
  private final Set<String> finishedSplits;
  private final HoodieRecordWithPosition<T> recordAndPosition;

  // point to current read position within the records list
  private int position;

  BatchRecords(
      String splitId,
      ClosableIterator<T> recordIterator,
      int fileOffset,
      long startingRecordOffset,
      Set<String> finishedSplits) {
    ValidationUtils.checkArgument(
        finishedSplits != null, "finishedSplits can be empty but not null");
    ValidationUtils.checkArgument(
        recordIterator != null, "recordIterator can be empty but not null");

    this.splitId = splitId;
    this.nextSprintId = splitId;
    this.recordIterator = recordIterator;
    this.finishedSplits = finishedSplits;
    this.recordAndPosition = new HoodieRecordWithPosition<>();
    this.recordAndPosition.set(null, fileOffset, startingRecordOffset);
    this.position = 0;
  }

  @Nullable
  @Override
  public String nextSplit() {
    if (splitId.equals(nextSprintId)) {
      // set the nextSprintId to null to indicate no more splits
      // this class only contains record for one split
      nextSprintId = null;
      return splitId;
    } else {
      return nextSprintId;
    }
  }

  @Nullable
  @Override
  public HoodieRecordWithPosition<T> nextRecordFromSplit() {
    if (recordIterator.hasNext()) {
      recordAndPosition.record(recordIterator.next());
      position = position + 1;
      return recordAndPosition;
    } else {
      finishedSplits.add(splitId);
      recordIterator.close();
      return null;
    }
  }

  @Override
  public Set<String> finishedSplits() {
    return finishedSplits;
  }

  @Override
  public void recycle() {
    if (recordIterator != null) {
      recordIterator.close();
    }
  }

  public void seek(long startingRecordOffset) {
    for (long i = 0; i < startingRecordOffset; ++i) {
      if (recordIterator.hasNext()) {
        position = position + 1;
        recordIterator.next();
      } else {
        throw new IllegalStateException(
            String.format(
                "Invalid starting record offset %d for split %s",
                startingRecordOffset,
                splitId));
      }
    }
  }

  public static <T> BatchRecords<T> forRecords(
      String splitId, ClosableIterator<T> recordIterator, int fileOffset, long startingRecordOffset) {

    return new BatchRecords<>(
        splitId, recordIterator, fileOffset, startingRecordOffset, new HashSet<>());
  }
}