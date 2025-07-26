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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import java.io.IOException;

/**
 * A {@link BufferedRecord} merger that covers three core merging scenarios within {@link org.apache.hudi.common.table.read.buffer.FileGroupRecordBuffer}:
 *
 * <ol>
 *   <li>log & log merging;</li>
 *   <li>log & delete record merging;</li>
 *   <li>base & log merging.</li>
 * </ol>
 */
public interface BufferedRecordMerger<T> {

  /**
   * Merges incoming record from log file with existing record in record buffer.
   *
   * @param newRecord      Incoming record from log file
   * @param existingRecord Existing record in record buffer
   *
   * @return The merged record.
   */
  Option<BufferedRecord<T>> deltaMerge(BufferedRecord<T> newRecord, BufferedRecord<T> existingRecord) throws IOException;

  /**
   * Merges incoming delete record from log file with existing record in record buffer.
   *
   * @param deleteRecord   Incoming delete record from log file
   * @param existingRecord Existing record in record buffer
   *
   * @return The merged record.
   */
  Option<DeleteRecord> deltaMerge(DeleteRecord deleteRecord, BufferedRecord<T> existingRecord);

  /**
   * Merges newer record from log file with old record from base file.
   *
   * @param olderRecord Older record from base file
   * @param newerRecord Newer record from log file
   *
   * @return The merged record.
   */
  Pair<Boolean, T> finalMerge(BufferedRecord<T> olderRecord, BufferedRecord<T> newerRecord) throws IOException;
}
