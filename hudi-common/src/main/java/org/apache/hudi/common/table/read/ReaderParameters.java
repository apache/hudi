/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.table.read;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/**
 * Parameters for how the reader should process the FileGroup while reading.
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Builder
public class ReaderParameters {

  // Rely on the position of the record in the file instead of the record keys while merging data between base and log files
  @Getter(AccessLevel.NONE)
  @Builder.Default
  private final boolean shouldUseRecordPosition = false;
  // Whether to emit delete records while reading
  @Builder.Default
  private final boolean emitDeletes = false;
  // Whether to sort the output records while reading, this implicitly requires the base file to be sorted
  @Builder.Default
  private final boolean sortOutputs = false;
  // Allows to consider inflight instants while merging log records using HoodieMergedLogRecordReader
  // The inflight instants need to be considered while updating RLI records. RLI needs to fetch the revived
  // and deleted keys from the log files written as part of active data commit. During the RLI update,
  // the inflightInstantsAllowed flag would need to be set to true. This would ensure the HoodieMergedLogRecordReader
  // considers the log records which are inflight.
  @Builder.Default
  private final boolean inflightInstantsAllowed = false;

  public boolean shouldUseRecordPosition() {
    return shouldUseRecordPosition;
  }
}
