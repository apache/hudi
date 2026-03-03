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

package org.apache.hudi.testutils;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility to capture write results in tests
 */
public class HoodieTestWriteResult {
  private final List<WriteStatus> statuses;
  private final List<HoodieRecord> records;
  private final Set<String> recordKeys;

  public HoodieTestWriteResult(List<WriteStatus> statuses, List<HoodieRecord> records) {
    this(statuses, records, records.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toSet()));
  }

  public HoodieTestWriteResult(List<WriteStatus> statuses, List<HoodieRecord> records, Set<String> recordKeys) {
    this.statuses = statuses;
    this.records = records;
    this.recordKeys = recordKeys;
  }

  public List<WriteStatus> getStatuses() {
    return statuses;
  }

  public List<HoodieRecord> getRecords() {
    return records;
  }

  public Set<String> getRecordKeys() {
    return recordKeys;
  }
}
