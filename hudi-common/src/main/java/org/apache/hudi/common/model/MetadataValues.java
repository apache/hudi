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

package org.apache.hudi.common.model;

import static org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION;

import java.util.HashMap;
import java.util.Map;
import org.apache.hudi.common.util.ValidationUtils;

public class MetadataValues {
  private Map<String, String> kv;

  public MetadataValues(Map<String, String> kv) {
    ValidationUtils.checkArgument(HOODIE_META_COLUMNS_WITH_OPERATION.containsAll(kv.values()));
    this.kv = kv;
  }

  public MetadataValues() {
    this.kv = new HashMap<>();
  }

  public void setCommitTime(String value) {
    this.kv.put(HoodieRecord.COMMIT_TIME_METADATA_FIELD, value);
  }

  public void setCommitSeqno(String value) {
    this.kv.put(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, value);
  }

  public void setRecordKey(String value) {
    this.kv.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, value);
  }

  public void setPartitionPath(String value) {
    this.kv.put(HoodieRecord.PARTITION_PATH_METADATA_FIELD, value);
  }

  public void setFileName(String value) {
    this.kv.put(HoodieRecord.FILENAME_METADATA_FIELD, value);
  }

  public void setOperation(String value) {
    this.kv.put(HoodieRecord.OPERATION_METADATA_FIELD, value);
  }

  public Map<String, String> getKv() {
    return kv;
  }
}
