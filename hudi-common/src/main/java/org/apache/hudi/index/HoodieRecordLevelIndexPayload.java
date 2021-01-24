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

package org.apache.hudi.index;

import org.apache.hudi.avro.model.HoodieRecordLevelIndexRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;

/**
 * Payload used in index table for Hoodie Record level index.
 */
public class HoodieRecordLevelIndexPayload implements HoodieRecordPayload<HoodieRecordLevelIndexPayload> {

  private String key;
  private String partitionPath;
  private String instantTime;
  private String fileId;

  public HoodieRecordLevelIndexPayload(Option<GenericRecord> record) {
    if (record.isPresent()) {
      // This can be simplified using SpecificData.deepcopy once this bug is fixed
      // https://issues.apache.org/jira/browse/AVRO-1811
      key = record.get().get("key").toString();
      partitionPath = record.get().get("partitionPath").toString();
      instantTime = record.get().get("instantTime").toString();
      fileId = record.get().get("fileId").toString();
    }
  }

  private HoodieRecordLevelIndexPayload(String key, String partitionPath, String instantTime, String fileId) {
    this.key = key;
    this.partitionPath = partitionPath;
    this.instantTime = instantTime;
    this.fileId = fileId;
  }

  @Override
  public HoodieRecordLevelIndexPayload preCombine(HoodieRecordLevelIndexPayload another) {
    if (this.instantTime.compareTo(another.instantTime) >= 0) {
      return this;
    } else {
      return another;
    }
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    return getInsertValue(schema);
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    if (key == null) {
      return Option.empty();
    }

    HoodieRecordLevelIndexRecord record = new HoodieRecordLevelIndexRecord(key, partitionPath, instantTime, fileId);
    return Option.of(record);
  }

  public String getKey() {
    return key;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public String getInstantTime() {
    return instantTime;
  }

  public String getFileId() {
    return fileId;
  }
}
