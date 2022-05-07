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

package org.apache.hudi.io.storage;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;

import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;

public interface HoodieFileWriter<R extends IndexedRecord> {

  void writeAvroWithMetadata(HoodieKey key, R newRecord) throws IOException;

  boolean canWrite();

  void close() throws IOException;

  void writeAvro(String key, R oldRecord) throws IOException;

  default void prepRecordWithMetadata(HoodieKey key, R avroRecord, String instantTime, Integer partitionId, AtomicLong recordIndex, String fileName) {
    String seqId = HoodieRecord.generateSequenceId(instantTime, partitionId, recordIndex.getAndIncrement());
    HoodieAvroUtils.addHoodieKeyToRecord((GenericRecord) avroRecord, key.getRecordKey(), key.getPartitionPath(), fileName);
    HoodieAvroUtils.addCommitMetadataToRecord((GenericRecord) avroRecord, instantTime, seqId);
    return;
  }
}
