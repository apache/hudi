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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.schema.HoodieSchema;

import java.io.IOException;
import java.util.Properties;

public interface HoodieAvroFileWriter extends HoodieFileWriter {

  boolean canWrite();

  void close() throws IOException;

  void writeAvroWithMetadata(HoodieKey key, IndexedRecord avroRecord) throws IOException;

  void writeAvro(String recordKey, IndexedRecord record) throws IOException;

  @Override
  default void writeWithMetadata(HoodieKey key, HoodieRecord record, HoodieSchema schema, Properties props) throws IOException {
    IndexedRecord avroPayload = record.toIndexedRecord(schema.getAvroSchema(), props).get().getData();
    writeAvroWithMetadata(key, avroPayload);
  }

  @Override
  default void write(String recordKey, HoodieRecord record, HoodieSchema schema, Properties props) throws IOException {
    IndexedRecord avroPayload = record.toIndexedRecord(schema.getAvroSchema(), props).get().getData();
    writeAvro(recordKey, avroPayload);
  }

  default void prepRecordWithMetadata(HoodieKey key, IndexedRecord avroRecord, String instantTime, Integer partitionId, long recordIndex, String fileName) {
    String seqId = HoodieRecord.generateSequenceId(instantTime, partitionId, recordIndex);
    HoodieAvroUtils.addHoodieKeyToRecord((GenericRecord) avroRecord, key.getRecordKey(), key.getPartitionPath(), fileName);
    HoodieAvroUtils.addCommitMetadataToRecord((GenericRecord) avroRecord, instantTime, seqId);
  }
}
