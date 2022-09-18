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

import org.apache.avro.Schema;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.spark.sql.catalyst.CatalystTypeConverters;
import org.apache.spark.sql.catalyst.InternalRow;

import java.io.IOException;
import java.util.Properties;

import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.COMMIT_SEQNO_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.COMMIT_TIME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.FILENAME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.PARTITION_PATH_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.RECORD_KEY_METADATA_FIELD;

public interface HoodieSparkFileWriter extends HoodieFileWriter {
  boolean canWrite();

  void close() throws IOException;

  void writeRowWithMetadata(HoodieKey recordKey, InternalRow row) throws IOException;

  void writeRow(String recordKey, InternalRow row) throws IOException;

  @Override
  default void write(String recordKey, HoodieRecord record, Schema schema, Properties props) throws IOException {
    writeRow(recordKey, (InternalRow) record.getData());
  }

  @Override
  default void writeWithMetadata(HoodieKey key, HoodieRecord record, Schema schema, Properties props) throws IOException {
    writeRowWithMetadata(key, (InternalRow) record.getData());
  }

  default InternalRow prepRecordWithMetadata(HoodieKey key, InternalRow row, String instantTime, Integer partitionId, long recordIndex, String fileName)  {
    String seqId = HoodieRecord.generateSequenceId(instantTime, partitionId, recordIndex);
    row.update(COMMIT_TIME_METADATA_FIELD.ordinal(), CatalystTypeConverters.convertToCatalyst(instantTime));
    row.update(COMMIT_SEQNO_METADATA_FIELD.ordinal(), CatalystTypeConverters.convertToCatalyst(seqId));
    row.update(RECORD_KEY_METADATA_FIELD.ordinal(), CatalystTypeConverters.convertToCatalyst(key.getRecordKey()));
    row.update(PARTITION_PATH_METADATA_FIELD.ordinal(), CatalystTypeConverters.convertToCatalyst(key.getPartitionPath()));
    row.update(FILENAME_METADATA_FIELD.ordinal(), CatalystTypeConverters.convertToCatalyst(fileName));
    return row;
  }
}
