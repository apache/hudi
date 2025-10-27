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

package org.apache.hudi.common.model.debezium;

import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.exception.HoodieDebeziumAvroPayloadException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

import static org.apache.hudi.common.model.debezium.DebeziumConstants.FLATTENED_FILE_COL_NAME;
import static org.apache.hudi.common.model.debezium.DebeziumConstants.FLATTENED_POS_COL_NAME;

/**
 * Provides support for seamlessly applying changes captured via Debezium for MysqlDB.
 * <p>
 * Debezium change event types are determined for the op field in the payload
 * <p>
 * - For inserts, op=i
 * - For deletes, op=d
 * - For updates, op=u
 * - For snapshot inserts, op=r
 * <p>
 * This payload implementation will issue matching insert, delete, updates against the hudi table
 */
public class MySqlDebeziumAvroPayload extends AbstractDebeziumAvroPayload {

  private static final Logger LOG = LoggerFactory.getLogger(MySqlDebeziumAvroPayload.class);

  public static final String ORDERING_FIELDS = FLATTENED_FILE_COL_NAME + "," + FLATTENED_POS_COL_NAME;

  public MySqlDebeziumAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public MySqlDebeziumAvroPayload(Option<GenericRecord> record) {
    super(record);
  }

  private Option<String> extractSeq(IndexedRecord record) {
    Object value = ((GenericRecord) record).get(DebeziumConstants.ADDED_SEQ_COL_NAME);
    return Option.ofNullable(Objects.toString(value, null));
  }

  @Override
  protected boolean shouldPickCurrentRecord(IndexedRecord currentRecord, IndexedRecord insertRecord, Schema schema) throws IOException {
    String insertSourceSeq = extractSeq(insertRecord)
        .orElseThrow(() ->
            new HoodieDebeziumAvroPayloadException(String.format("%s cannot be null in insert record: %s",
                DebeziumConstants.ADDED_SEQ_COL_NAME, insertRecord)));
    Option<String> currentSourceSeqOpt = extractSeq(currentRecord);

    // handle bootstrap case
    if (!currentSourceSeqOpt.isPresent()) {
      return false;
    }
    return isCurrentSeqLatest(currentSourceSeqOpt.get(), insertSourceSeq);
  }

  @Override
  public OverwriteWithLatestAvroPayload preCombine(OverwriteWithLatestAvroPayload oldValue) {
    if (oldValue.getRecordBytes().length == 0) {
      // use natural order for delete record
      return this;
    }
    if (isCurrentSeqLatest(String.valueOf(oldValue.getOrderingValue()), String.valueOf(orderingVal))) {
      return oldValue;
    }
    return this;
  }

  @VisibleForTesting
  static boolean isCurrentSeqLatest(String currentSeq, String newSeq) {
    // Seq is file+pos string like "001.000010", getting [001,000010] from it
    String[] currentFilePos = currentSeq.split("\\.");
    String[] newFilePos = newSeq.split("\\.");

    // pick the payload with the greatest seq based on file+pos
    long currentFileNum = Long.parseLong(currentFilePos[0]);
    long newFileNum = Long.parseLong(newFilePos[0]);
    if (newFileNum < currentFileNum) {
      return true;
    } else if (newFileNum > currentFileNum) {
      return false;
    }

    // file name is the same, compare the position in the file
    long currentPos = Long.parseLong(currentFilePos[1]);
    long newPos = Long.parseLong(newFilePos[1]);
    return newPos < currentPos;
  }
}
