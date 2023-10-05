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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieDebeziumAvroPayloadException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

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

    // Seq is file+pos string like "001.000010", getting [001,000010] from it
    String[] currentFilePos = currentSourceSeqOpt.get().split("\\.");
    String[] insertFilePos = insertSourceSeq.split("\\.");

    long currentFileNum = Long.valueOf(currentFilePos[0]);
    long insertFileNum = Long.valueOf(insertFilePos[0]);

    if (insertFileNum < currentFileNum) {
      // pick the current value
      return true;
    } else if (insertFileNum > currentFileNum) {
      // pick the insert value
      return false;
    }

    // file name is the same, compare the position in the file
    Long currentPos = Long.valueOf(currentFilePos[1]);
    Long insertPos = Long.valueOf(insertFilePos[1]);

    return insertPos <= currentPos;
  }
}
