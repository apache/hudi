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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Provides support for seamlessly applying changes captured via Debezium for MysqlDB.
 * <p>
 * Debezium change event types are determined for the op field in the payload
 * <p>
 * - For inserts, op=i
 * - For deletes, op=d
 * - For updates, op=u
 * - For snapshort inserts, op=r
 * <p>
 * This payload implementation will issue matching insert, delete, updates against the hudi table
 */
public class MySqlDebeziumAvroPayload extends AbstractDebeziumAvroPayload {

  private static final Logger LOG = LogManager.getLogger(MySqlDebeziumAvroPayload.class);

  public MySqlDebeziumAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public MySqlDebeziumAvroPayload(Option<GenericRecord> record) {
    super(record);
  }

  private String extractSeq(IndexedRecord record) {
    return ((CharSequence) ((GenericRecord) record).get(DebeziumConstants.ADDED_SEQ_COL_NAME)).toString();
  }

  @Override
  protected boolean shouldPickCurrentRecord(IndexedRecord currentRecord, IndexedRecord insertRecord, Schema schema) throws IOException {
    String currentSourceSeq = extractSeq(currentRecord);
    String insertSourceSeq = extractSeq(insertRecord);
    // Pick the current value in storage only if its Seq (file+pos) is latest
    // compared to the Seq (file+pos) of the insert value
    return insertSourceSeq.compareTo(currentSourceSeq) < 0;
  }
}
