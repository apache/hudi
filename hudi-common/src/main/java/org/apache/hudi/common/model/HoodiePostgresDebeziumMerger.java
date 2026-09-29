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

package org.apache.hudi.common.model;

import org.apache.hudi.common.model.debezium.DebeziumConstants;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieDebeziumAvroPayloadException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.nio.ByteBuffer;
import java.util.List;

import static org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload.DEBEZIUM_TOASTED_VALUE;
import static org.apache.hudi.common.util.StringUtils.fromUTF8Bytes;

public class HoodiePostgresDebeziumMerger extends HoodieCDCMerger {

  @Override
  protected boolean isDeleteRecord(GenericRecord record) {
    return isDeleteRecord(record, DebeziumConstants.FLATTENED_OP_COL_NAME)
        || isHoodieDeleteRecord(record);
  }

  @Override
  protected boolean shouldPickOldRecord(IndexedRecord oldRecord,
                                        IndexedRecord newRecord,
                                        Schema oldSchema,
                                        Schema newSchema) throws Exception {
    Long insertSourceLSN = extractLSN(newRecord)
        .orElseThrow(() ->
            new HoodieDebeziumAvroPayloadException(String.format("%s cannot be null in insert record: %s",
                DebeziumConstants.FLATTENED_LSN_COL_NAME, newRecord)));
    Option<Long> currentSourceLSNOpt = extractLSN(oldRecord);
    // Pick the current value in storage only if its LSN is latest compared to the LSN of the insert value
    return currentSourceLSNOpt.isPresent() && insertSourceLSN < currentSourceLSNOpt.get();
  }

  private Option<Long> extractLSN(IndexedRecord record) {
    Object value = ((GenericRecord) record).get(DebeziumConstants.FLATTENED_LSN_COL_NAME);
    return Option.ofNullable(value != null ? (Long) value : null);
  }

  @Override
  protected void postMergeOperation(IndexedRecord mergedRecord,
                                    IndexedRecord oldRecord) {
    List<Schema.Field> fields = mergedRecord.getSchema().getFields();
    fields.forEach(field -> {
      // There are only four avro data types that have unconstrained sizes, which are
      // NON-NULLABLE STRING, NULLABLE STRING, NON-NULLABLE BYTES, NULLABLE BYTES
      if (((GenericData.Record) mergedRecord).get(field.name()) != null
          && (containsStringToastedValues(mergedRecord, field) || containsBytesToastedValues(mergedRecord, field))) {
        ((GenericData.Record) mergedRecord).put(field.name(), ((GenericData.Record) oldRecord).get(field.name()));
      }
    });
  }

  /**
   * Returns true if a column is either of type string or a union of one or more strings that contain a debezium toasted value.
   *
   * @param incomingRecord The incoming avro record
   * @param field          the column of interest
   * @return
   */
  private boolean containsStringToastedValues(IndexedRecord incomingRecord, Schema.Field field) {
    return ((field.schema().getType() == Schema.Type.STRING
        || (field.schema().getType() == Schema.Type.UNION && field.schema().getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.STRING)))
        // Check length first as an optimization
        && ((CharSequence) ((GenericData.Record) incomingRecord).get(field.name())).length() == DEBEZIUM_TOASTED_VALUE.length()
        && DEBEZIUM_TOASTED_VALUE.equals(((CharSequence) ((GenericData.Record) incomingRecord).get(field.name())).toString()));
  }

  /**
   * Returns true if a column is either of type bytes or a union of one or more bytes that contain a debezium toasted value.
   *
   * @param incomingRecord The incoming avro record
   * @param field          the column of interest
   * @return
   */
  private boolean containsBytesToastedValues(IndexedRecord incomingRecord, Schema.Field field) {
    return ((field.schema().getType() == Schema.Type.BYTES
        || (field.schema().getType() == Schema.Type.UNION && field.schema().getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.BYTES)))
        // Check length first as an optimization
        && ((ByteBuffer) ((GenericData.Record) incomingRecord).get(field.name())).array().length == DEBEZIUM_TOASTED_VALUE.length()
        && DEBEZIUM_TOASTED_VALUE.equals(fromUTF8Bytes(((ByteBuffer) ((GenericData.Record) incomingRecord).get(field.name())).array())));
  }
}
