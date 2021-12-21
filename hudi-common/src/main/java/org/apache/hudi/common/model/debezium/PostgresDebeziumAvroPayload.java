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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Provides support for seamlessly applying changes captured via Debezium for PostgresDB.
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
public class PostgresDebeziumAvroPayload extends AbstractDebeziumAvroPayload {

  private static final Logger LOG = LogManager.getLogger(PostgresDebeziumAvroPayload.class);
  public static final String DEBEZIUM_TOASTED_VALUE = "__debezium_unavailable_value";

  public PostgresDebeziumAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public PostgresDebeziumAvroPayload(Option<GenericRecord> record) {
    super(record);
  }

  private Long extractLSN(IndexedRecord record) {
    GenericRecord genericRecord = (GenericRecord) record;
    return (Long) genericRecord.get(DebeziumConstants.FLATTENED_LSN_COL_NAME);
  }

  @Override
  protected boolean shouldPickCurrentRecord(IndexedRecord currentRecord, IndexedRecord insertRecord, Schema schema) throws IOException {
    Long currentSourceLSN = extractLSN(currentRecord);
    Long insertSourceLSN = extractLSN(insertRecord);

    // Pick the current value in storage only if its LSN is latest compared to the LSN of the insert value
    return insertSourceLSN < currentSourceLSN;
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    // Specific to Postgres: If the updated record has TOASTED columns,
    // we will need to keep the previous value for those columns
    // see https://debezium.io/documentation/reference/connectors/postgresql.html#postgresql-toasted-values
    Option<IndexedRecord> insertOrDeleteRecord = super.combineAndGetUpdateValue(currentValue, schema);

    if (insertOrDeleteRecord.isPresent()) {
      mergeToastedValuesIfPresent(insertOrDeleteRecord.get(), currentValue);
    }
    return insertOrDeleteRecord;
  }

  private void mergeToastedValuesIfPresent(IndexedRecord incomingRecord, IndexedRecord currentRecord) {
    List<Schema.Field> fields = incomingRecord.getSchema().getFields();

    fields.forEach(field -> {
      // There are only four avro data types that have unconstrained sizes, which are
      // NON-NULLABLE STRING, NULLABLE STRING, NON-NULLABLE BYTES, NULLABLE BYTES
      if (((GenericData.Record) incomingRecord).get(field.name()) != null
          && (containsStringToastedValues(incomingRecord, field) || containsBytesToastedValues(incomingRecord, field))) {
        ((GenericData.Record) incomingRecord).put(field.name(), ((GenericData.Record) currentRecord).get(field.name()));
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
        && DEBEZIUM_TOASTED_VALUE.equals(new String(((ByteBuffer) ((GenericData.Record) incomingRecord).get(field.name())).array(), StandardCharsets.UTF_8)));
  }
}

