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

import org.apache.hudi.common.model.HoodiePayloadProps;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Shared fixtures for the Debezium payload configured-ordering tests: one schema/record/props/assert
 * builder set, parameterized by the connector ordering column, so the MySQL and Postgres suites
 * cannot drift.
 */
final class DebeziumOrderingTestFixtures {

  static final String KEY_FIELD = "Key";
  static final String ORDERING_FIELD = "event_ts";
  static final String SECOND_ORDERING_FIELD = "event_ts2";

  private DebeziumOrderingTestFixtures() {
  }

  static Schema schemaWithOrderingField(String connectorColumn, Schema.Type connectorType) {
    return Schema.createRecord("test_ordering", null, "test_namespace", false, Arrays.asList(
        new Schema.Field(KEY_FIELD, Schema.create(Schema.Type.INT), "", 0),
        new Schema.Field(DebeziumConstants.FLATTENED_OP_COL_NAME,
            Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)), "", null),
        new Schema.Field(connectorColumn,
            Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(connectorType)), "", null),
        new Schema.Field(ORDERING_FIELD,
            Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG)), "", null),
        new Schema.Field(SECOND_ORDERING_FIELD,
            Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.LONG)), "", null)
    ));
  }

  static GenericRecord recordWithOrdering(Schema schema, int key, @Nullable Object op, String connectorColumn,
                                          @Nullable Object connectorValue, @Nullable Long orderingValue) {
    GenericRecord record = new GenericData.Record(schema);
    record.put(KEY_FIELD, key);
    record.put(DebeziumConstants.FLATTENED_OP_COL_NAME, Objects.toString(op, null));
    record.put(connectorColumn, connectorValue);
    record.put(ORDERING_FIELD, orderingValue);
    return record;
  }

  static GenericRecord recordWithCompositeOrdering(Schema schema, int key, @Nullable Object op, String connectorColumn,
                                                   @Nullable Object connectorValue, @Nullable Long orderingValue,
                                                   @Nullable Long secondOrderingValue) {
    GenericRecord record = recordWithOrdering(schema, key, op, connectorColumn, connectorValue, orderingValue);
    record.put(SECOND_ORDERING_FIELD, secondOrderingValue);
    return record;
  }

  static Properties orderingProps(String orderingField) {
    Properties props = new Properties();
    props.setProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, orderingField);
    return props;
  }

  /**
   * Asserts every field of a merged ordering-schema record, so a regression that returns the right
   * ordering value on the wrong row (wrong key / op / connector value) cannot pass.
   */
  static void validateOrderingRecord(Option<IndexedRecord> merged, int key, @Nullable Object op, String connectorColumn,
                                     @Nullable Object connectorValue, @Nullable Long orderingValue) {
    GenericRecord record = (GenericRecord) merged.get();
    assertEquals(key, record.get(KEY_FIELD));
    assertEquals(Objects.toString(op, null), Objects.toString(record.get(DebeziumConstants.FLATTENED_OP_COL_NAME), null));
    assertEquals(Objects.toString(connectorValue, null), Objects.toString(record.get(connectorColumn), null));
    assertEquals(orderingValue, record.get(ORDERING_FIELD));
  }
}
