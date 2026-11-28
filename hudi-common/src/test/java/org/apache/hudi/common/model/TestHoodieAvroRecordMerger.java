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

package org.apache.hudi.common.model;

import org.apache.hudi.common.avro.AvroRecordContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that {@link HoodieAvroRecordMerger} and {@link HoodiePreCombineAvroRecordMerger} report a delete
 * exactly when the merged row is a delete.
 */
public class TestHoodieAvroRecordMerger {
  private static final HoodieSchema SCHEMA = HoodieSchema.parse("{\"type\": \"record\", \"name\": \"markerRecord\", \"fields\": ["
      + "{\"name\": \"id\", \"type\": [\"null\", \"string\"], \"default\": null},"
      + "{\"name\": \"ts\", \"type\": [\"null\", \"long\"], \"default\": null},"
      + "{\"name\": \"_hoodie_is_deleted\", \"type\": [\"null\", \"boolean\"], \"default\": null},"
      + "{\"name\": \"city\", \"type\": [\"null\", \"string\"], \"default\": null}]}");

  private final AvroRecordContext recordContext = new AvroRecordContext();

  @Test
  public void testMergedLiveRecordDoesNotInheritFromOlderDelete() throws IOException {
    BufferedRecord<IndexedRecord> olderDelete = bufferedRecord(SCHEMA, 1L, true, "NY0");
    BufferedRecord<IndexedRecord> newerLive = bufferedRecord(SCHEMA, 2L, null, null);

    BufferedRecord<IndexedRecord> merged = new HoodieAvroRecordMerger()
        .merge(olderDelete, newerLive, recordContext, mergeProps(PartialUpdateAvroPayload.class));

    assertFalse(merged.isDelete());
    assertEquals(record(SCHEMA, 2L, null, null), merged.getRecord());
  }

  @Test
  public void testMergeReportsDeleteWhenPayloadReturnsDeleteRow() throws IOException {
    BufferedRecord<IndexedRecord> olderDelete = bufferedRecord(SCHEMA, 1L, true, "NY0");
    BufferedRecord<IndexedRecord> newerLive = bufferedRecord(SCHEMA, 2L, null, null);

    BufferedRecord<IndexedRecord> merged = new HoodieAvroRecordMerger()
        .merge(olderDelete, newerLive, recordContext, mergeProps(KeepCurrentValuePayload.class));

    assertTrue(merged.isDelete());
    assertEquals(record(SCHEMA, 1L, true, "NY0"), merged.getRecord());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPreCombineLiveRecordDoesNotInheritFromOlderDelete(boolean withMetaFields) throws IOException {
    HoodieSchema schema = withMetaFields ? HoodieSchemaUtils.addMetadataFields(SCHEMA) : SCHEMA;
    BufferedRecord<IndexedRecord> olderDelete = bufferedRecord(schema, 1L, true, "NY0");
    BufferedRecord<IndexedRecord> newerLive = bufferedRecord(schema, 2L, null, null);
    TypedProperties props = mergeProps(PartialUpdateAvroPayload.class);

    // reduceByKey may hand the two copies over in either order
    for (BufferedRecord<IndexedRecord> merged : Arrays.asList(
        new HoodiePreCombineAvroRecordMerger().merge(olderDelete, newerLive, recordContext, props),
        new HoodiePreCombineAvroRecordMerger().merge(newerLive, olderDelete, recordContext, props))) {
      assertFalse(merged.isDelete());
      assertEquals(record(schema, 2L, null, null), merged.getRecord());
      assertEquals(2L, merged.getOrderingValue());
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPreCombineNewerDeleteWins(boolean withMetaFields) throws IOException {
    HoodieSchema schema = withMetaFields ? HoodieSchemaUtils.addMetadataFields(SCHEMA) : SCHEMA;
    BufferedRecord<IndexedRecord> olderLive = bufferedRecord(schema, 1L, null, "NY0");
    BufferedRecord<IndexedRecord> newerDelete = bufferedRecord(schema, 2L, true, null);
    TypedProperties props = mergeProps(PartialUpdateAvroPayload.class);

    for (BufferedRecord<IndexedRecord> merged : Arrays.asList(
        new HoodiePreCombineAvroRecordMerger().merge(olderLive, newerDelete, recordContext, props),
        new HoodiePreCombineAvroRecordMerger().merge(newerDelete, olderLive, recordContext, props))) {
      assertTrue(merged.isDelete());
      assertEquals(2L, merged.getOrderingValue());
    }
  }

  private BufferedRecord<IndexedRecord> bufferedRecord(HoodieSchema schema, long ts, Boolean isDeleted, String city) {
    return BufferedRecords.fromEngineRecord(record(schema, ts, isDeleted, city), schema, recordContext, ts, "1", Boolean.TRUE.equals(isDeleted));
  }

  private static GenericRecord record(HoodieSchema schema, long ts, Boolean isDeleted, String city) {
    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put("id", "1");
    record.put("ts", ts);
    record.put(HoodieRecord.HOODIE_IS_DELETED_FIELD, isDeleted);
    record.put("city", city);
    return record;
  }

  private static TypedProperties mergeProps(Class<?> payloadClass) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieAvroRecordMerger.PAYLOAD_CLASS_PROP, payloadClass.getName());
    props.setProperty(HoodieTableConfig.ORDERING_FIELDS.key(), "ts");
    return props;
  }

  /**
   * Returns a copy of the stored row, so a stored delete row becomes the merge result.
   */
  public static class KeepCurrentValuePayload extends OverwriteWithLatestAvroPayload {
    public KeepCurrentValuePayload(GenericRecord record, Comparable orderingVal) {
      super(record, orderingVal);
    }

    @Override
    public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties properties) {
      return Option.of(GenericData.get().deepCopy(schema, currentValue));
    }
  }
}
