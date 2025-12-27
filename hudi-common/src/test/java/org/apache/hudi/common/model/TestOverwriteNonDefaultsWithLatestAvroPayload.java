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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;

/**
 * Unit tests {@link TestOverwriteNonDefaultsWithLatestAvroPayload}.
 */
public class TestOverwriteNonDefaultsWithLatestAvroPayload {
  private HoodieSchema schema;

  @BeforeEach
  public void setUp() throws Exception {
    schema = HoodieSchema.createRecord("TestSchema", null, null, false, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING), "", null),
        HoodieSchemaField.of("partition", HoodieSchema.create(HoodieSchemaType.STRING), "", ""),
        HoodieSchemaField.of("ts", HoodieSchema.create(HoodieSchemaType.LONG), "", null),
        HoodieSchemaField.of("_hoodie_is_deleted", HoodieSchema.create(HoodieSchemaType.BOOLEAN), "", false),
        HoodieSchemaField.of("city", HoodieSchema.create(HoodieSchemaType.STRING), "", "NY"),
        HoodieSchemaField.of("child", HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.STRING)), "", Collections.emptyList())));
  }

  @Test
  public void testActiveRecords() throws IOException {
    HoodieSchema writerSchema = HoodieSchemaUtils.addMetadataFields(schema);

    GenericRecord record1 = new GenericData.Record(schema.toAvroSchema());
    record1.put("id", "1");
    record1.put("partition", "partition1");
    record1.put("ts", 0L);
    record1.put("_hoodie_is_deleted", false);
    record1.put("city", "NY0");
    record1.put("child", Collections.singletonList("A"));

    GenericRecord record2 = new GenericData.Record(schema.toAvroSchema());
    record2.put("id", "2");
    record2.put("partition", "");
    record2.put("ts", 1L);
    record2.put("_hoodie_is_deleted", false);
    record2.put("city", "NY");
    record2.put("child", Collections.emptyList());

    GenericRecord record3 = new GenericData.Record(schema.toAvroSchema());
    record3.put("id", "2");
    record3.put("partition", "partition1");
    record3.put("ts", 1L);
    record3.put("_hoodie_is_deleted", false);
    record3.put("city", "NY0");
    record3.put("child", Collections.singletonList("A"));

    // same content with record1 plus metadata fields
    GenericRecord record4 = createRecordWithMetadataFields(writerSchema, "1", "partition1");
    record4.put("id", "1");
    record4.put("partition", "partition1");
    record4.put("ts", 0L);
    record4.put("_hoodie_is_deleted", false);
    record4.put("city", "NY0");
    record4.put("child", Collections.singletonList("A"));

    // same content with record2 plus metadata fields
    GenericRecord record5 = createRecordWithMetadataFields(writerSchema, "2", "");
    record5.put("id", "2");
    record5.put("partition", "");
    record5.put("ts", 1L);
    record5.put("_hoodie_is_deleted", false);
    record5.put("city", "NY");
    record5.put("child", Collections.emptyList());

    // same content with record3 plus metadata fields
    GenericRecord record6 = createRecordWithMetadataFields(writerSchema, "2", "");
    record6.put("id", "2");
    record6.put("partition", "partition1");
    record6.put("ts", 1L);
    record6.put("_hoodie_is_deleted", false);
    record6.put("city", "NY0");
    record6.put("child", Collections.singletonList("A"));

    OverwriteNonDefaultsWithLatestAvroPayload payload1 = new OverwriteNonDefaultsWithLatestAvroPayload(record1, 1);
    OverwriteNonDefaultsWithLatestAvroPayload payload2 = new OverwriteNonDefaultsWithLatestAvroPayload(record2, 2);
    OverwriteNonDefaultsWithLatestAvroPayload payload5 = new OverwriteNonDefaultsWithLatestAvroPayload(record5, 2);
    assertEquals(payload1.preCombine(payload2), payload2);
    assertEquals(payload2.preCombine(payload1), payload2);

    assertEquals(record1, payload1.getInsertValue(schema.toAvroSchema()).get());
    assertEquals(record2, payload2.getInsertValue(schema.toAvroSchema()).get());

    IndexedRecord combinedVal1 = payload1.combineAndGetUpdateValue(record2, schema.toAvroSchema()).get();
    assertEquals(combinedVal1, record1);
    assertNotSame(combinedVal1, record1);

    IndexedRecord combinedVal2 = payload2.combineAndGetUpdateValue(record1, schema.toAvroSchema()).get();
    assertEquals(combinedVal2, record3);
    assertNotSame(combinedVal2, record3);

    // the real case in production is: the current record to be combined includes the metadata fields,
    // the payload record could include the metadata fields (for compaction) or not (for normal writer path).

    // case1: validate normal writer path
    IndexedRecord combinedVal3 = payload2.combineAndGetUpdateValue(record4, schema.toAvroSchema()).get();
    assertEquals(combinedVal3, record3);
    assertNotSame(combinedVal3, record3);

    // case2: validate compaction path
    IndexedRecord combinedVal4 = payload5.combineAndGetUpdateValue(record4, writerSchema.toAvroSchema()).get();
    assertEquals(combinedVal4, record6);
    assertNotSame(combinedVal4, record6);
  }

  @Test
  public void testDeletedRecord() throws IOException {
    GenericRecord record1 = new GenericData.Record(schema.toAvroSchema());
    record1.put("id", "1");
    record1.put("partition", "partition0");
    record1.put("ts", 0L);
    record1.put("_hoodie_is_deleted", false);
    record1.put("city", "NY0");
    record1.put("child", Collections.emptyList());

    GenericRecord delRecord1 = new GenericData.Record(schema.toAvroSchema());
    delRecord1.put("id", "2");
    delRecord1.put("partition", "partition1");
    delRecord1.put("ts", 1L);
    delRecord1.put("_hoodie_is_deleted", true);
    delRecord1.put("city", "NY0");
    delRecord1.put("child", Collections.emptyList());

    GenericRecord record2 = new GenericData.Record(schema.toAvroSchema());
    record2.put("id", "1");
    record2.put("partition", "partition0");
    record2.put("ts", 0L);
    record2.put("_hoodie_is_deleted", true);
    record2.put("city", "NY0");
    record2.put("child", Collections.emptyList());

    OverwriteNonDefaultsWithLatestAvroPayload payload1 = new OverwriteNonDefaultsWithLatestAvroPayload(record1, 1);
    OverwriteNonDefaultsWithLatestAvroPayload payload2 = new OverwriteNonDefaultsWithLatestAvroPayload(delRecord1, 2);

    assertEquals(payload1.preCombine(payload2), payload2);
    assertEquals(payload2.preCombine(payload1), payload2);

    assertEquals(record1, payload1.getInsertValue(schema.toAvroSchema()).get());
    assertFalse(payload2.getInsertValue(schema.toAvroSchema()).isPresent());

    assertEquals(payload1.combineAndGetUpdateValue(delRecord1, schema.toAvroSchema()).get(), record2);
    assertFalse(payload2.combineAndGetUpdateValue(record1, schema.toAvroSchema()).isPresent());
  }

  @Test
  public void testNullColumn() throws IOException {
    Schema avroSchema = Schema.createRecord(Arrays.asList(
            new Schema.Field("id", Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)), "", JsonProperties.NULL_VALUE),
            new Schema.Field("name", Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)), "", JsonProperties.NULL_VALUE),
            new Schema.Field("age", Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)), "", JsonProperties.NULL_VALUE),
            new Schema.Field("job", Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)), "", JsonProperties.NULL_VALUE)
            ));
    GenericRecord record1 = new GenericData.Record(avroSchema);
    record1.put("id", "1");
    record1.put("name", "aa");
    record1.put("age", "1");
    record1.put("job", "1");

    GenericRecord record2 = new GenericData.Record(avroSchema);
    record2.put("id", "1");
    record2.put("name", "bb");
    record2.put("age", "2");
    record2.put("job", null);

    GenericRecord record3 = new GenericData.Record(avroSchema);
    record3.put("id", "1");
    record3.put("name", "bb");
    record3.put("age", "2");
    record3.put("job", "1");

    OverwriteNonDefaultsWithLatestAvroPayload payload2 = new OverwriteNonDefaultsWithLatestAvroPayload(record2, 1);
    assertEquals(payload2.combineAndGetUpdateValue(record1, avroSchema).get(), record3);
  }

  private static GenericRecord createRecordWithMetadataFields(HoodieSchema schema, String recordKey, String partitionPath) {
    GenericRecord record = new GenericData.Record(schema.toAvroSchema());
    record.put(HoodieRecord.COMMIT_TIME_METADATA_FIELD, "001");
    record.put(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, "123");
    record.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, recordKey);
    record.put(HoodieRecord.PARTITION_PATH_METADATA_FIELD, partitionPath);
    record.put(HoodieRecord.FILENAME_METADATA_FIELD, "file1");
    return record;
  }
}
