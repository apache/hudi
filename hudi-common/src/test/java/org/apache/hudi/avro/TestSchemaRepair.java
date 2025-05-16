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

package org.apache.hudi.avro;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestSchemaRepair {

  private final Schema longSchema = Schema.create(Schema.Type.LONG);
  private final Schema milliSchema = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
  private final Schema microSchema = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));

  @Test
  public void testRepairTimestampMillisToMicros() {
    Schema result = HoodieAvroUtils.repairSchema(milliSchema, microSchema);
    assertEquals(Schema.Type.LONG, result.getType());
    assertEquals(LogicalTypes.timestampMicros(), result.getLogicalType());
  }

  @Test
  public void testNoChangeIfLogicalTypesAreSame() {
    Schema result = HoodieAvroUtils.repairSchema(milliSchema, milliSchema);
    assertEquals(milliSchema, result);

    result = HoodieAvroUtils.repairSchema(microSchema, microSchema);
    assertEquals(microSchema, result);
  }

  @Test
  public void testNoRepairForNonTimestampLogicalTypes() {
    Schema result = HoodieAvroUtils.repairSchema(longSchema, Schema.create(Schema.Type.INT));
    assertEquals(longSchema, result);
  }

  @Test
  void testRecordTypeRepair() {
    Schema source = Schema.createRecord("TestRecord", null, null, false);
    Schema target = Schema.createRecord("TestRecord", null, null, false);

    Schema.Field sourceField = new Schema.Field("id", microSchema, null, null);
    Schema.Field targetField = new Schema.Field("id", milliSchema, null, null);

    source.setFields(Collections.singletonList(sourceField));
    target.setFields(Collections.singletonList(targetField));

    Schema result = HoodieAvroUtils.repairSchema(source, target);

    assertEquals("TestRecord", result.getName());
    assertEquals(Schema.Type.LONG, result.getField("id").schema().getType());
    assertEquals(LogicalTypes.timestampMillis(), result.getField("id").schema().getLogicalType());
  }

  @Test
  void testArrayTypeRepair() {
    Schema source = Schema.createArray(milliSchema);
    Schema target = Schema.createArray(microSchema);

    Schema result = HoodieAvroUtils.repairSchema(source, target);
    assertEquals(Schema.Type.ARRAY, result.getType());
    assertEquals(Schema.Type.LONG, result.getElementType().getType());
    assertEquals(LogicalTypes.timestampMicros(), result.getElementType().getLogicalType());
  }

  @Test
  void testMapTypeRepair() {
    Schema source = Schema.createMap(microSchema);
    Schema target = Schema.createMap(milliSchema);

    Schema result = HoodieAvroUtils.repairSchema(source, target);
    assertEquals(Schema.Type.MAP, result.getType());
    assertEquals(Schema.Type.LONG, result.getValueType().getType());
    assertEquals(LogicalTypes.timestampMillis(), result.getValueType().getLogicalType());
  }

  @Test
  void testUnionTypeRepair() {
    Schema source = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), milliSchema));
    Schema target = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), microSchema));

    Schema result = HoodieAvroUtils.repairSchema(source, target);
    assertEquals(2, result.getTypes().size());
    assertEquals(Schema.Type.NULL, result.getTypes().get(0).getType());
    assertEquals(Schema.Type.LONG, result.getTypes().get(1).getType());
    assertEquals(LogicalTypes.timestampMicros(), result.getTypes().get(1).getLogicalType());
  }

  @Test
  void testFieldMissingThrowsException() {
    Schema source = Schema.createRecord("Test", null, null, false);
    Schema target = Schema.createRecord("Test", null, null, false);

    source.setFields(Collections.singletonList(new Schema.Field("fieldA", Schema.create(Schema.Type.STRING), null, null)));
    target.setFields(Collections.emptyList()); // Missing field

    assertThrows(IllegalArgumentException.class, () -> {
      HoodieAvroUtils.repairSchema(source, target);
    });
  }
}
