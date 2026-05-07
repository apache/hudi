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

package org.apache.hudi.common.schema.evolution;

import org.apache.hudi.common.schema.HoodieJsonProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaIdAssigner;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.testutils.SchemaTestUtil;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * Tests {@link HoodieSchemaEvolutionUtils} reconciliation entry points.
 */
public class TestHoodieSchemaEvolutionUtils {

  private HoodieSchema create(String name, HoodieSchemaField... fields) {
    return HoodieSchema.createRecord(name, null, null, Arrays.asList(fields));
  }

  @Test
  public void testFixNullOrdering() {
    HoodieSchema schema = SchemaTestUtil.getSchemaFromResource(TestHoodieSchemaEvolutionUtils.class, "/nullWrong.avsc");
    HoodieSchema expectedSchema = SchemaTestUtil.getSchemaFromResource(TestHoodieSchemaEvolutionUtils.class, "/nullRight.avsc");
    Assertions.assertEquals(expectedSchema, HoodieSchemaEvolutionUtils.fixNullOrdering(schema));
    Assertions.assertEquals(expectedSchema, HoodieSchemaEvolutionUtils.fixNullOrdering(expectedSchema));
  }

  @Test
  public void testFixNullOrderingSameSchemaCheck() {
    HoodieSchema schema = SchemaTestUtil.getSchemaFromResource(TestHoodieSchemaEvolutionUtils.class, "/source_evolved.avsc");
    Assertions.assertEquals(schema, HoodieSchemaEvolutionUtils.fixNullOrdering(schema));
  }

  @Test
  public void testReconcileSchema() {
    // simple schema test
    // a: boolean, b: int, c: long, d: date
    HoodieSchema schema = create("simple",
        HoodieSchemaField.of("a", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.BOOLEAN)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("b", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.INT)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("c", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.LONG)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("d", HoodieSchema.createNullable(HoodieSchema.fromAvroSchema(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)))), null, HoodieJsonProperties.NULL_VALUE));
    // a: boolean, c: long, c_1: long, d: date
    HoodieSchema incomingSchema = create("simpleIncoming",
        HoodieSchemaField.of("a", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.BOOLEAN)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("a1", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.LONG)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("c", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.LONG)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("c1", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.LONG)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("c2", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.LONG)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("d", HoodieSchema.createNullable(HoodieSchema.fromAvroSchema(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)))), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("d1", HoodieSchema.createNullable(HoodieSchema.fromAvroSchema(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)))), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("d2", HoodieSchema.createNullable(HoodieSchema.fromAvroSchema(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)))), null, HoodieJsonProperties.NULL_VALUE));

    HoodieSchema simpleCheckSchema = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"simple\",\"fields\":[{\"name\":\"a\",\"type\":[\"null\",\"boolean\"],\"default\":null,\"field-id\":0},"
        + "{\"name\":\"b\",\"type\":[\"null\",\"int\"],\"default\":null,\"field-id\":1},"
        + "{\"name\":\"c\",\"type\":[\"null\",\"long\"],\"default\":null,\"field-id\":2},"
        + "{\"name\":\"d\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}],\"default\":null,\"field-id\":3},"
        + "{\"name\":\"a1\",\"type\":[\"null\",\"long\"],\"default\":null,\"field-id\":4},"
        + "{\"name\":\"c1\",\"type\":[\"null\",\"long\"],\"default\":null,\"field-id\":5},"
        + "{\"name\":\"c2\",\"type\":[\"null\",\"long\"],\"default\":null,\"field-id\":6},"
        + "{\"name\":\"d1\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}],\"default\":null,\"field-id\":7},"
        + "{\"name\":\"d2\",\"type\":[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}],\"default\":null,\"field-id\":8}]}");

    HoodieSchema simpleReconcileSchema = HoodieSchemaEvolutionUtils.reconcileSchema(incomingSchema, schema, false);
    Assertions.assertEquals(simpleCheckSchema, simpleReconcileSchema);
  }

  @Test
  public void testNotEvolveSchemaIfReconciledSchemaUnchanged() {
    // a: boolean, c: long, c_1: long, d: date
    HoodieSchema oldSchema = create("simple",
        HoodieSchemaField.of("a", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.BOOLEAN)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("b", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.INT)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("c", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.LONG)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("d", HoodieSchema.createNullable(HoodieSchema.fromAvroSchema(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)))), null, HoodieJsonProperties.NULL_VALUE));
    // incoming schema is part of old schema
    // a: boolean, b: int, c: long
    HoodieSchema incomingSchema = create("simple",
        HoodieSchemaField.of("a", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.BOOLEAN)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("b", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.INT)), null, HoodieJsonProperties.NULL_VALUE),
        HoodieSchemaField.of("c", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.LONG)), null, HoodieJsonProperties.NULL_VALUE));

    // Stamp fresh ids on old so the no-op short-circuit can compare against
    // a schema with a known schemaId. Set schemaId=2 to verify it round-trips
    // through the no-op path.
    HoodieSchemaIdAssigner.assignFresh(oldSchema);
    oldSchema.setSchemaId(2);
    HoodieSchema evolvedSchema = HoodieSchemaEvolutionUtils.reconcileSchema(incomingSchema, oldSchema, false);
    // No type change, no missing columns to flip → the entry returns
    // oldSchema unchanged (same instance).
    Assertions.assertSame(oldSchema, evolvedSchema);
  }
}
