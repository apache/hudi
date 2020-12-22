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

package org.apache.hudi.common.util;

import org.apache.avro.Schema;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests serializable schema.
 */
public class TestSerializableSchema {

  @Test
  public void testSerDeser() throws IOException {
    verifySchema(HoodieTestDataGenerator.AVRO_TRIP_SCHEMA);
    verifySchema(HoodieAvroUtils.addMetadataFields(HoodieTestDataGenerator.AVRO_TRIP_SCHEMA));
    verifySchema(HoodieTestDataGenerator.AVRO_SHORT_TRIP_SCHEMA);
    verifySchema(HoodieAvroUtils.addMetadataFields(HoodieTestDataGenerator.AVRO_SHORT_TRIP_SCHEMA));
    verifySchema(HoodieTestDataGenerator.FLATTENED_AVRO_SCHEMA);
    verifySchema(HoodieAvroUtils.addMetadataFields(HoodieTestDataGenerator.FLATTENED_AVRO_SCHEMA));
    verifySchema(HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS);
  }
  
  private void verifySchema(Schema schema) throws IOException {
    SerializableSchema serializableSchema = new SerializableSchema(schema);
    assertEquals(schema, serializableSchema.get());
    assertTrue(schema != serializableSchema.get());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(baos);
    serializableSchema.writeObjectTo(oos);
    oos.flush();
    oos.close();
    
    byte[] bytesWritten = baos.toByteArray();
    SerializableSchema newSchema = new SerializableSchema();
    newSchema.readObjectFrom(new ObjectInputStream(new ByteArrayInputStream(bytesWritten)));
    assertEquals(schema, newSchema.get());
  }
}
