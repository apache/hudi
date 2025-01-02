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

package org.apache.hudi.common.util;

import org.apache.hudi.common.testutils.HoodieTestDataGenerator;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestAvroSchemaCache {

  @Test
  public void testBasicCacheUsage() {
    AvroSchemaCache avroSchemaCache = AvroSchemaCache.getInstance();
    Integer avroSchemaCacheNum = avroSchemaCache.cacheSchema(HoodieTestDataGenerator.AVRO_SCHEMA);
    Integer avroTripSchemaCacheNum = avroSchemaCache.cacheSchema(HoodieTestDataGenerator.AVRO_TRIP_SCHEMA);
    Integer flatAvroSchemaCacheNum = avroSchemaCache.cacheSchema(HoodieTestDataGenerator.FLATTENED_AVRO_SCHEMA);
    Integer nestAvroSchemaCacheNum = avroSchemaCache.cacheSchema(HoodieTestDataGenerator.NESTED_AVRO_SCHEMA);
    Set<Integer> uniqueSet = new HashSet<>(Arrays.asList(avroSchemaCacheNum, avroTripSchemaCacheNum, flatAvroSchemaCacheNum, nestAvroSchemaCacheNum));
    assertEquals(4, uniqueSet.size());
    assertTrue(avroSchemaCache.getSchema(avroSchemaCacheNum).isPresent());
    assertEquals(HoodieTestDataGenerator.AVRO_SCHEMA, avroSchemaCache.getSchema(avroSchemaCacheNum).get());
    assertTrue(avroSchemaCache.getSchema(avroTripSchemaCacheNum).isPresent());
    assertEquals(HoodieTestDataGenerator.AVRO_TRIP_SCHEMA, avroSchemaCache.getSchema(avroTripSchemaCacheNum).get());
    assertTrue(avroSchemaCache.getSchema(flatAvroSchemaCacheNum).isPresent());
    assertEquals(HoodieTestDataGenerator.FLATTENED_AVRO_SCHEMA, avroSchemaCache.getSchema(flatAvroSchemaCacheNum).get());
    assertTrue(avroSchemaCache.getSchema(nestAvroSchemaCacheNum).isPresent());
    assertEquals(HoodieTestDataGenerator.NESTED_AVRO_SCHEMA, avroSchemaCache.getSchema(nestAvroSchemaCacheNum).get());
  }

  @Test
  public void testCopiesOfSameSchema() {
    AvroSchemaCache avroSchemaCache = AvroSchemaCache.getInstance();
    Schema testSchema1 = new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);
    Schema testSchema2 = new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);
    Integer cachenum = avroSchemaCache.cacheSchema(testSchema1);
    Integer secondSchemaCacheNum = avroSchemaCache.cacheSchema(testSchema2);
    assertEquals(cachenum, secondSchemaCacheNum);
    assertTrue(avroSchemaCache.getSchema(cachenum).isPresent());
    assertEquals(testSchema1, avroSchemaCache.getSchema(cachenum).get());
  }
}
