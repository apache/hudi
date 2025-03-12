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

public class TestLocalAvroSchemaCache {

  @Test
  public void testBasicCacheUsage() {
    LocalAvroSchemaCache localAvroSchemaCache = LocalAvroSchemaCache.getInstance();
    Integer avroSchemaCacheNum = localAvroSchemaCache.cacheSchema(HoodieTestDataGenerator.AVRO_SCHEMA);
    Integer avroTripSchemaCacheNum = localAvroSchemaCache.cacheSchema(HoodieTestDataGenerator.AVRO_TRIP_SCHEMA);
    Integer flatAvroSchemaCacheNum = localAvroSchemaCache.cacheSchema(HoodieTestDataGenerator.FLATTENED_AVRO_SCHEMA);
    Integer nestAvroSchemaCacheNum = localAvroSchemaCache.cacheSchema(HoodieTestDataGenerator.NESTED_AVRO_SCHEMA);
    Set<Integer> uniqueSet = new HashSet<>(Arrays.asList(avroSchemaCacheNum, avroTripSchemaCacheNum, flatAvroSchemaCacheNum, nestAvroSchemaCacheNum));
    assertEquals(4, uniqueSet.size());
    assertTrue(localAvroSchemaCache.getSchema(avroSchemaCacheNum).isPresent());
    assertEquals(HoodieTestDataGenerator.AVRO_SCHEMA, localAvroSchemaCache.getSchema(avroSchemaCacheNum).get());
    assertTrue(localAvroSchemaCache.getSchema(avroTripSchemaCacheNum).isPresent());
    assertEquals(HoodieTestDataGenerator.AVRO_TRIP_SCHEMA, localAvroSchemaCache.getSchema(avroTripSchemaCacheNum).get());
    assertTrue(localAvroSchemaCache.getSchema(flatAvroSchemaCacheNum).isPresent());
    assertEquals(HoodieTestDataGenerator.FLATTENED_AVRO_SCHEMA, localAvroSchemaCache.getSchema(flatAvroSchemaCacheNum).get());
    assertTrue(localAvroSchemaCache.getSchema(nestAvroSchemaCacheNum).isPresent());
    assertEquals(HoodieTestDataGenerator.NESTED_AVRO_SCHEMA, localAvroSchemaCache.getSchema(nestAvroSchemaCacheNum).get());
  }

  @Test
  public void testCopiesOfSameSchema() {
    LocalAvroSchemaCache localAvroSchemaCache = LocalAvroSchemaCache.getInstance();
    Schema testSchema1 = new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);
    Schema testSchema2 = new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);
    Integer cachenum = localAvroSchemaCache.cacheSchema(testSchema1);
    Integer secondSchemaCacheNum = localAvroSchemaCache.cacheSchema(testSchema2);
    assertEquals(cachenum, secondSchemaCacheNum);
    assertTrue(localAvroSchemaCache.getSchema(cachenum).isPresent());
    assertEquals(testSchema1, localAvroSchemaCache.getSchema(cachenum).get());
  }
}
