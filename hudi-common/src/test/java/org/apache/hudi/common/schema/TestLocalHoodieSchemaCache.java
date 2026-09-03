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

package org.apache.hudi.common.schema;

import org.apache.hudi.common.testutils.HoodieTestDataGenerator;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestLocalHoodieSchemaCache {

  @Test
  public void testBasicCacheUsage() {
    LocalHoodieSchemaCache schemaCache = LocalHoodieSchemaCache.create();
    Integer schemaCacheNum = schemaCache.cacheSchema(HoodieTestDataGenerator.HOODIE_SCHEMA);
    Integer nestedSchemaCacheNum = schemaCache.cacheSchema(HoodieTestDataGenerator.NESTED_SCHEMA);
    Integer metaFieldsSchemaCacheNum = schemaCache.cacheSchema(HoodieTestDataGenerator.HOODIE_SCHEMA_WITH_METADATA_FIELDS);
    Integer decimalSchemaCacheNum = schemaCache.cacheSchema(HoodieTestDataGenerator.HOODIE_TRIP_ENCODED_DECIMAL_SCHEMA);
    Set<Integer> uniqueSet = new HashSet<>(
        Arrays.asList(schemaCacheNum, nestedSchemaCacheNum, metaFieldsSchemaCacheNum, decimalSchemaCacheNum));
    assertEquals(4, uniqueSet.size());
    assertTrue(schemaCache.getSchema(schemaCacheNum).isPresent());
    assertEquals(HoodieTestDataGenerator.HOODIE_SCHEMA, schemaCache.getSchema(schemaCacheNum).get());
    assertTrue(schemaCache.getSchema(nestedSchemaCacheNum).isPresent());
    assertEquals(HoodieTestDataGenerator.NESTED_SCHEMA, schemaCache.getSchema(nestedSchemaCacheNum).get());
    assertTrue(schemaCache.getSchema(metaFieldsSchemaCacheNum).isPresent());
    assertEquals(HoodieTestDataGenerator.HOODIE_SCHEMA_WITH_METADATA_FIELDS, schemaCache.getSchema(metaFieldsSchemaCacheNum).get());
    assertTrue(schemaCache.getSchema(decimalSchemaCacheNum).isPresent());
    assertEquals(HoodieTestDataGenerator.HOODIE_TRIP_ENCODED_DECIMAL_SCHEMA, schemaCache.getSchema(decimalSchemaCacheNum).get());
    assertFalse(schemaCache.getSchema(999).isPresent());
  }

  @Test
  public void testCopiesOfSameSchema() {
    LocalHoodieSchemaCache schemaCache = LocalHoodieSchemaCache.create();
    HoodieSchema testSchema1 = HoodieSchema.parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);
    HoodieSchema testSchema2 = HoodieSchema.parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);
    Integer cacheNum = schemaCache.cacheSchema(testSchema1);
    Integer secondSchemaCacheNum = schemaCache.cacheSchema(testSchema2);
    assertEquals(cacheNum, secondSchemaCacheNum);
    assertTrue(schemaCache.getSchema(cacheNum).isPresent());
    assertEquals(testSchema1, schemaCache.getSchema(cacheNum).get());
  }

  @Test
  public void testCreateReturnsIndependentCaches() {
    LocalHoodieSchemaCache first = LocalHoodieSchemaCache.create();
    LocalHoodieSchemaCache second = LocalHoodieSchemaCache.create();
    assertNotSame(first, second);

    Integer firstId = first.cacheSchema(HoodieTestDataGenerator.HOODIE_SCHEMA);
    // the second cache has not seen the schema: a shared instance would resolve the id
    assertFalse(second.getSchema(firstId).isPresent());

    // each cache starts its own id space, so the same id resolves to a different schema per cache
    Integer secondId = second.cacheSchema(HoodieTestDataGenerator.NESTED_SCHEMA);
    assertEquals(firstId, secondId);
    assertEquals(HoodieTestDataGenerator.HOODIE_SCHEMA, first.getSchema(firstId).get());
    assertEquals(HoodieTestDataGenerator.NESTED_SCHEMA, second.getSchema(secondId).get());
  }
}
