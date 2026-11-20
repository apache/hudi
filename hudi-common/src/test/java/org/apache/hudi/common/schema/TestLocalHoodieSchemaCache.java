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
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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

  private static HoodieSchema recordSchema(String name) {
    return HoodieSchema.parse("{\"type\":\"record\",\"name\":\"" + name + "\",\"fields\":["
        + "{\"name\":\"_hoodie_record_key\",\"type\":\"string\"},"
        + "{\"name\":\"ts\",\"type\":[\"null\",\"long\"],\"default\":null}]}");
  }

  @Test
  public void testIdsAreStableAndResolve() {
    LocalHoodieSchemaCache cache = LocalHoodieSchemaCache.create();
    HoodieSchema a = recordSchema("a");
    HoodieSchema b = recordSchema("b");
    Integer idA = cache.cacheSchema(a);
    Integer idB = cache.cacheSchema(b);
    assertNotEquals(idA, idB);
    assertEquals(idA, cache.cacheSchema(recordSchema("a")), "an equal schema must get the same id");
    assertEquals(a, cache.getSchema(idA).get());
    assertEquals(b, cache.getSchema(idB).get());
    // delete records carry no schema id
    assertFalse(cache.getSchema(null).isPresent());
    assertFalse(cache.getSchema(Integer.MAX_VALUE).isPresent());
    // a null schema gets one stable id of its own that resolves to no schema
    Integer idNull = cache.cacheSchema(null);
    assertEquals(idNull, cache.cacheSchema(null));
    assertNotEquals(idA, idNull);
    assertNotEquals(idB, idNull);
    assertFalse(cache.getSchema(idNull).isPresent());
  }

  /**
   * The cache travels inside a serialized {@code RecordContext}: a copy keeps every id and issues fresh ids after them.
   */
  @Test
  public void testJavaSerializationRoundTrip() throws Exception {
    LocalHoodieSchemaCache cache = LocalHoodieSchemaCache.create();
    HoodieSchema a = recordSchema("a");
    Integer idA = cache.cacheSchema(a);
    Integer idNull = cache.cacheSchema(null);

    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(cache);
    }
    LocalHoodieSchemaCache copy;
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      copy = (LocalHoodieSchemaCache) in.readObject();
    }

    assertEquals(a, copy.getSchema(idA).get());
    assertEquals(idA, copy.cacheSchema(recordSchema("a")));
    assertEquals(idNull, copy.cacheSchema(null));
    Integer idB = copy.cacheSchema(recordSchema("b"));
    assertNotEquals(idA, idB);
    assertNotEquals(idNull, idB);
    assertEquals(recordSchema("b"), copy.getSchema(idB).get());
  }

  /**
   * Several task threads register the schemas of new queries while another task keeps resolving the id of a
   * schema it registered earlier, as a merge does once per record. Every id must resolve to its schema, both
   * while the threads run and afterwards.
   */
  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  public void testConcurrentRegistrationAndLookup() throws Exception {
    final int writers = 4;
    final int schemasPerWriter = 20_000;
    final LocalHoodieSchemaCache cache = LocalHoodieSchemaCache.create();
    final HoodieSchema merging = recordSchema("merging_task");
    final Integer mergingId = cache.cacheSchema(merging);

    // build the schemas up front so the threads spend their time inside the cache
    final List<List<HoodieSchema>> perWriter = new ArrayList<>();
    for (int w = 0; w < writers; w++) {
      List<HoodieSchema> schemas = new ArrayList<>(schemasPerWriter);
      for (int i = 0; i < schemasPerWriter; i++) {
        // every writer also registers the shared "q<i>" schemas, so equal schemas race for one id
        schemas.add(i % 2 == 0 ? recordSchema("w" + w + "_" + i) : recordSchema("q" + i));
      }
      perWriter.add(schemas);
    }

    final ExecutorService pool = Executors.newFixedThreadPool(writers + 1);
    final CountDownLatch start = new CountDownLatch(1);
    final AtomicBoolean writing = new AtomicBoolean(true);
    final AtomicLong misses = new AtomicLong();
    try {
      Future<?> reader = pool.submit(() -> {
        start.await();
        while (writing.get() && !Thread.currentThread().isInterrupted()) {
          Option<HoodieSchema> back = cache.getSchema(mergingId);
          if (!back.isPresent() || !merging.equals(back.get())) {
            misses.incrementAndGet();
          }
        }
        return null;
      });
      List<Future<List<Integer>>> futures = new ArrayList<>();
      for (int w = 0; w < writers; w++) {
        final List<HoodieSchema> schemas = perWriter.get(w);
        futures.add(pool.submit(() -> {
          start.await();
          List<Integer> ids = new ArrayList<>(schemas.size());
          for (HoodieSchema s : schemas) {
            Integer id = cache.cacheSchema(s);
            Option<HoodieSchema> back = cache.getSchema(id);
            if (!back.isPresent() || !s.equals(back.get())) {
              misses.incrementAndGet();
            }
            ids.add(id);
          }
          return ids;
        }));
      }
      start.countDown();
      List<List<Integer>> idsPerWriter = new ArrayList<>();
      for (Future<List<Integer>> f : futures) {
        idsPerWriter.add(f.get());
      }
      writing.set(false);
      reader.get();

      assertEquals(0, misses.get(), "a schema id resolved to no schema, or to another schema, while tasks ran");

      // afterwards: every issued id still resolves, equal schemas share one id, distinct schemas never share one
      Set<Integer> distinctIds = new HashSet<>();
      for (int w = 0; w < writers; w++) {
        for (int i = 0; i < schemasPerWriter; i++) {
          HoodieSchema s = perWriter.get(w).get(i);
          Integer id = idsPerWriter.get(w).get(i);
          assertTrue(cache.getSchema(id).isPresent(), "id " + id + " lost its schema");
          assertEquals(s, cache.getSchema(id).get());
          assertEquals(id, cache.cacheSchema(s), "schema re-registered under a new id");
          distinctIds.add(id);
        }
      }
      // writer-private schemas (writers * schemasPerWriter / 2) plus the shared q-schemas (schemasPerWriter / 2)
      assertEquals(writers * schemasPerWriter / 2 + schemasPerWriter / 2, distinctIds.size());
      assertEquals(merging, cache.getSchema(mergingId).get());
    } finally {
      // stop the reader even when an assertion or the timeout ends the test early
      writing.set(false);
      pool.shutdownNow();
    }
  }
}
