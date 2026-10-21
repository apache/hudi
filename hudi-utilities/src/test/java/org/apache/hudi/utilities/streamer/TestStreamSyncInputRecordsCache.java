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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the per-write tagging and tag-based release of the cached input records RDD in StreamSync: the tag is
 * unique per (base path, instant), the tagged RDD is found and released by tag without retaining a reference, and
 * releasing one write's tag leaves another write's cache intact.
 */
public class TestStreamSyncInputRecordsCache extends UtilitiesTestBase {

  @BeforeAll
  public static void setupAll() throws Exception {
    initTestServices();
  }

  @AfterAll
  public static void tearDownAll() {
    cleanUpUtilitiesTestServices();
  }

  private static boolean isPersistedByName(String name) {
    return jsc.getPersistentRDDs().values().stream().anyMatch(rdd -> name.equals(rdd.name()));
  }

  @Test
  public void testCacheNameIsUniquePerBasePathAndInstant() {
    String name = StreamSync.inputRecordsCacheName("/base/path/tbl", "20240102030405006");
    assertEquals("hoodie-input-records-/base/path/tbl-20240102030405006", name);
    // a different instant or base path yields a different tag
    assertNotEquals(name, StreamSync.inputRecordsCacheName("/base/path/tbl", "20240102030405007"));
    assertNotEquals(name, StreamSync.inputRecordsCacheName("/base/path/other", "20240102030405006"));
  }

  @Test
  public void testUnpersistCachedInputRecordsFindsAndReleasesByTag() {
    String nameA = StreamSync.inputRecordsCacheName("/base/path/tbl", "20240102030405001");
    String nameB = StreamSync.inputRecordsCacheName("/base/path/tbl", "20240102030405002");

    JavaRDD<Integer> rddA = jsc.parallelize(Arrays.asList(1, 2, 3), 1);
    rddA.rdd().setName(nameA);
    rddA.persist(StorageLevel.MEMORY_AND_DISK_SER());
    JavaRDD<Integer> rddB = jsc.parallelize(Arrays.asList(4, 5, 6), 1);
    rddB.rdd().setName(nameB);
    rddB.persist(StorageLevel.MEMORY_AND_DISK_SER());
    try {
      // both writes' caches are registered and findable by their tags
      assertTrue(isPersistedByName(nameA));
      assertTrue(isPersistedByName(nameB));

      // releasing write A's tag releases only A (no reference retained), leaving write B intact - proving the
      // per-write tag prevents one write from evicting another's cache in the shared SparkContext
      StreamSync.unpersistCachedInputRecords(jsc, nameA);
      assertFalse(isPersistedByName(nameA));
      assertTrue(isPersistedByName(nameB));

      // an unknown tag is a no-op
      StreamSync.unpersistCachedInputRecords(jsc, StreamSync.inputRecordsCacheName("/base/path/tbl", "no-such-instant"));
      assertTrue(isPersistedByName(nameB));

      StreamSync.unpersistCachedInputRecords(jsc, nameB);
      assertFalse(isPersistedByName(nameB));
    } finally {
      rddA.unpersist(false);
      rddB.unpersist(false);
    }
  }
}
