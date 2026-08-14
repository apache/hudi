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

package org.apache.hudi.common.util.collection;

import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;

import lombok.Value;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests RocksDB manager {@link RocksDBDAO}.
 */
public class TestRocksDBDAO {

  private RocksDBDAO dbManager;

  @BeforeEach
  public void setUpClass() {
    dbManager = new RocksDBDAO("/dummy/path/" + UUID.randomUUID().toString(),
        FileSystemViewStorageConfig.newBuilder().build().newBuilder().build().getRocksdbBasePath());
  }

  @AfterEach
  public void tearDownClass() {
    if (dbManager != null) {
      dbManager.close();
      dbManager = null;
    }
  }

  @Test
  public void testRocksDBManager() {
    String prefix1 = "prefix1_";
    String prefix2 = "prefix2_";
    String prefix3 = "prefix3_";
    String prefix4 = "prefix4_";
    List<String> prefixes = Arrays.asList(prefix1, prefix2, prefix3, prefix4);
    String family1 = "family1";
    String family2 = "family2";
    List<String> colFamilies = Arrays.asList(family1, family2);

    final List<Payload<String>> payloads = new ArrayList<>();
    IntStream.range(0, 100).forEach(index -> {
      String prefix = prefixes.get(index % 4);
      String key = prefix + UUID.randomUUID();
      String family = colFamilies.get(index % 2);
      String val = "VALUE_" + UUID.randomUUID();
      payloads.add(new Payload(prefix, key, val, family));
    });

    colFamilies.forEach(family -> dbManager.dropColumnFamily(family));
    colFamilies.forEach(family -> dbManager.addColumnFamily(family));

    Map<String, Map<String, Integer>> countsMap = new HashMap<>();
    payloads.forEach(payload -> {
      dbManager.put(payload.getFamily(), payload.getKey(), payload);

      if (!countsMap.containsKey(payload.family)) {
        countsMap.put(payload.family, new HashMap<>());
      }
      Map<String, Integer> c = countsMap.get(payload.family);
      if (!c.containsKey(payload.prefix)) {
        c.put(payload.prefix, 0);
      }
      int currCount = c.get(payload.prefix);
      c.put(payload.prefix, currCount + 1);
    });

    colFamilies.forEach(family -> {
      prefixes.forEach(prefix -> {
        List<Pair<String, Payload>> gotPayloads =
            dbManager.<Payload>prefixSearch(family, prefix).collect(Collectors.toList());
        Integer expCount = countsMap.get(family).get(prefix);
        assertEquals(expCount == null ? 0L : expCount.longValue(), gotPayloads.size(),
            "Size check for prefix (" + prefix + ") and family (" + family + ")");
        gotPayloads.forEach(p -> {
          assertEquals(p.getRight().getFamily(), family);
          assertTrue(p.getRight().getKey().toString().startsWith(prefix));
        });
      });
    });

    payloads.stream().filter(p -> !p.getPrefix().equalsIgnoreCase(prefix1)).forEach(payload -> {
      Payload p = dbManager.get(payload.getFamily(), payload.getKey());
      assertEquals(payload, p, "Retrieved correct payload for key :" + payload.getKey());

      dbManager.delete(payload.getFamily(), payload.getKey());

      Payload p2 = dbManager.get(payload.getFamily(), payload.getKey());
      assertNull(p2, "Retrieved correct payload for key :" + payload.getKey());
    });

    colFamilies.forEach(family -> {
      long countBeforeDeletion = dbManager.prefixSearch(family, prefix1).count();
      dbManager.prefixDelete(family, prefix1);
      if (countBeforeDeletion > 0) {
        long countAfterDeletion = dbManager.prefixSearch(family, prefix1).count();
        assertEquals(0, countAfterDeletion,
                "Expected prefixDelete to remove all items for family: " + family);
      }
    });

    payloads.stream().filter(p -> !p.getPrefix().equalsIgnoreCase(prefix1)).forEach(payload -> {
      Payload p2 = dbManager.get(payload.getFamily(), payload.getKey());
      assertNull(p2, "Retrieved correct payload for key :" + payload.getKey());
    });

    // Now do a prefix search
    colFamilies.forEach(family -> {
      prefixes.stream().filter(p -> !p.equalsIgnoreCase(prefix1)).forEach(prefix -> {
        List<Pair<String, Payload>> gotPayloads =
            dbManager.<Payload>prefixSearch(family, prefix).collect(Collectors.toList());
        assertEquals(0, gotPayloads.size(),
            "Size check for prefix (" + prefix + ") and family (" + family + ")");
      });
    });

    String rocksDBBasePath = dbManager.getRocksDBBasePath();
    dbManager.close();
    assertFalse(new File(rocksDBBasePath).exists());
  }

  @Test
  public void testWithSerializableKey() {
    String prefix1 = "prefix1_";
    String prefix2 = "prefix2_";
    String prefix3 = "prefix3_";
    String prefix4 = "prefix4_";
    List<String> prefixes = Arrays.asList(prefix1, prefix2, prefix3, prefix4);
    String family1 = "family1";
    String family2 = "family2";
    List<String> colFamilies = Arrays.asList(family1, family2);

    final List<Payload<PayloadKey>> payloads = new ArrayList<>();
    IntStream.range(0, 100).forEach(index -> {
      String prefix = prefixes.get(index % 4);
      String key = prefix + UUID.randomUUID().toString();
      String family = colFamilies.get(index % 2);
      String val = "VALUE_" + UUID.randomUUID().toString();
      payloads.add(new Payload(prefix, new PayloadKey((key)), val, family));
    });

    colFamilies.forEach(family -> dbManager.dropColumnFamily(family));
    colFamilies.forEach(family -> dbManager.addColumnFamily(family));

    Map<String, Map<String, Integer>> countsMap = new HashMap<>();
    dbManager.writeBatch(batch -> {
      payloads.forEach(payload -> {
        dbManager.putInBatch(batch, payload.getFamily(), payload.getKey(), payload);

        if (!countsMap.containsKey(payload.family)) {
          countsMap.put(payload.family, new HashMap<>());
        }
        Map<String, Integer> c = countsMap.get(payload.family);
        if (!c.containsKey(payload.prefix)) {
          c.put(payload.prefix, 0);
        }
        int currCount = c.get(payload.prefix);
        c.put(payload.prefix, currCount + 1);
      });
    });

    Iterator<List<Payload<PayloadKey>>> payloadSplits = payloads.stream()
        .collect(Collectors.partitioningBy(s -> payloads.indexOf(s) > payloads.size() / 2)).values()
        .iterator();

    payloads.forEach(payload -> {
      Payload p = dbManager.get(payload.getFamily(), payload.getKey());
      assertEquals(payload, p, "Retrieved correct payload for key :" + payload.getKey());
    });

    payloadSplits.next().forEach(payload -> {
      dbManager.delete(payload.getFamily(), payload.getKey());
      Payload want = dbManager.get(payload.getFamily(), payload.getKey());
      assertNull(want, "Verify deleted during single delete for key :" + payload.getKey());
    });

    dbManager.writeBatch(batch -> {
      payloadSplits.next().forEach(payload -> {
        dbManager.deleteInBatch(batch, payload.getFamily(), payload.getKey());
        Payload want = dbManager.get(payload.getFamily(), payload.getKey());
        assertEquals(payload, want, "Verify not deleted during batch delete in progress for key :" + payload.getKey());
      });
    });

    payloads.forEach(payload -> {
      Payload want = dbManager.get(payload.getFamily(), payload.getKey());
      assertNull(want, "Verify delete for key :" + payload.getKey());
    });

    // Now do a prefix search
    colFamilies.forEach(family -> {
      prefixes.forEach(prefix -> {
        List<Pair<String, Payload>> gotPayloads =
            dbManager.<Payload>prefixSearch(family, prefix).collect(Collectors.toList());
        assertEquals(0, gotPayloads.size(),
            "Size check for prefix (" + prefix + ") and family (" + family + ")");
      });
    });

    String rocksDBBasePath = dbManager.getRocksDBBasePath();
    dbManager.close();
    assertFalse(new File(rocksDBBasePath).exists());
  }

  /**
   * Test that concurrent access to RocksDBDAO does not cause ConcurrentModificationException.
   * This test verifies the thread-safety of the columnFamilySerializers map which is accessed
   * via getSerializerForColumnFamily() during get/put operations.
   */
  @Test
  public void testConcurrentAccess() throws InterruptedException {
    int numThreads = 10;
    int numOperationsPerThread = 100;
    int numColumnFamilies = 5;

    List<String> columnFamilies = new ArrayList<>();
    for (int i = 0; i < numColumnFamilies; i++) {
      String family = "concurrent_family_" + i;
      columnFamilies.add(family);
      dbManager.addColumnFamily(family);
    }

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    try {
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch doneLatch = new CountDownLatch(numThreads);
      AtomicReference<Throwable> error = new AtomicReference<>(null);

      // Spawn threads that concurrently access different column families
      for (int t = 0; t < numThreads; t++) {
        final int threadId = t;
        executor.submit(() -> {
          try {
            // Wait for all threads to be ready
            startLatch.await();

            for (int i = 0; i < numOperationsPerThread; i++) {
              // Each thread accesses different column families to trigger
              // concurrent calls to getSerializerForColumnFamily()
              String family = columnFamilies.get((threadId + i) % numColumnFamilies);
              String key = "key_" + threadId + "_" + i;
              String value = "value_" + threadId + "_" + i;

              dbManager.put(family, key, value);
              String retrieved = dbManager.get(family, key);
              assertEquals(value, retrieved, "Value mismatch for key: " + key);
            }
          } catch (Throwable t1) {
            error.compareAndSet(null, t1);
          } finally {
            doneLatch.countDown();
          }
        });
      }

      startLatch.countDown();

      // Wait for all threads to complete
      boolean completed = doneLatch.await(60, TimeUnit.SECONDS);

      assertTrue(completed, "Test timed out - threads did not complete in time");
      assertNull(error.get(), "Concurrent access caused an exception: "
          + (error.get() != null ? error.get().getMessage() : ""));
    } finally {
      executor.shutdownNow();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDisableWalAtDaoLevel(boolean disableWAL) {
    RocksDBDAO dbManager = new RocksDBDAO("/dummy/path/" + UUID.randomUUID(),
        FileSystemViewStorageConfig.newBuilder().build().newBuilder().build().getRocksdbBasePath(),
        new ConcurrentHashMap<>(),
        disableWAL);

    String family = "family_disable_wal";
    dbManager.dropColumnFamily(family);
    dbManager.addColumnFamily(family);

    String key1 = "key1";
    String key2 = "key2";
    String key3 = "key3";
    String value1 = "value1";
    String value2 = "value2";
    String value3 = "value3";

    dbManager.put(family, key1, value1);
    dbManager.writeBatch(batch -> {
      dbManager.putInBatch(batch, family, key2, value2);
      dbManager.putInBatch(batch, family, key3, value3);
    });
    dbManager.delete(family, key2);

    assertEquals(value1, dbManager.get(family, key1));
    assertNull(dbManager.get(family, key2));
    assertEquals(value3, dbManager.get(family, key3));

    File rocksDbDir = new File(dbManager.getRocksDBBasePath());
    File[] walFiles = rocksDbDir.listFiles((dir, name) -> name.matches("\\d+\\.log"));
    long walFileSize = walFiles == null ? 0 : Arrays.stream(walFiles).mapToLong(File::length).sum();
    assertEquals(disableWAL, walFileSize == 0, "WAL log total size should be 0 when disableWAL=true");
  }

  /**
   * Payload key object.
   */
  public static class PayloadKey implements Serializable {
    private String key;

    public PayloadKey(String key) {
      this.key = key;
    }

    @Override
    public String toString() {
      return key;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PayloadKey that = (PayloadKey) o;
      return Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key);
    }
  }

  /**
   * A payload definition for {@link TestRocksDBDAO}.
   */
  @Value
  public static class Payload<T> implements Serializable {

    String prefix;
    T key;
    String val;
    String family;
  }
}
