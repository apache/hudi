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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
      String key = prefix + UUID.randomUUID().toString();
      String family = colFamilies.get(index % 2);
      String val = "VALUE_" + UUID.randomUUID().toString();
      payloads.add(new Payload(prefix, key, val, family));
    });

    colFamilies.forEach(family -> dbManager.dropColumnFamily(family));
    colFamilies.forEach(family -> dbManager.addColumnFamily(family));
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
      dbManager.prefixDelete(family, prefix1);

      int got = dbManager.prefixSearch(family, prefix1).collect(Collectors.toList()).size();
      assertEquals(countsMap.get(family).get(prefix1) == null ? 0 : 1, got,
          "Expected prefix delete to leave at least one item for family: " + family);
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
  public static class Payload<T> implements Serializable {

    private final String prefix;
    private final T key;
    private final String val;
    private final String family;

    public Payload(String prefix, T key, String val, String family) {
      this.prefix = prefix;
      this.key = key;
      this.val = val;
      this.family = family;
    }

    public String getPrefix() {
      return prefix;
    }

    public T getKey() {
      return key;
    }

    public String getVal() {
      return val;
    }

    public String getFamily() {
      return family;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Payload payload = (Payload) o;
      return Objects.equals(prefix, payload.prefix) && Objects.equals(key, payload.key)
          && Objects.equals(val, payload.val) && Objects.equals(family, payload.family);
    }

    @Override
    public int hashCode() {
      return Objects.hash(prefix, key, val, family);
    }
  }
}
