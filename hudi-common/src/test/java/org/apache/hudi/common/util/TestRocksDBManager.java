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

import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.util.collection.Pair;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Tests RocksDB manager {@link RocksDBDAO}.
 */
public class TestRocksDBManager {

  private static RocksDBDAO dbManager;

  @BeforeClass
  public static void setUpClass() {
    dbManager = new RocksDBDAO("/dummy/path",
        FileSystemViewStorageConfig.newBuilder().build().newBuilder().build().getRocksdbBasePath());
  }

  @AfterClass
  public static void tearDownClass() {
    if (dbManager != null) {
      dbManager.close();
      dbManager = null;
    }
  }

  @Test
  public void testRocksDBManager() throws Exception {
    String prefix1 = "prefix1_";
    String prefix2 = "prefix2_";
    String prefix3 = "prefix3_";
    String prefix4 = "prefix4_";
    List<String> prefixes = Arrays.asList(prefix1, prefix2, prefix3, prefix4);
    String family1 = "family1";
    String family2 = "family2";
    List<String> colFamilies = Arrays.asList(family1, family2);

    List<Payload> payloads = IntStream.range(0, 100).mapToObj(index -> {
      String prefix = prefixes.get(index % 4);
      String key = prefix + UUID.randomUUID().toString();
      String family = colFamilies.get(index % 2);
      String val = "VALUE_" + UUID.randomUUID().toString();
      return new Payload(prefix, key, val, family);
    }).collect(Collectors.toList());

    colFamilies.stream().forEach(family -> dbManager.dropColumnFamily(family));
    colFamilies.stream().forEach(family -> dbManager.addColumnFamily(family));

    Map<String, Map<String, Integer>> countsMap = new HashMap<>();
    payloads.stream().forEach(payload -> {
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

    colFamilies.stream().forEach(family -> {
      prefixes.stream().forEach(prefix -> {
        List<Pair<String, Payload>> gotPayloads =
            dbManager.<Payload>prefixSearch(family, prefix).collect(Collectors.toList());
        Integer expCount = countsMap.get(family).get(prefix);
        Assert.assertEquals("Size check for prefix (" + prefix + ") and family (" + family + ")",
            expCount == null ? 0L : expCount.longValue(), gotPayloads.size());
        gotPayloads.stream().forEach(p -> {
          Assert.assertEquals(p.getRight().getFamily(), family);
          Assert.assertTrue(p.getRight().getKey().startsWith(prefix));
        });
      });
    });

    payloads.stream().forEach(payload -> {
      Payload p = dbManager.get(payload.getFamily(), payload.getKey());
      Assert.assertEquals("Retrieved correct payload for key :" + payload.getKey(), payload, p);

      // Now, delete the key
      dbManager.delete(payload.getFamily(), payload.getKey());

      // Now retrieve
      Payload p2 = dbManager.get(payload.getFamily(), payload.getKey());
      Assert.assertNull("Retrieved correct payload for key :" + p.getKey(), p2);
    });

    // Now do a prefix search
    colFamilies.stream().forEach(family -> {
      prefixes.stream().forEach(prefix -> {
        List<Pair<String, Payload>> gotPayloads =
            dbManager.<Payload>prefixSearch(family, prefix).collect(Collectors.toList());
        Assert.assertEquals("Size check for prefix (" + prefix + ") and family (" + family + ")", 0,
            gotPayloads.size());
      });
    });

    String rocksDBBasePath = dbManager.getRocksDBBasePath();
    dbManager.close();
    Assert.assertFalse(new File(rocksDBBasePath).exists());
  }

  /**
   * A payload definition for {@link TestRocksDBManager}.
   */
  public static class Payload implements Serializable {

    private final String prefix;
    private final String key;
    private final String val;
    private final String family;

    public Payload(String prefix, String key, String val, String family) {
      this.prefix = prefix;
      this.key = key;
      this.val = val;
      this.family = family;
    }

    public String getPrefix() {
      return prefix;
    }

    public String getKey() {
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
