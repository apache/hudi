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

package org.apache.hudi.common.bloom;

import org.junit.jupiter.api.Test;

import java.util.Locale;
import java.util.Random;
import java.util.function.Supplier;

/**
 * Manual microbenchmark for bloom filter hot paths: key adds (the write-path cost paid per
 * record by the HFile writer), membership tests, and serialization round trips.
 * <p>
 * The class name intentionally does not match the surefire test patterns, so it never runs
 * in CI. Run it explicitly with:
 * <pre>
 * mvn test -pl hudi-common -Dtest=InternalBloomFilterBenchmark -Dsurefire.failIfNoSpecifiedTests=false
 * </pre>
 */
public class InternalBloomFilterBenchmark {

  private static final int WARMUP_ROUNDS = 1;
  private static final int MEASURED_ROUNDS = 3;
  private static final int KEY_POOL_SIZE = 1_000_000;
  private static final int MEMBERSHIP_PROBES = 100_000;

  @Test
  public void benchmarkBloomFilterHotPaths() {
    runScenario("SIMPLE, 1M entries, fpp 1e-3, 1M adds",
        () -> BloomFilterFactory.createBloomFilter(
            1_000_000, 0.001, -1, BloomFilterTypeCode.SIMPLE.name()),
        1_000_000);
    runScenario("SIMPLE, 10M entries, fpp 1e-9, 10M adds",
        () -> BloomFilterFactory.createBloomFilter(
            10_000_000, 0.000000001, -1, BloomFilterTypeCode.SIMPLE.name()),
        10_000_000);
    runScenario("DYNAMIC_V0, 60K entries, fpp 1e-9, max 100K, 10M adds",
        () -> BloomFilterFactory.createBloomFilter(
            60_000, 0.000000001, 100_000, BloomFilterTypeCode.DYNAMIC_V0.name()),
        10_000_000);
  }

  private void runScenario(String name, Supplier<BloomFilter> filterSupplier, int numAdds) {
    String[] keys = generateKeys(KEY_POOL_SIZE, 42);
    String[] absentKeys = generateKeys(MEMBERSHIP_PROBES, 4242);
    System.out.println("== " + name + " ==");
    BloomFilter filter = null;
    for (int round = 0; round < WARMUP_ROUNDS + MEASURED_ROUNDS; round++) {
      filter = filterSupplier.get();
      long start = System.nanoTime();
      for (int i = 0; i < numAdds; i++) {
        filter.add(keys[i % KEY_POOL_SIZE]);
      }
      long addMs = (System.nanoTime() - start) / 1_000_000;

      start = System.nanoTime();
      int hits = 0;
      for (int i = 0; i < MEMBERSHIP_PROBES; i++) {
        if (filter.mightContain(keys[i])) {
          hits++;
        }
      }
      long hitMs = (System.nanoTime() - start) / 1_000_000;

      start = System.nanoTime();
      int falsePositives = 0;
      for (String absentKey : absentKeys) {
        if (filter.mightContain(absentKey)) {
          falsePositives++;
        }
      }
      long missMs = (System.nanoTime() - start) / 1_000_000;

      String label = round < WARMUP_ROUNDS ? "warmup" : "round" + (round - WARMUP_ROUNDS + 1);
      System.out.println(String.format(Locale.ROOT,
          "%s: adds(%d)=%d ms, membership hits(%d)=%d ms, misses(%d)=%d ms (hits=%d, falsePositives=%d)",
          label, numAdds, addMs, MEMBERSHIP_PROBES, hitMs, absentKeys.length, missMs, hits, falsePositives));
    }

    for (int round = 0; round < WARMUP_ROUNDS + MEASURED_ROUNDS; round++) {
      long start = System.nanoTime();
      String serialized = filter.serializeToString();
      long serMs = (System.nanoTime() - start) / 1_000_000;
      start = System.nanoTime();
      BloomFilterFactory.fromString(serialized, filter.getBloomFilterTypeCode().name());
      long deserMs = (System.nanoTime() - start) / 1_000_000;
      String label = round < WARMUP_ROUNDS ? "warmup" : "round" + (round - WARMUP_ROUNDS + 1);
      System.out.println(String.format(Locale.ROOT,
          "%s: serialize=%d ms, deserialize=%d ms (serialized length=%d)",
          label, serMs, deserMs, serialized.length()));
    }
  }

  /** Generates fixed-seed 32-character hex keys, mirroring hash-based record keys. */
  private static String[] generateKeys(int count, long seed) {
    Random random = new Random(seed);
    String[] keys = new String[count];
    byte[] buffer = new byte[16];
    StringBuilder sb = new StringBuilder(32);
    for (int i = 0; i < count; i++) {
      random.nextBytes(buffer);
      sb.setLength(0);
      for (byte b : buffer) {
        sb.append(Character.forDigit((b >> 4) & 0xF, 16)).append(Character.forDigit(b & 0xF, 16));
      }
      keys[i] = sb.toString();
    }
    return keys;
  }
}
