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

/*
 * Micro-benchmark for the Inflater/Deflater reuse change in BitCaskDiskMap — RLI payload variant.
 *
 * Same OLD-vs-NEW comparison as InflaterDeflaterReuseBenchmark, but the payload
 * fed to compress/decompress is a kryo-serialized HoodieAvroRecord<HoodieMetadataPayload>
 * carrying a record-index entry. This mirrors what BitCaskDiskMap.compressBytes actually
 * sees in production when spilling metadata-table record-index records.
 *
 * Each "op" compresses + decompresses ONE serialized record by default. To stress the
 * codec with larger buffers, pass recordsPerOp > 1 — the benchmark packs that many
 * serialized records into a single byte[] per op.
 *
 * Run via Maven (needs Hudi classes on the test classpath):
 *
 *   JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_192.jdk/Contents/Home \
 *   mvn -pl hudi-common -DskipTests test-compile
 *   JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_192.jdk/Contents/Home \
 *   mvn -pl hudi-common exec:java -Dexec.classpathScope=test \
 *       -Dexec.mainClass=org.apache.hudi.common.util.collection.InflaterDeflaterReuseRLIBenchmark \
 *       -Dexec.args="8 100000 1"
 *
 * Args: [threads] [opsPerThread] [recordsPerOp]
 */

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.metadata.HoodieMetadataPayload;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

public class InflaterDeflaterReuseRLIBenchmark {

  private static final int DEFAULT_THREADS = 8;
  private static final int DEFAULT_OPS_PER_THREAD = 100_000;
  private static final int DEFAULT_RECORDS_PER_OP = 1;
  private static final int DECOMPRESS_INTERMEDIATE_BUFFER_SIZE = 8192;
  private static final int COMPRESS_INITIAL_BUFFER_SIZE = 1024 * 1024;

  public static void main(String[] args) throws Exception {
    int threads = args.length > 0 ? Integer.parseInt(args[0]) : DEFAULT_THREADS;
    int opsPerThread = args.length > 1 ? Integer.parseInt(args[1]) : DEFAULT_OPS_PER_THREAD;
    int recordsPerOp = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_RECORDS_PER_OP;

    // Build a pool of pre-serialized RLI records so the timed loop only measures
    // compress/decompress, not record construction + kryo serialization.
    byte[][] serializedRecords = buildSerializedRLIRecords(2048);
    int singleRecordSize = serializedRecords[0].length;

    System.out.println("=== Inflater/Deflater reuse benchmark — RLI payload ===");
    System.out.println("threads=" + threads
        + " opsPerThread=" + opsPerThread
        + " recordsPerOp=" + recordsPerOp
        + " singleRecordSerializedBytes=" + singleRecordSize
        + " payloadBytesPerOp=" + (singleRecordSize * recordsPerOp)
        + " totalOps=" + ((long) threads * opsPerThread));
    System.out.println("java.version=" + System.getProperty("java.version")
        + " vm=" + System.getProperty("java.vm.name"));
    System.out.println();

    // Warm both paths.
    runScenario("warmup-old", 2, 2_000, serializedRecords, recordsPerOp, false);
    runScenario("warmup-new", 2, 2_000, serializedRecords, recordsPerOp, true);
    System.gc();
    Thread.sleep(200);

    for (int trial = 1; trial <= 3; trial++) {
      System.out.println("--- Trial " + trial + " ---");
      if (trial % 2 == 1) {
        runScenario("OLD (new Deflater/Inflater per call)", threads, opsPerThread, serializedRecords, recordsPerOp, false);
        runScenario("NEW (reuse via ThreadLocal)",          threads, opsPerThread, serializedRecords, recordsPerOp, true);
      } else {
        runScenario("NEW (reuse via ThreadLocal)",          threads, opsPerThread, serializedRecords, recordsPerOp, true);
        runScenario("OLD (new Deflater/Inflater per call)", threads, opsPerThread, serializedRecords, recordsPerOp, false);
      }
      System.out.println();
    }
  }

  private static byte[][] buildSerializedRLIRecords(int count) throws IOException {
    byte[][] out = new byte[count][];
    Random rnd = new Random(42);
    String instantTime = "20260520120000"; // any valid yyyyMMddHHmmss; not what we're measuring
    for (int i = 0; i < count; i++) {
      String recordKey = "user_" + rnd.nextLong() + "_" + i;
      String partition = "date=2026-05-" + String.format("%02d", 1 + (i % 28));
      String fileId = UUID.randomUUID().toString() + "-0";
      HoodieRecord<HoodieMetadataPayload> rec =
          HoodieMetadataPayload.createRecordIndexUpdate(recordKey, partition, fileId, instantTime, 0);
      out[i] = SerializationUtils.serialize(rec);
    }
    return out;
  }

  /**
   * Build the per-op payload by concatenating `recordsPerOp` serialized records.
   * BitCaskDiskMap actually compresses one record at a time; this stitching mode is
   * here only to simulate larger-buffer scenarios on demand.
   */
  private static byte[] buildPayload(byte[][] pool, int recordsPerOp, int seed) {
    if (recordsPerOp == 1) {
      return pool[Math.floorMod(seed, pool.length)];
    }
    int total = 0;
    int start = Math.floorMod(seed, pool.length);
    for (int i = 0; i < recordsPerOp; i++) {
      total += pool[(start + i) % pool.length].length;
    }
    byte[] out = new byte[total];
    int pos = 0;
    for (int i = 0; i < recordsPerOp; i++) {
      byte[] r = pool[(start + i) % pool.length];
      System.arraycopy(r, 0, out, pos, r.length);
      pos += r.length;
    }
    return out;
  }

  private static void runScenario(String label,
                                  int threads,
                                  int opsPerThread,
                                  byte[][] pool,
                                  int recordsPerOp,
                                  boolean reuse) throws Exception {
    ExecutorService poolExec = Executors.newFixedThreadPool(threads);
    CountDownLatch start = new CountDownLatch(1);
    CountDownLatch done = new CountDownLatch(threads);

    long[][] perThreadLatenciesNs = new long[threads][];
    AtomicLong failures = new AtomicLong();

    for (int t = 0; t < threads; t++) {
      final int idx = t;
      poolExec.submit(new Runnable() {
        @Override
        public void run() {
          long[] latencies = new long[opsPerThread];
          ThreadCompressionContext ctx = new ThreadCompressionContext();
          try {
            start.await();
            for (int i = 0; i < opsPerThread; i++) {
              byte[] payload = buildPayload(pool, recordsPerOp, idx * 1_000_003 + i);
              long t0 = System.nanoTime();
              byte[] compressed = reuse ? ctx.compressReuse(payload) : ctx.compressNew(payload);
              byte[] decompressed = reuse ? ctx.decompressReuse(compressed) : ctx.decompressNew(compressed);
              long t1 = System.nanoTime();
              latencies[i] = t1 - t0;
              if (decompressed.length != payload.length) {
                failures.incrementAndGet();
              }
            }
            perThreadLatenciesNs[idx] = latencies;
          } catch (Throwable th) {
            failures.incrementAndGet();
            th.printStackTrace();
          } finally {
            ctx.close();
            done.countDown();
          }
        }
      });
    }

    long wallStart = System.nanoTime();
    start.countDown();
    done.await();
    long wallEnd = System.nanoTime();
    poolExec.shutdown();

    long totalOps = (long) threads * opsPerThread;
    long wallMs = (wallEnd - wallStart) / 1_000_000;
    double opsPerSec = totalOps / ((wallEnd - wallStart) / 1e9);

    long[] all = flatten(perThreadLatenciesNs);
    Arrays.sort(all);

    System.out.printf("%-42s wall=%6d ms  throughput=%9.0f ops/s"
            + "  avg=%6.1f us  p50=%6.1f us  p95=%6.1f us  p99=%6.1f us  max=%7.1f us  failures=%d%n",
        label,
        wallMs,
        opsPerSec,
        mean(all) / 1000.0,
        percentile(all, 50) / 1000.0,
        percentile(all, 95) / 1000.0,
        percentile(all, 99) / 1000.0,
        all[all.length - 1] / 1000.0,
        failures.get());
  }

  /** Mirrors BitCaskDiskMap.CompressionHandler with both pre-fix and post-fix paths. */
  private static final class ThreadCompressionContext {
    private final ByteArrayOutputStream compressBaos =
        new ByteArrayOutputStream(COMPRESS_INITIAL_BUFFER_SIZE);
    private final ByteArrayOutputStream decompressBaos =
        new ByteArrayOutputStream(COMPRESS_INITIAL_BUFFER_SIZE);
    private final byte[] intermediate = new byte[DECOMPRESS_INTERMEDIATE_BUFFER_SIZE];

    private Deflater reusedDeflater;
    private Inflater reusedInflater;

    byte[] compressNew(byte[] value) throws IOException {
      compressBaos.reset();
      Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION);
      DeflaterOutputStream dos = new DeflaterOutputStream(compressBaos, deflater);
      try {
        dos.write(value);
      } finally {
        dos.close();
        deflater.end();
      }
      return compressBaos.toByteArray();
    }

    byte[] decompressNew(byte[] bytes) throws IOException {
      decompressBaos.reset();
      try (InputStream in = new InflaterInputStream(new ByteArrayInputStream(bytes))) {
        int len;
        while ((len = in.read(intermediate)) > 0) {
          decompressBaos.write(intermediate, 0, len);
        }
      }
      return decompressBaos.toByteArray();
    }

    byte[] compressReuse(byte[] value) throws IOException {
      compressBaos.reset();
      if (reusedDeflater == null) {
        reusedDeflater = new Deflater(Deflater.BEST_COMPRESSION);
      }
      reusedDeflater.reset();
      try (DeflaterOutputStream dos = new DeflaterOutputStream(compressBaos, reusedDeflater)) {
        dos.write(value);
      }
      return compressBaos.toByteArray();
    }

    byte[] decompressReuse(byte[] bytes) throws IOException {
      decompressBaos.reset();
      if (reusedInflater == null) {
        reusedInflater = new Inflater();
      }
      reusedInflater.reset();
      try (InputStream in = new InflaterInputStream(new ByteArrayInputStream(bytes), reusedInflater)) {
        int len;
        while ((len = in.read(intermediate)) > 0) {
          decompressBaos.write(intermediate, 0, len);
        }
      }
      return decompressBaos.toByteArray();
    }

    void close() {
      if (reusedDeflater != null) {
        reusedDeflater.end();
      }
      if (reusedInflater != null) {
        reusedInflater.end();
      }
    }
  }

  private static long[] flatten(long[][] arrs) {
    int total = 0;
    for (long[] a : arrs) {
      total += a.length;
    }
    long[] out = new long[total];
    int pos = 0;
    for (long[] a : arrs) {
      System.arraycopy(a, 0, out, pos, a.length);
      pos += a.length;
    }
    return out;
  }

  private static double mean(long[] sorted) {
    long sum = 0;
    for (long v : sorted) {
      sum += v;
    }
    return (double) sum / sorted.length;
  }

  private static long percentile(long[] sorted, int p) {
    int idx = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
    if (idx < 0) {
      idx = 0;
    }
    if (idx >= sorted.length) {
      idx = sorted.length - 1;
    }
    return sorted[idx];
  }
}
