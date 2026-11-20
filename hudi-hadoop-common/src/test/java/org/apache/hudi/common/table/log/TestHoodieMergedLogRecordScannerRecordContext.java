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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.avro.AvroRecordContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.testutils.FileCreateUtilsLegacy;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The merged log record scanner encodes the schemas of the records it merges in a record context. The schema cache of a
 * record context is per thread, so each scanner must merge through its own context, never through the JVM-wide
 * {@link AvroRecordContext#getFieldAccessorInstance()} that every task thread of an executor shares.
 */
public class TestHoodieMergedLogRecordScannerRecordContext extends HoodieCommonTestHarness {

  private static final int NUM_RECORDS = 50;

  private HoodieStorage storage;
  private StoragePath partitionPath;
  private List<String> logFilePaths;
  private HoodieSchema writerSchema;

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    initMetaClient(false, HoodieTableType.MERGE_ON_READ);
    storage = metaClient.getStorage();
    partitionPath = new StoragePath(basePath, "partition_path");
    storage.createDirectory(partitionPath);
    writerSchema = HoodieSchemaUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());
    // two log blocks with the same keys, so every record of the second block is merged with the first
    List<IndexedRecord> records = new ArrayList<>(new SchemaTestUtil().generateHoodieTestRecords(0, NUM_RECORDS));
    records.addAll(new SchemaTestUtil().generateHoodieTestRecords(0, NUM_RECORDS));
    List<HoodieLogFile> logFiles = writeLogFiles(partitionPath, writerSchema, records, 2, storage);
    logFilePaths = logFiles.stream().map(logFile -> logFile.getPath().toString()).distinct().collect(Collectors.toList());
    FileCreateUtilsLegacy.createDeltaCommit(basePath, "100", storage);
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanMetaClient();
  }

  @Test
  public void testMergingScanDoesNotUseTheSharedRecordContext() throws Exception {
    RecordContext<IndexedRecord> shared = AvroRecordContext.getFieldAccessorInstance();
    int before = shared.encodeSchema(probeSchema());

    assertEquals(NUM_RECORDS, scan(uniqueReaderSchema()));

    // the shared context issued exactly one id in between: the second probe's, none for the scanner's reader schema
    assertEquals(before + 1, (int) shared.encodeSchema(probeSchema()),
        "the scanner registered its reader schema in the JVM-wide field-accessor record context");
  }

  @Test
  @Timeout(value = 300, unit = TimeUnit.SECONDS)
  public void testConcurrentMergingScans() throws Exception {
    int threads = 8;
    int scansPerThread = 20;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<Integer>> results = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        results.add(pool.submit(() -> {
          start.await();
          int merged = 0;
          for (int i = 0; i < scansPerThread; i++) {
            // a new reader schema per scan, so the scans keep issuing schema ids while the others merge
            merged += scan(uniqueReaderSchema());
          }
          return merged;
        }));
      }
      start.countDown();
      for (Future<Integer> result : results) {
        assertEquals(NUM_RECORDS * scansPerThread, (int) result.get());
      }
    } finally {
      pool.shutdownNow();
    }
  }

  private int scan(HoodieSchema readerSchema) throws Exception {
    try (HoodieMergedLogRecordScanner scanner = HoodieMergedLogRecordScanner.newBuilder()
        .withStorage(storage)
        .withBasePath(basePath)
        .withLogFilePaths(logFilePaths)
        .withReaderSchema(readerSchema)
        .withLatestInstantTime("100")
        .withMaxMemorySizeInBytes(16L * 1024 * 1024)
        .withReverseReader(false)
        .withBufferSize(4096)
        .withSpillableMapBasePath(tempDir.resolve("spill-" + UUID.randomUUID()).toString())
        .build()) {
      int count = 0;
      for (HoodieRecord<?> ignored : scanner) {
        count++;
      }
      return count;
    }
  }

  /** The writer schema with a unique property: equal in content, but a schema no record context has seen. */
  private HoodieSchema uniqueReaderSchema() {
    Schema avroSchema = new Schema.Parser().parse(writerSchema.toString());
    avroSchema.addProp("test.instance", UUID.randomUUID().toString());
    return HoodieSchema.fromAvroSchema(avroSchema);
  }

  private static HoodieSchema probeSchema() {
    return HoodieSchema.parse("{\"type\":\"record\",\"name\":\"Probe" + UUID.randomUUID().toString().replace("-", "")
        + "\",\"fields\":[]}");
  }
}
