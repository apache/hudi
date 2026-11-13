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

package org.apache.hudi.client.functional;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordDelegate;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.HoodieMetadataWriteUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieJavaTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;
import org.apache.hudi.testutils.HoodieMergeOnReadTestUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.io.ArrayWritable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieRecord.COMMIT_SEQNO_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.FILENAME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Writes of the Java write client with {@link HoodieWriteConfig#JAVA_WRITE_MAX_PARALLELISM} above 1: file groups are
 * written on several threads at once, and the table, its files and its timeline equal those of a write on one thread.
 */
@Timeout(value = 10, unit = TimeUnit.MINUTES)
public class TestHoodieJavaClientConcurrentWrites extends HoodieJavaClientTestHarness {

  private static final String[] PARTITIONS = {"2020/01/01", "2020/01/02", "2020/01/03", "2020/01/04",
      "2020/01/05", "2020/01/06", "2020/01/07", "2020/01/08"};
  private static final String WRITE_THREAD_PREFIX = "hoodie-java-write";
  private static final String QUEUE_THREAD_PREFIX = "executor-queue-";
  private static final long GATE_WAIT_SECONDS = 20;

  @AfterEach
  void releaseGate() {
    GateWriteStatus.reset();
  }

  private HoodieWriteConfig.Builder configBuilder(String path, int maxParallelism) {
    return getConfigBuilder(TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.SIMPLE, HoodieFailedWritesCleaningPolicy.EAGER)
        .withPath(path)
        .withWriteStatusClass(GateWriteStatus.class)
        .withJavaWriteMaxParallelism(maxParallelism)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .compactionSmallFileSize(1024 * 1024).insertSplitSize(20).autoTuneInsertSplits(false).build());
  }

  private HoodieTableMetaClient initTable(String path, HoodieTableType tableType) throws IOException {
    return HoodieTestUtils.init(storageConf, path, tableType);
  }

  private static List<HoodieRecord> copy(List<HoodieRecord> records) {
    Schema schema = new Schema.Parser().parse(TRIP_EXAMPLE_SCHEMA);
    return records.stream().map(r -> (HoodieRecord) new HoodieAvroIndexedRecord(
            new HoodieKey(r.getRecordKey(), r.getPartitionPath()),
            GenericData.get().deepCopy(schema, (IndexedRecord) r.getData())))
        .collect(Collectors.toList());
  }

  private static List<HoodieRecord> copyWithLocation(List<HoodieRecord> tagged) {
    List<HoodieRecord> copies = copy(tagged);
    for (int i = 0; i < copies.size(); i++) {
      if (tagged.get(i).getCurrentLocation() != null) {
        copies.get(i).unseal();
        copies.get(i).setCurrentLocation(tagged.get(i).getCurrentLocation());
        copies.get(i).seal();
      }
    }
    return copies;
  }

  // Concurrency, measured inside the write: the gate holds the first record of each file group until enough
  // file groups are being written at the same time.

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  void testInsertBucketsAreWrittenConcurrently(HoodieTableType tableType) throws IOException {
    initTable(basePath, tableType);
    HoodieJavaWriteClient client = getHoodieWriteClient(configBuilder(basePath, 4).build());
    GateWriteStatus.arm(4);
    String instant = WriteClientTestUtils.createNewInstantTime();
    WriteClientTestUtils.startCommitWithTime(client, instant);
    List<WriteStatus> statuses = client.insert(new HoodieTestDataGenerator(PARTITIONS).generateInserts(instant, 240), instant);
    assertNoWriteErrors(statuses);
    GateWriteStatus.assertOpened(4);
  }

  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  void testUpdateBucketsAreWrittenConcurrently(HoodieTableType tableType) throws IOException {
    initTable(basePath, tableType);
    HoodieWriteConfig config = configBuilder(basePath, 4)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(0).insertSplitSize(1000).build())
        .build();
    HoodieJavaWriteClient client = getHoodieWriteClient(config);
    HoodieTestDataGenerator generator = new HoodieTestDataGenerator(PARTITIONS);
    String first = WriteClientTestUtils.createNewInstantTime();
    WriteClientTestUtils.startCommitWithTime(client, first);
    List<HoodieRecord> inserts = generator.generateInserts(first, 160);
    commitWithoutErrors(client, first, client.insert(copy(inserts), first));

    GateWriteStatus.arm(4);
    String second = WriteClientTestUtils.createNewInstantTime();
    WriteClientTestUtils.startCommitWithTime(client, second);
    assertNoWriteErrors(client.upsert(generator.generateUpdates(second, inserts), second));
    GateWriteStatus.assertOpened(4);
  }

  @Test
  void testLogAppendsAreWrittenConcurrently() throws IOException {
    initTable(basePath, HoodieTableType.MERGE_ON_READ);
    HoodieWriteConfig config = configBuilder(basePath, 4)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
        .build();
    HoodieJavaWriteClient client = getHoodieWriteClient(config);
    GateWriteStatus.arm(4);
    String instant = WriteClientTestUtils.createNewInstantTime();
    WriteClientTestUtils.startCommitWithTime(client, instant);
    assertNoWriteErrors(client.insert(new HoodieTestDataGenerator(PARTITIONS).generateInserts(instant, 240), instant));
    GateWriteStatus.assertOpened(4);
    String partition = PARTITIONS[0];
    List<StoragePathInfo> files = storage.listDirectEntries(new StoragePath(basePath, partition));
    assertTrue(files.stream().anyMatch(f -> FSUtils.isLogFile(f.getPath())), "Inserts go to log files with this index");
  }

  @Test
  void testQueueBasedWriteExecutorWritesConcurrently() throws IOException {
    initTable(basePath, HoodieTableType.COPY_ON_WRITE);
    HoodieJavaWriteClient client = getHoodieWriteClient(configBuilder(basePath, 4).withExecutorType("BOUNDED_IN_MEMORY").build());
    GateWriteStatus.arm(4);
    String instant = WriteClientTestUtils.createNewInstantTime();
    WriteClientTestUtils.startCommitWithTime(client, instant);
    assertNoWriteErrors(client.insert(new HoodieTestDataGenerator(PARTITIONS).generateInserts(instant, 240), instant));
    GateWriteStatus.assertOpened(4);
  }

  // Default scope.

  @Test
  void testDefaultWritesFileGroupsOnTheCallingThread() throws IOException {
    initTable(basePath, HoodieTableType.COPY_ON_WRITE);
    HoodieWriteConfig config = getConfigBuilder(TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.SIMPLE, HoodieFailedWritesCleaningPolicy.EAGER)
        .withWriteStatusClass(GateWriteStatus.class).build();
    assertEquals(1, config.getJavaWriteMaxParallelism());
    HoodieJavaWriteClient client = getHoodieWriteClient(config);
    GateWriteStatus.record();
    String instant = WriteClientTestUtils.createNewInstantTime();
    WriteClientTestUtils.startCommitWithTime(client, instant);
    assertNoWriteErrors(client.insert(new HoodieTestDataGenerator(PARTITIONS).generateInserts(instant, 80), instant));
    assertEquals(Collections.singleton(threadIdentity(Thread.currentThread())), GateWriteStatus.writerThreads());
  }

  @Test
  void testOneBucketStartsNoPool() throws IOException {
    initTable(basePath, HoodieTableType.COPY_ON_WRITE);
    HoodieJavaWriteClient client = getHoodieWriteClient(configBuilder(basePath, 4)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().insertSplitSize(1000).autoTuneInsertSplits(false).build()).build());
    GateWriteStatus.record();
    String instant = WriteClientTestUtils.createNewInstantTime();
    WriteClientTestUtils.startCommitWithTime(client, instant);
    assertNoWriteErrors(client.insert(new HoodieTestDataGenerator(new String[] {PARTITIONS[0]}).generateInserts(instant, 50), instant));
    assertEquals(Collections.singleton(threadIdentity(Thread.currentThread())), GateWriteStatus.writerThreads());
  }

  @Test
  void testMetadataTableWritesKeepOneThread() throws IOException {
    initTable(basePath, HoodieTableType.COPY_ON_WRITE);
    HoodieWriteConfig config = configBuilder(basePath, 4)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build()).build();
    HoodieWriteConfig metadataConfig = HoodieMetadataWriteUtils.createMetadataWriteConfig(
        config, HoodieFailedWritesCleaningPolicy.EAGER, HoodieTableVersion.current());
    assertEquals(1, metadataConfig.getJavaWriteMaxParallelism());

    HoodieJavaWriteClient client = getHoodieWriteClient(config);
    String instant = WriteClientTestUtils.createNewInstantTime();
    WriteClientTestUtils.startCommitWithTime(client, instant);
    List<HoodieRecord> inserts = new HoodieTestDataGenerator(PARTITIONS).generateInserts(instant, 160);
    commitWithoutErrors(client, instant, client.insert(inserts, instant));
    HoodieTableMetaClient reloaded = HoodieTableMetaClient.reload(createMetaClient(basePath));
    assertTrue(reloaded.getTableConfig().isMetadataTableAvailable());
    assertEquals(160, readRows(basePath, HoodieTableType.COPY_ON_WRITE).size());
  }

  @Test
  void testParallelismMustBePositive() {
    assertThrows(IllegalArgumentException.class, () -> configBuilder(basePath, 0).build());
    assertThrows(IllegalArgumentException.class, () -> configBuilder(basePath, -2).build());
  }

  // Parity: the same writes on two tables, one thread against four.

  private static Stream<Arguments> parityCases() {
    return Stream.of(
        Arguments.of(HoodieTableType.COPY_ON_WRITE, false, "SIMPLE"),
        Arguments.of(HoodieTableType.COPY_ON_WRITE, true, "SIMPLE"),
        Arguments.of(HoodieTableType.MERGE_ON_READ, false, "SIMPLE"),
        Arguments.of(HoodieTableType.MERGE_ON_READ, true, "SIMPLE"),
        Arguments.of(HoodieTableType.COPY_ON_WRITE, false, "BOUNDED_IN_MEMORY"));
  }

  @ParameterizedTest
  @MethodSource("parityCases")
  void testWritesEqualTheSerialWrites(HoodieTableType tableType, boolean timelineServer, String executorType) throws Exception {
    String serialPath = basePath + "/serial";
    String parallelPath = basePath + "/parallel";
    initTable(serialPath, tableType);
    initTable(parallelPath, tableType);
    HoodieWriteConfig serialConfig = configBuilder(serialPath, 1)
        .withEmbeddedTimelineServerEnabled(timelineServer).withExecutorType(executorType).build();
    HoodieWriteConfig parallelConfig = configBuilder(parallelPath, 4)
        .withEmbeddedTimelineServerEnabled(timelineServer).withExecutorType(executorType).build();
    try (TwinWriter twin = new TwinWriter(serialConfig, parallelConfig, tableType)) {
      HoodieTestDataGenerator generator = new HoodieTestDataGenerator(PARTITIONS);

      String instant = twin.next();
      List<HoodieRecord> batch = generator.generateInserts(instant, 480);
      twin.write(instant, (c, records) -> c.insert(records, instant), batch);

      String upsert = twin.next();
      List<HoodieRecord> mixed = new ArrayList<>(generator.generateUpdates(upsert, batch.subList(0, 200)));
      mixed.addAll(generator.generateInserts(upsert, 100));
      twin.write(upsert, (c, records) -> c.upsert(records, upsert), mixed);

      if (tableType == HoodieTableType.COPY_ON_WRITE) {
        // Merge-on-read prepped upserts have their own write loop.
        String prepped = twin.next();
        List<HoodieRecord> preppedUpdates = generator.generateUpdates(prepped, batch.subList(200, 320));
        twin.writePrepped(prepped, (c, records) -> c.upsertPreppedRecords(records, prepped), preppedUpdates);
      }

      String preppedInsert = twin.next();
      twin.write(preppedInsert, (c, records) -> c.insertPreppedRecords(records, preppedInsert), generator.generateInserts(preppedInsert, 80));

      String delete = twin.next();
      List<HoodieKey> deletes = batch.subList(320, 370).stream().map(HoodieRecord::getKey).collect(Collectors.toList());
      twin.writeKeys(delete, deletes);

      String bulk = twin.next();
      twin.write(bulk, (c, records) -> c.bulkInsert(records, bulk), generator.generateInserts(bulk, 60));

      if (tableType == HoodieTableType.COPY_ON_WRITE) {
        String overwrite = twin.nextReplace();
        twin.insertOverwrite(overwrite, generator.generateInsertsForPartition(overwrite, 40, PARTITIONS[2]));
      }
    }
  }

  @RepeatedTest(10)
  void testRepeatedWritesEqualTheSerialWrites() throws Exception {
    String serialPath = basePath + "/serial";
    String parallelPath = basePath + "/parallel";
    initTable(serialPath, HoodieTableType.COPY_ON_WRITE);
    initTable(parallelPath, HoodieTableType.COPY_ON_WRITE);
    try (TwinWriter twin = new TwinWriter(configBuilder(serialPath, 1).build(), configBuilder(parallelPath, 4).build(),
        HoodieTableType.COPY_ON_WRITE)) {
      HoodieTestDataGenerator generator = new HoodieTestDataGenerator(Arrays.copyOf(PARTITIONS, 4));
      String instant = twin.next();
      List<HoodieRecord> batch = generator.generateInserts(instant, 160);
      twin.write(instant, (c, records) -> c.insert(records, instant), batch);
      String upsert = twin.next();
      List<HoodieRecord> mixed = new ArrayList<>(generator.generateUpdates(upsert, batch.subList(0, 80)));
      mixed.addAll(generator.generateInserts(upsert, 40));
      twin.write(upsert, (c, records) -> c.upsert(records, upsert), mixed);
    }
  }

  @Test
  void testRolloverWithinABucketEqualsTheSerialWrite() throws Exception {
    String serialPath = basePath + "/serial";
    String parallelPath = basePath + "/parallel";
    initTable(serialPath, HoodieTableType.COPY_ON_WRITE);
    initTable(parallelPath, HoodieTableType.COPY_ON_WRITE);
    HoodieStorageConfig smallFiles = HoodieStorageConfig.newBuilder().parquetMaxFileSize(16 * 1024).build();
    HoodieCompactionConfig oneBucketPerPartition = HoodieCompactionConfig.newBuilder()
        .compactionSmallFileSize(0).insertSplitSize(100000).autoTuneInsertSplits(false).build();
    try (TwinWriter twin = new TwinWriter(
        configBuilder(serialPath, 1).withStorageConfig(smallFiles).withCompactionConfig(oneBucketPerPartition).build(),
        configBuilder(parallelPath, 4).withStorageConfig(smallFiles).withCompactionConfig(oneBucketPerPartition).build(),
        HoodieTableType.COPY_ON_WRITE)) {
      HoodieTestDataGenerator generator = new HoodieTestDataGenerator(Arrays.copyOf(PARTITIONS, 2));
      String instant = twin.next();
      twin.write(instant, (c, records) -> c.insert(records, instant), generator.generateInserts(instant, 2000));
      for (String partition : Arrays.copyOf(PARTITIONS, 2)) {
        assertTrue(fileGroups(parallelPath, partition).size() > 1, "The insert rolls over into several file groups");
      }
    }
  }

  @Test
  void testRecordErrorsAreCountedAsInTheSerialWrite() throws Exception {
    String serialPath = basePath + "/serial";
    String parallelPath = basePath + "/parallel";
    initTable(serialPath, HoodieTableType.COPY_ON_WRITE);
    initTable(parallelPath, HoodieTableType.COPY_ON_WRITE);
    try (HoodieJavaWriteClient serial = new HoodieJavaWriteClient(context, configBuilder(serialPath, 1).build());
         HoodieJavaWriteClient parallel = new HoodieJavaWriteClient(context, configBuilder(parallelPath, 4).build())) {
      HoodieTestDataGenerator generator = new HoodieTestDataGenerator(PARTITIONS);
      String instant = WriteClientTestUtils.createNewInstantTime();
      List<HoodieRecord> records = generator.generateInserts(instant, 240);
      WriteClientTestUtils.startCommitWithTime(serial, instant);
      WriteClientTestUtils.startCommitWithTime(parallel, instant);
      List<WriteStatus> serialStatuses = serial.insert(withBadRecords(records), instant);
      List<WriteStatus> parallelStatuses = parallel.insert(withBadRecords(records), instant);
      long serialErrors = serialStatuses.stream().mapToLong(WriteStatus::getTotalErrorRecords).sum();
      assertEquals(2, serialErrors, "The two bad records are reported as record errors");
      assertEquals(serialErrors, parallelStatuses.stream().mapToLong(WriteStatus::getTotalErrorRecords).sum());
      assertEquals(statusSummary(serialStatuses), statusSummary(parallelStatuses));
      assertEquals(commitOutcome(serial, instant, serialStatuses), commitOutcome(parallel, instant, parallelStatuses));
      assertEquals(readRows(serialPath, HoodieTableType.COPY_ON_WRITE), readRows(parallelPath, HoodieTableType.COPY_ON_WRITE));
    }
  }

  private static String commitOutcome(HoodieJavaWriteClient client, String instant, List<WriteStatus> statuses) {
    try {
      return "committed=" + client.commit(instant, statuses);
    } catch (RuntimeException e) {
      return "failed " + e.getClass().getName();
    }
  }

  private static void commitWithoutErrors(HoodieJavaWriteClient client, String instant, List<WriteStatus> statuses) {
    assertNoWriteErrors(statuses);
    assertTrue(client.commit(instant, statuses));
  }

  private static List<HoodieRecord> withBadRecords(List<HoodieRecord> records) {
    List<HoodieRecord> withBad = new ArrayList<>(copy(records));
    for (String partition : Arrays.asList(PARTITIONS[1], PARTITIONS[5])) {
      withBad.add(badRecord(partition));
    }
    return withBad;
  }

  private static HoodieRecord badRecord(String partition) {
    Schema wrongSchema = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"wrong\",\"fields\":[{\"name\":\"_row_key\",\"type\":\"string\"}]}");
    GenericRecord bad = new GenericData.Record(wrongSchema);
    bad.put("_row_key", "bad-" + partition);
    return new HoodieAvroIndexedRecord(new HoodieKey("bad-" + partition, partition), bad);
  }

  // A failing file group while others are being written.

  @ParameterizedTest
  @EnumSource(value = MarkerType.class, names = {"DIRECT", "TIMELINE_SERVER_BASED"})
  void testFailedFileGroupStopsTheWriteAfterRunningWritersFinish(MarkerType markerType) throws Exception {
    String serialPath = basePath + "/serial";
    String parallelPath = basePath + "/parallel";
    initTable(serialPath, HoodieTableType.COPY_ON_WRITE);
    initTable(parallelPath, HoodieTableType.COPY_ON_WRITE);
    HoodieCompactionConfig oneFileGroupPerPartition = HoodieCompactionConfig.newBuilder()
        .compactionSmallFileSize(0).insertSplitSize(100000).autoTuneInsertSplits(false).build();
    HoodieWriteConfig serialConfig = configBuilder(serialPath, 1).withCompactionConfig(oneFileGroupPerPartition)
        .withMarkersType(markerType.name()).withEmbeddedTimelineServerEnabled(markerType == MarkerType.TIMELINE_SERVER_BASED).build();
    HoodieWriteConfig parallelConfig = configBuilder(parallelPath, PARTITIONS.length).withCompactionConfig(oneFileGroupPerPartition)
        .withMarkersType(markerType.name()).withEmbeddedTimelineServerEnabled(markerType == MarkerType.TIMELINE_SERVER_BASED).build();
    HoodieTestDataGenerator generator = new HoodieTestDataGenerator(PARTITIONS);
    String first = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> inserts = generator.generateInserts(first, 400);
    String second = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> updates = generator.generateUpdates(second, inserts);

    Throwable serialFailure = runWriteWithACorruptFileGroup(serialConfig, first, second, inserts, updates, false);
    Throwable parallelFailure = runWriteWithACorruptFileGroup(parallelConfig, first, second, inserts, updates, true);
    assertEquals(serialFailure.getClass(), parallelFailure.getClass());
    assertEquals(rootCause(serialFailure).getClass(), rootCause(parallelFailure).getClass());
  }

  @Test
  void testFailedInsertOnAQueueBasedExecutorStopsTheWriteAfterRunningWritersFinish() throws Exception {
    initTable(basePath, HoodieTableType.COPY_ON_WRITE);
    String failingPartition = PARTITIONS[0];
    String siblingPartition = PARTITIONS[1];
    HoodieWriteConfig config = configBuilder(basePath, 2)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .compactionSmallFileSize(0).insertSplitSize(100000).autoTuneInsertSplits(false).build())
        .withExecutorType("BOUNDED_IN_MEMORY")
        .withWriteIgnoreFailed(false)
        .build();
    String instant = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> inserts = new HoodieTestDataGenerator(new String[] {failingPartition, siblingPartition})
        .generateInserts(instant, 200);
    long siblingRows = inserts.stream().filter(r -> r.getPartitionPath().equals(siblingPartition)).count();
    List<HoodieRecord> input = new ArrayList<>();
    input.add(badRecord(failingPartition));
    input.addAll(copy(inserts));
    Set<Thread> queueThreadsBefore = threads(QUEUE_THREAD_PREFIX);
    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(context, config)) {
      GateWriteStatus.holdSiblingUntilTheFailureIsRecorded(failingPartition, siblingPartition);
      WriteClientTestUtils.startCommitWithTime(client, instant);
      assertThrows(HoodieException.class, () -> client.insert(input, instant));

      assertTrue(liveThreads(WRITE_THREAD_PREFIX).isEmpty(), "No write thread runs after the write fails");
      Set<Thread> queueThreadsLeft = new HashSet<>(liveThreads(QUEUE_THREAD_PREFIX));
      queueThreadsLeft.removeAll(queueThreadsBefore);
      assertTrue(queueThreadsLeft.isEmpty(), "No queue thread of the write runs after it fails: " + queueThreadsLeft);
      assertSiblingFinishedBeforeTheCallReturned(basePath, siblingPartition, instant, siblingRows);
      assertEveryFileOfTheInstantIsMarked(config, basePath, instant);
      assertTrue(client.rollback(instant));
      assertTrue(dataFiles(basePath).isEmpty(), "Rollback removes every file of the failed write");
    } finally {
      GateWriteStatus.reset();
    }
  }

  private void assertSiblingFinishedBeforeTheCallReturned(String path, String partition, String instant, long expectedRows)
      throws IOException {
    GateWriteStatus.assertSiblingHeldUntilTheFailureWasRecorded();
    Map<String, Long> files = filesOfInstant(path, partition, instant);
    assertEquals(1, files.size(), "The sibling file group was written: " + files);
    StoragePath file = new StoragePath(path, files.keySet().iterator().next());
    assertEquals(expectedRows, HoodieIOFactory.getIOFactory(storage).getFileFormatUtils(file).getRowCount(storage, file),
        "The sibling file is complete when the write fails");
    assertEquals(expectedRows, GateWriteStatus.siblingRecords());
  }

  private void assertEveryFileOfTheInstantIsMarked(HoodieWriteConfig config, String path, String instant) throws IOException {
    HoodieTable afterFailure = HoodieJavaTable.create(config, context, HoodieTableMetaClient.reload(createMetaClient(path)));
    Set<String> marked = WriteMarkersFactory.get(config.getMarkersType(), afterFailure, instant).createdAndMergedDataPaths(context, 2);
    Set<String> written = new TreeSet<>();
    for (String partition : PARTITIONS) {
      if (storage.exists(new StoragePath(path, partition))) {
        filesOfInstant(path, partition, instant).keySet().forEach(written::add);
      }
    }
    assertEquals(written, new TreeSet<>(marked), "Every file the failed write created has a marker");
  }

  private Throwable runWriteWithACorruptFileGroup(HoodieWriteConfig config, String first, String second,
                                                  List<HoodieRecord> inserts, List<HoodieRecord> updates, boolean withSlowSibling) throws Exception {
    String path = config.getBasePath();
    String corruptPartition = PARTITIONS[0];
    String slowPartition = PARTITIONS[1];
    try (HoodieJavaWriteClient client = new HoodieJavaWriteClient(context, config)) {
      WriteClientTestUtils.startCommitWithTime(client, first);
      commitWithoutErrors(client, first, client.insert(copy(inserts), first));
      Map<String, Long> filesBefore = dataFiles(path);
      Map<String, String> rowsBefore = readRows(path, HoodieTableType.COPY_ON_WRITE);

      HoodieTable table = HoodieJavaTable.create(config, context, HoodieTableMetaClient.reload(createMetaClient(path)));
      List<HoodieRecord> tagged = tagLocation(table.getIndex(), context, copy(updates), table);
      StoragePath corruptFile = baseFiles(path, corruptPartition).get(0);
      byte[] original = readBytes(corruptFile);
      try (OutputStream out = storage.create(corruptFile, true)) {
        out.write(new byte[] {1, 2, 3});
      }

      if (withSlowSibling) {
        GateWriteStatus.holdSiblingUntilTheFailureIsRecorded(corruptPartition, slowPartition);
      }
      WriteClientTestUtils.startCommitWithTime(client, second);
      Throwable failure = assertThrows(HoodieUpsertException.class, () -> client.upsertPreppedRecords(copyWithLocation(tagged), second));

      assertTrue(liveThreads(WRITE_THREAD_PREFIX).isEmpty(), "No write thread runs after the write fails");
      if (withSlowSibling) {
        assertSiblingFinishedBeforeTheCallReturned(path, slowPartition, second,
            inserts.stream().filter(r -> r.getPartitionPath().equals(slowPartition)).count());
      }
      assertEveryFileOfTheInstantIsMarked(config, path, second);

      try (OutputStream out = storage.create(corruptFile, true)) {
        out.write(original);
      }
      assertTrue(client.rollback(second));
      assertEquals(filesBefore, dataFiles(path), "Rollback removes every file of the failed write");
      assertEquals(rowsBefore, readRows(path, HoodieTableType.COPY_ON_WRITE));
      return failure;
    } finally {
      GateWriteStatus.reset();
    }
  }

  // Helpers.

  @FunctionalInterface
  private interface WriteFunction {
    List<WriteStatus> write(HoodieJavaWriteClient client, List<HoodieRecord> records);
  }

  /**
   * Writes the same input to a table at parallelism 1 and to a byte copy of it at the configured parallelism, so each
   * step starts from identical file groups, and compares the outcome.
   */
  private final class TwinWriter implements AutoCloseable {
    private final HoodieWriteConfig serialConfig;
    private final HoodieWriteConfig parallelConfig;
    private final String serialPath;
    private final String parallelPath;
    private final HoodieTableType tableType;
    private HoodieJavaWriteClient serial;
    private HoodieJavaWriteClient parallel;

    TwinWriter(HoodieWriteConfig serialConfig, HoodieWriteConfig parallelConfig, HoodieTableType tableType) {
      this.serialConfig = serialConfig;
      this.parallelConfig = parallelConfig;
      this.serialPath = serialConfig.getBasePath();
      this.parallelPath = parallelConfig.getBasePath();
      this.tableType = tableType;
    }

    String next() throws IOException {
      return start(Option.empty());
    }

    String nextReplace() throws IOException {
      return start(Option.of(REPLACE_COMMIT_ACTION));
    }

    private String start(Option<String> action) throws IOException {
      close();
      replaceWithCopy(new java.io.File(serialPath).toPath(), new java.io.File(parallelPath).toPath());
      serial = new HoodieJavaWriteClient(context, serialConfig);
      parallel = new HoodieJavaWriteClient(context, parallelConfig);
      String instant = WriteClientTestUtils.createNewInstantTime();
      if (action.isPresent()) {
        WriteClientTestUtils.startCommitWithTime(serial, instant, action.get());
        WriteClientTestUtils.startCommitWithTime(parallel, instant, action.get());
      } else {
        WriteClientTestUtils.startCommitWithTime(serial, instant);
        WriteClientTestUtils.startCommitWithTime(parallel, instant);
      }
      return instant;
    }

    @Override
    public void close() {
      if (serial != null) {
        serial.close();
        serial = null;
      }
      if (parallel != null) {
        parallel.close();
        parallel = null;
      }
    }

    void write(String instant, WriteFunction function, List<HoodieRecord> records) throws IOException {
      List<WriteStatus> serialStatuses = function.write(serial, copy(records));
      List<WriteStatus> parallelStatuses = function.write(parallel, copy(records));
      compare(instant, serialStatuses, parallelStatuses);
    }

    void writePrepped(String instant, WriteFunction function, List<HoodieRecord> records) throws IOException {
      List<HoodieRecord> serialInput = copyWithLocation(tag(serial, serialPath, records));
      List<HoodieRecord> parallelInput = copyWithLocation(tag(parallel, parallelPath, records));
      compare(instant, function.write(serial, serialInput), function.write(parallel, parallelInput));
    }

    void writeKeys(String instant, List<HoodieKey> keys) throws IOException {
      compare(instant, serial.delete(new ArrayList<>(keys), instant), parallel.delete(new ArrayList<>(keys), instant));
    }

    void insertOverwrite(String instant, List<HoodieRecord> records) throws IOException {
      HoodieTable serialTable = HoodieJavaTable.create(serial.getConfig(), context, HoodieTableMetaClient.reload(createMetaClient(serialPath)));
      HoodieTable parallelTable = HoodieJavaTable.create(parallel.getConfig(), context, HoodieTableMetaClient.reload(createMetaClient(parallelPath)));
      HoodieWriteMetadata<List<WriteStatus>> serialResult = serialTable.insertOverwrite(context, instant, copy(records));
      HoodieWriteMetadata<List<WriteStatus>> parallelResult = parallelTable.insertOverwrite(context, instant, copy(records));
      assertEquals(statusSummary(serialResult.getWriteStatuses()), statusSummary(parallelResult.getWriteStatuses()));
      assertEquals(serialResult.getPartitionToReplaceFileIds().keySet(), parallelResult.getPartitionToReplaceFileIds().keySet());
      assertTrue(serial.commit(instant, serialResult.getWriteStatuses(), Option.empty(), REPLACE_COMMIT_ACTION,
          serialResult.getPartitionToReplaceFileIds()));
      assertTrue(parallel.commit(instant, parallelResult.getWriteStatuses(), Option.empty(), REPLACE_COMMIT_ACTION,
          parallelResult.getPartitionToReplaceFileIds()));
      assertEquals(readRows(serialPath, tableType), readRows(parallelPath, tableType));
      assertEquals(fileStructure(serialPath), fileStructure(parallelPath));
      assertEquals(completedInstants(serialPath), completedInstants(parallelPath));
    }

    private List<HoodieRecord> tag(HoodieJavaWriteClient client, String path, List<HoodieRecord> records) {
      HoodieTable table = HoodieJavaTable.create(client.getConfig(), context, HoodieTableMetaClient.reload(createMetaClient(path)));
      return tagLocation(table.getIndex(), context, copy(records), table);
    }

    private void compare(String instant, List<WriteStatus> serialStatuses, List<WriteStatus> parallelStatuses) throws IOException {
      assertEquals(statusSummary(serialStatuses), statusSummary(parallelStatuses));
      commitWithoutErrors(serial, instant, serialStatuses);
      commitWithoutErrors(parallel, instant, parallelStatuses);
      assertEquals(readRows(serialPath, tableType), readRows(parallelPath, tableType));
      assertEquals(fileStructure(serialPath), fileStructure(parallelPath));
      assertEquals(commitCounts(serialPath, instant), commitCounts(parallelPath, instant));
      assertEquals(completedInstants(serialPath), completedInstants(parallelPath));
      for (String partition : PARTITIONS) {
        assertOnePartitionMetafile(parallelPath, partition);
      }
    }
  }

  private static void replaceWithCopy(java.nio.file.Path source, java.nio.file.Path target) throws IOException {
    if (java.nio.file.Files.exists(target)) {
      try (Stream<java.nio.file.Path> paths = java.nio.file.Files.walk(target)) {
        for (java.nio.file.Path path : paths.sorted(Collections.reverseOrder()).collect(Collectors.toList())) {
          java.nio.file.Files.delete(path);
        }
      }
    }
    try (Stream<java.nio.file.Path> paths = java.nio.file.Files.walk(source)) {
      for (java.nio.file.Path path : paths.collect(Collectors.toList())) {
        java.nio.file.Path copy = target.resolve(source.relativize(path).toString());
        if (java.nio.file.Files.isDirectory(path)) {
          java.nio.file.Files.createDirectories(copy);
        } else {
          java.nio.file.Files.copy(path, copy);
        }
      }
    }
  }

  private static List<String> statusSummary(List<WriteStatus> statuses) {
    return statuses.stream().map(s -> {
      HoodieWriteStat stat = s.getStat();
      return s.getPartitionPath() + " writes=" + stat.getNumWrites() + " inserts=" + stat.getNumInserts()
          + " updates=" + stat.getNumUpdateWrites() + " deletes=" + stat.getNumDeletes() + " errors=" + s.getTotalErrorRecords();
    }).sorted().collect(Collectors.toList());
  }

  private Map<String, String> readRows(String path, HoodieTableType tableType) {
    List<String> partitionPaths = Arrays.stream(PARTITIONS).map(p -> new StoragePath(path, p).toString()).collect(Collectors.toList());
    List<GenericRecord> records = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(
        storageConf, partitionPaths, path, new org.apache.hadoop.mapred.JobConf(storageConf.unwrapAs(org.apache.hadoop.conf.Configuration.class)),
        tableType == HoodieTableType.MERGE_ON_READ);
    Map<String, String> rows = new TreeMap<>();
    for (GenericRecord record : records) {
      StringBuilder row = new StringBuilder();
      for (Schema.Field field : record.getSchema().getFields()) {
        if (!field.name().equals(FILENAME_METADATA_FIELD) && !field.name().equals(COMMIT_SEQNO_METADATA_FIELD)) {
          row.append(field.name()).append('=').append(valueString(record.get(field.pos()))).append(';');
        }
      }
      rows.put(record.get(RECORD_KEY_METADATA_FIELD).toString(), row.toString());
    }
    return rows;
  }

  private static String valueString(Object value) {
    if (value instanceof ArrayWritable) {
      return Arrays.stream(((ArrayWritable) value).get()).map(TestHoodieJavaClientConcurrentWrites::valueString)
          .collect(Collectors.joining(",", "[", "]"));
    }
    return String.valueOf(value);
  }

  private Map<String, Map<Set<String>, String>> fileStructure(String path) throws IOException {
    List<String> partitionPaths = Arrays.stream(PARTITIONS).map(p -> new StoragePath(path, p).toString()).collect(Collectors.toList());
    HoodieTableMetaClient reloaded = HoodieTableMetaClient.reload(createMetaClient(path));
    List<GenericRecord> records = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(
        storageConf, partitionPaths, path, new org.apache.hadoop.mapred.JobConf(storageConf.unwrapAs(org.apache.hadoop.conf.Configuration.class)),
        reloaded.getTableType() == HoodieTableType.MERGE_ON_READ);
    Map<String, Set<String>> keysByFileGroup = new HashMap<>();
    for (GenericRecord record : records) {
      String fileName = record.get(FILENAME_METADATA_FIELD).toString();
      String fileId = FSUtils.isLogFile(fileName) ? FSUtils.getFileIdFromLogPath(new StoragePath(fileName)) : FSUtils.getFileId(fileName);
      keysByFileGroup.computeIfAbsent(fileId, k -> new TreeSet<>()).add(record.get(RECORD_KEY_METADATA_FIELD).toString());
    }
    Map<String, Map<Set<String>, String>> structure = new TreeMap<>();
    for (String partition : PARTITIONS) {
      Map<Set<String>, String> groups = new HashMap<>();
      int emptyGroups = 0;
      for (Map.Entry<String, int[]> group : fileGroups(path, partition).entrySet()) {
        Set<String> keys = keysByFileGroup.getOrDefault(group.getKey(), Collections.emptySet());
        String files = "base=" + group.getValue()[0] + " log=" + group.getValue()[1];
        if (keys.isEmpty()) {
          groups.put(Collections.singleton("empty-" + emptyGroups++), files);
        } else {
          groups.put(keys, files);
        }
      }
      structure.put(partition, groups);
    }
    return structure;
  }

  private Map<String, int[]> fileGroups(String path, String partition) throws IOException {
    Map<String, int[]> groups = new TreeMap<>();
    StoragePath partitionPath = new StoragePath(path, partition);
    if (!storage.exists(partitionPath)) {
      return groups;
    }
    for (StoragePathInfo info : storage.listDirectEntries(partitionPath)) {
      String name = info.getPath().getName();
      if (FSUtils.isLogFile(info.getPath())) {
        groups.computeIfAbsent(FSUtils.getFileIdFromLogPath(info.getPath()), k -> new int[2])[1]++;
      } else if (name.endsWith(".parquet")) {
        groups.computeIfAbsent(FSUtils.getFileId(name), k -> new int[2])[0]++;
      }
    }
    return groups;
  }

  private Map<String, Long> commitCounts(String path, String instant) throws IOException {
    HoodieTableMetaClient reloaded = HoodieTableMetaClient.reload(createMetaClient(path));
    HoodieTimeline timeline = reloaded.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
    HoodieInstant completed = timeline.getInstants().stream().filter(i -> i.requestedTime().equals(instant)).findFirst()
        .orElseThrow(() -> new AssertionError("No completed instant " + instant));
    HoodieCommitMetadata metadata = timeline.readCommitMetadata(completed);
    Map<String, Long> counts = new TreeMap<>();
    metadata.getPartitionToWriteStats().forEach((partition, stats) -> {
      counts.put(partition + " writes", stats.stream().mapToLong(HoodieWriteStat::getNumWrites).sum());
      counts.put(partition + " inserts", stats.stream().mapToLong(HoodieWriteStat::getNumInserts).sum());
      counts.put(partition + " updates", stats.stream().mapToLong(HoodieWriteStat::getNumUpdateWrites).sum());
      counts.put(partition + " deletes", stats.stream().mapToLong(HoodieWriteStat::getNumDeletes).sum());
      counts.put(partition + " errors", stats.stream().mapToLong(HoodieWriteStat::getTotalWriteErrors).sum());
      counts.put(partition + " stats", (long) stats.size());
    });
    return counts;
  }

  private List<String> completedInstants(String path) {
    HoodieTableMetaClient reloaded = HoodieTableMetaClient.reload(createMetaClient(path));
    return reloaded.getActiveTimeline().filterCompletedInstants().getInstants().stream()
        .map(i -> i.requestedTime() + " " + i.getAction()).collect(Collectors.toList());
  }

  private void assertOnePartitionMetafile(String path, String partition) throws IOException {
    StoragePath partitionPath = new StoragePath(path, partition);
    if (!storage.exists(partitionPath)) {
      return;
    }
    List<String> metafiles = storage.listDirectEntries(partitionPath).stream().map(f -> f.getPath().getName())
        .filter(n -> n.startsWith(".hoodie_partition_metadata")).collect(Collectors.toList());
    assertEquals(1, metafiles.size(), "One partition metafile and no temporary one in " + partition + ": " + metafiles);
  }

  private Map<String, Long> dataFiles(String path) throws IOException {
    Map<String, Long> files = new TreeMap<>();
    for (String partition : PARTITIONS) {
      StoragePath partitionPath = new StoragePath(path, partition);
      if (storage.exists(partitionPath)) {
        for (StoragePathInfo info : storage.listDirectEntries(partitionPath)) {
          if (!info.getPath().getName().startsWith(".")) {
            files.put(partition + "/" + info.getPath().getName(), info.getLength());
          }
        }
      }
    }
    return files;
  }

  private Map<String, Long> filesOfInstant(String path, String partition, String instant) throws IOException {
    Map<String, Long> files = new TreeMap<>();
    StoragePath partitionPath = new StoragePath(path, partition);
    for (StoragePathInfo info : storage.listDirectEntries(partitionPath)) {
      String name = info.getPath().getName();
      if (!name.startsWith(".") && FSUtils.getCommitTime(name).equals(instant)) {
        files.put(partition + "/" + name, info.getLength());
      }
    }
    return files;
  }

  private List<StoragePath> baseFiles(String path, String partition) throws IOException {
    return storage.listDirectEntries(new StoragePath(path, partition)).stream().map(StoragePathInfo::getPath)
        .filter(p -> p.getName().endsWith(".parquet")).collect(Collectors.toList());
  }

  private byte[] readBytes(StoragePath file) throws IOException {
    try (java.io.InputStream in = storage.open(file)) {
      java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
      byte[] buffer = new byte[8192];
      int read;
      while ((read = in.read(buffer)) > 0) {
        out.write(buffer, 0, read);
      }
      return out.toByteArray();
    }
  }

  private static String threadIdentity(Thread thread) {
    return thread.getName() + "#" + thread.getId();
  }

  private static Set<Thread> threads(String prefix) {
    return Thread.getAllStackTraces().keySet().stream().filter(t -> t.getName().startsWith(prefix)).collect(Collectors.toSet());
  }

  private static List<Thread> liveThreads(String prefix) throws InterruptedException {
    List<Thread> threads = Thread.getAllStackTraces().keySet().stream()
        .filter(t -> t.getName().startsWith(prefix)).collect(Collectors.toList());
    for (Thread thread : threads) {
      thread.join(TimeUnit.SECONDS.toMillis(5));
    }
    return threads.stream().filter(Thread::isAlive).collect(Collectors.toList());
  }

  private static Throwable rootCause(Throwable t) {
    Throwable cause = t;
    while (cause.getCause() != null && cause.getCause() != cause) {
      cause = cause.getCause();
    }
    return cause;
  }

  private static void assertNoWriteErrors(List<WriteStatus> statuses) {
    for (WriteStatus status : statuses) {
      assertFalse(status.hasErrors(), "Errors found in write of " + status.getFileId());
    }
  }

  /**
   * A write status that is created per write handle on the thread that writes the file group. Tests use it to hold
   * the first record of each file group until a number of file groups are being written at the same time, to record
   * the threads that write, and to hold one file group's writer until another file group's failure has been recorded
   * by the write stage.
   */
  public static class GateWriteStatus extends WriteStatus {
    private static volatile int parties;
    private static volatile CountDownLatch open = new CountDownLatch(0);
    private static final AtomicInteger ARRIVALS = new AtomicInteger();
    private static final Set<String> THREADS = ConcurrentHashMap.newKeySet();
    private static volatile boolean timedOut;
    private static volatile boolean recording;
    private static volatile String failingPartition;
    private static volatile String siblingPartition;
    private static volatile CountDownLatch siblingWriting = new CountDownLatch(0);
    private static volatile CountDownLatch failingWriting = new CountDownLatch(0);
    private static volatile boolean siblingHeld;
    private static volatile boolean siblingReleased;
    private static volatile boolean failingStartedAfterSibling;
    private static volatile boolean failingStartedWhileSiblingHeld;
    private static volatile boolean siblingTimedOut;
    private static final AtomicLong SIBLING_RECORDS = new AtomicLong();

    private final AtomicInteger seen = new AtomicInteger();

    public GateWriteStatus(Boolean trackSuccessRecords, Double failureFraction) {
      super(trackSuccessRecords, failureFraction);
    }

    public GateWriteStatus(Boolean trackSuccessRecords, Double failureFraction, Boolean isMetadataTable) {
      super(trackSuccessRecords, failureFraction, isMetadataTable);
    }

    static void arm(int count) {
      reset();
      parties = count;
      open = new CountDownLatch(count);
      recording = true;
    }

    static void record() {
      reset();
      recording = true;
    }

    /**
     * The failing partition's writer starts once the sibling partition's writer has written its first record; the
     * sibling then waits until the failing writer has started and every other write thread has ended, which the
     * failing thread does only after the write stage has recorded its failure.
     */
    static void holdSiblingUntilTheFailureIsRecorded(String failing, String sibling) {
      reset();
      failingPartition = failing;
      siblingPartition = sibling;
      siblingWriting = new CountDownLatch(1);
      failingWriting = new CountDownLatch(1);
    }

    static void reset() {
      CountDownLatch previous = open;
      while (previous.getCount() > 0) {
        previous.countDown();
      }
      siblingWriting.countDown();
      failingWriting.countDown();
      parties = 0;
      ARRIVALS.set(0);
      THREADS.clear();
      timedOut = false;
      recording = false;
      failingPartition = null;
      siblingPartition = null;
      siblingHeld = false;
      siblingReleased = false;
      failingStartedAfterSibling = false;
      failingStartedWhileSiblingHeld = false;
      siblingTimedOut = false;
      SIBLING_RECORDS.set(0);
    }

    static Set<String> writerThreads() {
      return new HashSet<>(THREADS);
    }

    static void assertOpened(int expectedFileGroups) {
      assertFalse(timedOut, "The gate timed out: fewer than " + expectedFileGroups + " file groups were written at once");
      assertEquals(0, open.getCount());
      String caller = threadIdentity(Thread.currentThread());
      assertTrue(THREADS.stream().filter(n -> !n.equals(caller)).count() >= 2, "File groups are written on several threads: " + THREADS);
    }

    static void assertSiblingHeldUntilTheFailureWasRecorded() {
      assertTrue(siblingHeld, "The sibling file group was being written when the other file group failed");
      assertTrue(failingStartedAfterSibling, "The failing file group started after the sibling had written a record");
      assertTrue(failingStartedWhileSiblingHeld, "The failing file group was written while the sibling was still being written");
      assertFalse(siblingTimedOut, "The failure was recorded while the sibling file group was still being written");
    }

    static long siblingRecords() {
      return SIBLING_RECORDS.get();
    }

    @Override
    public void setPartitionPath(String partitionPath) {
      super.setPartitionPath(partitionPath);
      if (partitionPath != null && partitionPath.equals(failingPartition)) {
        failingStartedAfterSibling = awaitQuietly(siblingWriting);
        failingStartedWhileSiblingHeld = !siblingReleased;
        failingWriting.countDown();
      }
    }

    @Override
    public void markSuccess(HoodieRecord record, Option<Map<String, String>> optionalRecordMetadata) {
      onRecord(record.getPartitionPath());
      super.markSuccess(record, optionalRecordMetadata);
    }

    @Override
    public void markSuccess(HoodieRecordDelegate recordDelegate, Option<Map<String, String>> optionalRecordMetadata) {
      onRecord(recordDelegate.getPartitionPath());
      super.markSuccess(recordDelegate, optionalRecordMetadata);
    }

    private void onRecord(String partitionPath) {
      if (recording) {
        THREADS.add(threadIdentity(Thread.currentThread()));
      }
      boolean sibling = partitionPath != null && partitionPath.equals(siblingPartition);
      if (sibling) {
        SIBLING_RECORDS.incrementAndGet();
      }
      if (seen.getAndIncrement() > 0) {
        return;
      }
      if (sibling) {
        siblingWriting.countDown();
        siblingHeld = true;
        siblingTimedOut = !awaitQuietly(failingWriting) || !awaitOtherWriteThreadsEnded();
        siblingReleased = true;
      }
      if (parties > 0 && ARRIVALS.getAndIncrement() < parties) {
        CountDownLatch gate = open;
        gate.countDown();
        if (!awaitQuietly(gate)) {
          timedOut = true;
        }
      }
    }

    private static boolean awaitQuietly(CountDownLatch latch) {
      try {
        return latch.await(GATE_WAIT_SECONDS, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }
    }

    private static boolean awaitOtherWriteThreadsEnded() {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(GATE_WAIT_SECONDS);
      while (threads(WRITE_THREAD_PREFIX).size() > 1) {
        if (System.nanoTime() > deadline) {
          return false;
        }
        sleepQuietly(10);
      }
      return true;
    }

    private static void sleepQuietly(long millis) {
      try {
        Thread.sleep(millis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
