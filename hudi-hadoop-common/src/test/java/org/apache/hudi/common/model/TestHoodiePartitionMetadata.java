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

package org.apache.hudi.common.model;

import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestLogAppender;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.RetryHelper;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

/**
 * Tests {@link HoodiePartitionMetadata}.
 */
public class TestHoodiePartitionMetadata extends HoodieCommonTestHarness {

  HoodieStorage storage;

  @BeforeEach
  public void setupTest() throws IOException {
    initMetaClient();
    storage = metaClient.getStorage();
  }

  @AfterEach
  public void tearDown() throws Exception {
    storage.close();
    cleanMetaClient();
  }

  static Stream<Arguments> formatProviderFn() {
    return Stream.of(
        Arguments.arguments(Option.empty()),
        Arguments.arguments(Option.of(HoodieFileFormat.PARQUET)),
        Arguments.arguments(Option.of(HoodieFileFormat.ORC))
    );
  }

  @ParameterizedTest
  @MethodSource("formatProviderFn")
  public void testTextFormatMetaFile(Option<HoodieFileFormat> format) throws IOException {
    // given
    final StoragePath partitionPath = new StoragePath(basePath, "a/b/"
        + format.map(Enum::name).orElse("text"));
    storage.createDirectory(partitionPath);
    final String commitTime = "000000000001";
    HoodiePartitionMetadata writtenMetadata = new HoodiePartitionMetadata(
        metaClient.getStorage(), commitTime, new StoragePath(basePath), partitionPath,
        format);
    writtenMetadata.trySave();

    // when
    HoodiePartitionMetadata readMetadata = new HoodiePartitionMetadata(
        metaClient.getStorage(), partitionPath);

    // then
    assertTrue(HoodiePartitionMetadata.hasPartitionMetadata(storage, partitionPath));
    assertEquals(Option.of(commitTime), readMetadata.readPartitionCreatedCommitTime());
    assertEquals(3, readMetadata.getPartitionDepth());
  }

  /**
   * Storage that creates the metafile directly, without the temp-file-then-rename dance that
   * local and HDFS storage use. That is what object stores do, and it is the case where the loser
   * of a creation race sees an 'already exists' failure surface out of the storage layer.
   */
  private HoodieStorage directCreateStorage() {
    HoodieStorage objectStore = spy(storage);
    doReturn("s3a").when(objectStore).getScheme();
    return objectStore;
  }

  @Test
  public void testTrySaveWhenMetafileConcurrentlyCreated() throws IOException {
    final StoragePath partitionPath = new StoragePath(basePath, "a/b/raced");
    storage.createDirectory(partitionPath);
    final StoragePath metaPath = new StoragePath(
        partitionPath, HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX);

    // a peer task has already won the race and written the metafile
    new HoodiePartitionMetadata(storage, "000000000001", new StoragePath(basePath), partitionPath, Option.empty()).trySave();
    assertTrue(storage.exists(metaPath));

    HoodieStorage racingStorage = directCreateStorage();
    // the existence check slips through the race window once, then observes the peer's file
    doReturn(false).doCallRealMethod().when(racingStorage).exists(metaPath);

    // watch what the retry helper logs while the race is being lost
    HoodieTestLogAppender appender = new HoodieTestLogAppender().attachTo(RetryHelper.class);

    long elapsedMs;
    try {
      long startMs = System.currentTimeMillis();
      assertDoesNotThrow(() -> new HoodiePartitionMetadata(
          racingStorage, "000000000002", new StoragePath(basePath), partitionPath, Option.empty()).trySave());
      elapsedMs = System.currentTimeMillis() - startMs;
    } finally {
      appender.detach();
    }

    // the symptom from HUDI-9095: losing the race used to be reported as a warning with a full
    // stack trace, even though the file the caller asked for is right there
    List<String> problems = appender.getCapturedEvents().stream()
        .filter(event -> event.getLevel().isMoreSpecificThan(Level.WARN))
        .map(event -> event.getMessage().getFormattedMessage())
        .collect(Collectors.toList());
    assertTrue(problems.isEmpty(),
        "losing the metafile creation race must not be logged as a problem, got: " + problems);

    // Losing the race is a success, so it must not go through the retry helper: every retry sleeps
    // for at least the initial 1s interval.
    assertTrue(elapsedMs < 1000,
        "trySave should not retry when the metafile already exists, but it took " + elapsedMs + " ms");
    assertTrue(HoodiePartitionMetadata.hasPartitionMetadata(storage, partitionPath));
  }

  @Test
  public void testTrySaveKeepsWriteFailureWhenExistenceRecheckFails() throws IOException {
    final StoragePath partitionPath = new StoragePath(basePath, "a/b/recheck-fails");
    storage.createDirectory(partitionPath);
    final StoragePath metaPath = new StoragePath(
        partitionPath, HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX);

    // a peer has the metafile, so the write below fails the way a lost race does
    new HoodiePartitionMetadata(storage, "000000000001", new StoragePath(basePath), partitionPath, Option.empty()).trySave();

    IOException recheckFailure = new IOException("storage unavailable");
    HoodieStorage flakyStorage = directCreateStorage();
    // Each attempt checks twice: once before writing, once after the write fails. Slip through the
    // first so the write is reached, then fail the second. The write failure is retryable, so this
    // has to hold for every attempt rather than only the first.
    AtomicInteger checks = new AtomicInteger();
    doAnswer(invocation -> {
      if (checks.getAndIncrement() % 2 == 0) {
        return false;
      }
      throw recheckFailure;
    }).when(flakyStorage).exists(metaPath);

    // The re-check only exists to recognise a lost race. When it cannot answer, the failure the
    // caller gets has to remain the write failure: that is what RetryHelper classifies as
    // retryable, whereas a bare IOException from the check is not in its retry list.
    Exception thrown = assertThrows(Exception.class, () -> new HoodiePartitionMetadata(
        flakyStorage, "000000000002", new StoragePath(basePath), partitionPath, Option.empty()).trySave());

    assertNotSame(recheckFailure, thrown, "the check failure must not replace the write failure");
    assertTrue(Arrays.stream(thrown.getSuppressed()).anyMatch(s -> s == recheckFailure),
        "the check failure must be kept as suppressed rather than dropped, got: "
            + Arrays.toString(thrown.getSuppressed()));
  }

  @Test
  public void testConcurrentTrySaveWritesOneMetafile() throws Exception {
    final StoragePath partitionPath = new StoragePath(basePath, "a/b/parallel");
    storage.createDirectory(partitionPath);
    final int writers = 8;

    ExecutorService pool = Executors.newFixedThreadPool(writers);
    try {
      CountDownLatch startLine = new CountDownLatch(1);
      List<Future<?>> futures = new ArrayList<>();
      for (int i = 0; i < writers; i++) {
        final String commitTime = String.format("%012d", i + 1);
        futures.add(pool.submit(() -> {
          startLine.await();
          new HoodiePartitionMetadata(directCreateStorage(), commitTime,
              new StoragePath(basePath), partitionPath, Option.empty()).trySave();
          return null;
        }));
      }
      startLine.countDown();
      for (Future<?> future : futures) {
        // any writer failing the race would surface here
        future.get(60, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
    }

    assertTrue(HoodiePartitionMetadata.hasPartitionMetadata(storage, partitionPath));
    assertEquals(1, storage.listDirectEntries(partitionPath).stream()
        .filter(e -> e.getPath().getName().startsWith(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX))
        .count(), "exactly one partition metafile should be left behind");
  }

  @Test
  public void testErrorIfAbsent() throws IOException {
    final StoragePath partitionPath = new StoragePath(basePath, "a/b/not-a-partition");
    storage.createDirectory(partitionPath);
    HoodiePartitionMetadata readMetadata = new HoodiePartitionMetadata(
        metaClient.getStorage(), partitionPath);
    assertThrows(HoodieException.class, readMetadata::readPartitionCreatedCommitTime);
  }

  @Test
  public void testFileNames() {
    assertEquals(new StoragePath("/a/b/c/.hoodie_partition_metadata"),
        HoodiePartitionMetadata.textFormatMetaFilePath(new StoragePath("/a/b/c")));
    assertEquals(Arrays.asList(new StoragePath("/a/b/c/.hoodie_partition_metadata.parquet"),
            new StoragePath("/a/b/c/.hoodie_partition_metadata.orc")),
        HoodiePartitionMetadata.baseFormatMetaFilePaths(new StoragePath("/a/b/c")));
  }
}
