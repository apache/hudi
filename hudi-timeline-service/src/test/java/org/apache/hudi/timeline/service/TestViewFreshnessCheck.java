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

package org.apache.hudi.timeline.service;

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineServiceClient;
import org.apache.hudi.common.table.timeline.dto.BaseFileDTO;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieInstant.State.COMPLETED;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLEAN_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.TimelineServiceClientBase.RequestMethod.GET;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.BASEPATH_PARAM;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.LAST_INSTANT_TS;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.LATEST_PARTITION_DATA_FILES_URL;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.PARTITION_PARAM;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.TIMELINE_HASH;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.TIMELINE_FACTORY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the per-request check of the client's timeline against the server view in {@link RequestHandler}
 * under concurrent requests.
 */
class TestViewFreshnessCheck extends HoodieCommonTestHarness {

  private static final int NUM_THREADS = 16;

  private TimelineService server;
  private TimelineServiceClient client;
  private SyncableFileSystemView view;
  private final AtomicReference<HoodieTimeline> viewTimelineRef = new AtomicReference<>();
  private ExecutorService executor;

  @BeforeEach
  void setUp() throws IOException {
    metaClient = HoodieTestUtils.init(tempDir.toAbsolutePath().toString());
    basePath = metaClient.getBasePath().toString();
    FileSystemViewManager manager = mock(FileSystemViewManager.class);
    view = mock(SyncableFileSystemView.class);
    when(manager.getFileSystemView(basePath)).thenAnswer(invocation -> view);
    when(view.getTimeline()).thenAnswer(invocation -> viewTimelineRef.get());
    when(view.getLatestBaseFiles(anyString())).thenAnswer(invocation -> Stream.empty());
    server = new TimelineService(metaClient.getStorageConf(), TimelineService.Config.builder().serverPort(0).build(), manager);
    server.startService();
    client = new TimelineServiceClient(FileSystemViewStorageConfig.newBuilder()
        .withRemoteServerHost("localhost").withRemoteServerPort(server.getServerPort()).build());
    executor = Executors.newFixedThreadPool(NUM_THREADS + 2);
  }

  @AfterEach
  void tearDown() {
    executor.shutdownNow();
    server.close();
  }

  @Test
  void testTimelineHashComputedOncePerViewTimeline() throws Exception {
    HoodieTimeline viewTimeline = spy(timeline("001", "002"));
    viewTimelineRef.set(viewTimeline);
    runConcurrently(64, () -> request(viewTimeline, "002"));
    verify(viewTimeline, times(1)).filterCompletedOrMajorOrMinorCompactionInstants();

    // A different timeline object (e.g. a recreated view) is filtered again, once, even with equal instants.
    HoodieTimeline recreatedTimeline = spy(timeline("001", "002"));
    viewTimelineRef.set(recreatedTimeline);
    runConcurrently(64, () -> request(recreatedTimeline, "002"));
    verify(recreatedTimeline, times(1)).filterCompletedOrMajorOrMinorCompactionInstants();
    verify(viewTimeline, times(1)).filterCompletedOrMajorOrMinorCompactionInstants();

    // A sync replaces the view timeline, and the synced timeline is filtered once as well.
    HoodieTimeline syncedTimeline = spy(timeline("001", "002", "003"));
    doAnswer(invocation -> {
      viewTimelineRef.set(syncedTimeline);
      return null;
    }).when(view).sync();
    runConcurrently(64, () -> request(syncedTimeline, "003"));
    verify(view, times(1)).sync();
    verify(syncedTimeline, times(1)).filterCompletedOrMajorOrMinorCompactionInstants();
  }

  @Test
  void testUnregisterBasePathDropsCheckedTimeline() throws Exception {
    HoodieTimeline viewTimeline = spy(timeline("001"));
    viewTimelineRef.set(viewTimeline);
    request(viewTimeline, "001");
    request(viewTimeline, "001");
    verify(viewTimeline, times(1)).filterCompletedOrMajorOrMinorCompactionInstants();

    server.unregisterBasePath(basePath);
    request(viewTimeline, "001");
    verify(viewTimeline, times(2)).filterCompletedOrMajorOrMinorCompactionInstants();
  }

  @Test
  void testUpToDateRequestDoesNotWaitForViewMonitor() throws Exception {
    HoodieTimeline viewTimeline = timeline("001");
    viewTimelineRef.set(viewTimeline);
    request(viewTimeline, "001");

    CountDownLatch monitorHeld = new CountDownLatch(1);
    CountDownLatch releaseMonitor = new CountDownLatch(1);
    Future<?> holder = executor.submit(() -> {
      synchronized (view) {
        monitorHeld.countDown();
        releaseMonitor.await();
      }
      return null;
    });
    try {
      assertTrue(monitorHeld.await(30, TimeUnit.SECONDS));
      Future<Integer> upToDate = executor.submit(() -> request(viewTimeline, "001"));
      assertEquals(0, upToDate.get(30, TimeUnit.SECONDS));

      // A client ahead of the server still waits for the monitor and syncs the view.
      HoodieTimeline newerTimeline = timeline("001", "002");
      doAnswer(invocation -> {
        viewTimelineRef.set(newerTimeline);
        return null;
      }).when(view).sync();
      Future<Integer> ahead = executor.submit(() -> request(newerTimeline, "002"));
      awaitThreadsBlockedOnViewMonitor(1, ahead::isDone);
      assertFalse(ahead.isDone());
      verify(view, never()).sync();
      releaseMonitor.countDown();
      assertEquals(0, ahead.get(30, TimeUnit.SECONDS));
      verify(view, times(1)).sync();
    } finally {
      releaseMonitor.countDown();
      holder.get(30, TimeUnit.SECONDS);
    }
  }

  /**
   * A client that has not seen a trailing clean is accepted without a sync, and, like an up-to-date client,
   * without the view monitor or a per-request hash of the view timeline.
   */
  @Test
  void testClientMissingTrailingCleanDoesNotWaitForViewMonitor() throws Exception {
    HoodieTimeline clientTimeline = timeline("001", "002");
    HoodieTimeline viewTimeline = spy(withTrailingClean(clientTimeline, "003"));
    viewTimelineRef.set(viewTimeline);
    runConcurrently(64, () -> request(clientTimeline, "002"));
    verify(viewTimeline, times(1)).findInstantsBefore("003");

    CountDownLatch monitorHeld = new CountDownLatch(1);
    CountDownLatch releaseMonitor = new CountDownLatch(1);
    Future<?> holder = executor.submit(() -> {
      synchronized (view) {
        monitorHeld.countDown();
        releaseMonitor.await();
      }
      return null;
    });
    try {
      assertTrue(monitorHeld.await(30, TimeUnit.SECONDS));
      Future<Integer> missingClean = executor.submit(() -> request(clientTimeline, "002"));
      assertEquals(0, missingClean.get(30, TimeUnit.SECONDS));
    } finally {
      releaseMonitor.countDown();
      holder.get(30, TimeUnit.SECONDS);
    }
    verify(view, never()).sync();
    verify(viewTimeline, times(1)).findInstantsBefore("003");
  }

  /**
   * A request never skips the view monitor using a checked timeline that is not the view's current one, and the
   * checked timeline is dropped before a sync starts. So a request that arrives while a sync runs waits for the
   * monitor, whether the sync has already published its new timeline or is still updating the view under the old
   * one. A request that passed its check before the sync started is not covered here: it behaves as if it had been
   * checked before the sync, as it does without this check.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testNoRequestServedDuringSync(boolean publishTimelineFirst) throws Exception {
    HoodieTimeline oldTimeline = timeline("001");
    HoodieTimeline newTimeline = timeline("001", "002");
    viewTimelineRef.set(oldTimeline);
    request(oldTimeline, "001");

    AtomicBoolean syncing = new AtomicBoolean(false);
    AtomicInteger servedDuringSync = new AtomicInteger();
    CountDownLatch syncStarted = new CountDownLatch(1);
    CountDownLatch finishSync = new CountDownLatch(1);
    AtomicInteger syncCalls = new AtomicInteger();
    doAnswer(invocation -> {
      if (syncCalls.getAndIncrement() == 0) {
        syncing.set(true);
        if (publishTimelineFirst) {
          viewTimelineRef.set(newTimeline);
        }
        syncStarted.countDown();
        assertTrue(finishSync.await(30, TimeUnit.SECONDS));
        viewTimelineRef.set(newTimeline);
        syncing.set(false);
      }
      return null;
    }).when(view).sync();
    when(view.getLatestBaseFiles(anyString())).thenAnswer(invocation -> {
      if (syncing.get()) {
        servedDuringSync.incrementAndGet();
      }
      return Stream.empty();
    });

    Future<Integer> syncTrigger = executor.submit(() -> request(newTimeline, "002"));
    assertTrue(syncStarted.await(30, TimeUnit.SECONDS));
    List<Future<Integer>> upToDate = new ArrayList<>();
    List<Future<Integer>> stale = new ArrayList<>();
    for (int i = 0; i < NUM_THREADS / 2; i++) {
      upToDate.add(executor.submit(() -> request(newTimeline, "002")));
      stale.add(executor.submit(() -> request(oldTimeline, "001")));
    }
    awaitThreadsBlockedOnViewMonitor(NUM_THREADS, () -> servedDuringSync.get() > 0);
    assertEquals(0, servedDuringSync.get());
    finishSync.countDown();

    assertEquals(0, syncTrigger.get(30, TimeUnit.SECONDS));
    for (Future<Integer> future : upToDate) {
      assertEquals(0, future.get(30, TimeUnit.SECONDS));
    }
    for (Future<Integer> future : stale) {
      // The server is now ahead of these clients, so the response is rejected as it is without concurrency.
      ExecutionException e = assertThrows(ExecutionException.class, () -> future.get(30, TimeUnit.SECONDS));
      assertTrue(e.getCause() instanceof IOException, e.toString());
    }
    assertEquals(0, servedDuringSync.get());
    assertFalse(syncing.get());
  }

  /**
   * Waits until the given number of threads are blocked on the view's monitor, or until {@code stopWaiting} holds.
   */
  private void awaitThreadsBlockedOnViewMonitor(int numThreads, BooleanSupplier stopWaiting) throws InterruptedException {
    int viewMonitor = System.identityHashCode(view);
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (!stopWaiting.getAsBoolean()) {
      long blocked = Arrays.stream(ManagementFactory.getThreadMXBean().dumpAllThreads(false, false))
          .filter(info -> info.getThreadState() == Thread.State.BLOCKED)
          .map(ThreadInfo::getLockInfo)
          .filter(lock -> lock != null && lock.getIdentityHashCode() == viewMonitor)
          .count();
      if (blocked >= numThreads) {
        return;
      }
      assertTrue(System.nanoTime() < deadline, "Only " + blocked + " of " + numThreads + " threads blocked on the view monitor");
      Thread.sleep(10);
    }
  }

  private void runConcurrently(int numRequests, Callable<Integer> request) throws Exception {
    List<Future<Integer>> futures = new ArrayList<>();
    for (int i = 0; i < numRequests; i++) {
      futures.add(executor.submit(request));
    }
    for (Future<Integer> future : futures) {
      assertEquals(0, future.get(60, TimeUnit.SECONDS));
    }
  }

  private int request(HoodieTimeline clientTimeline, String lastInstant) throws IOException {
    Map<String, String> params = new HashMap<>();
    params.put(BASEPATH_PARAM, basePath);
    params.put(PARTITION_PARAM, "partition");
    params.put(TIMELINE_HASH, clientTimeline.getTimelineHash());
    params.put(LAST_INSTANT_TS, lastInstant);
    List<BaseFileDTO> result = client.makeRequest(TimelineServiceClient.Request.newBuilder(GET, LATEST_PARTITION_DATA_FILES_URL)
        .addQueryParams(params).build()).getDecodedContent(new TypeReference<List<BaseFileDTO>>() {});
    return result.size();
  }

  private HoodieTimeline timeline(String... completedCommits) {
    return TIMELINE_FACTORY.createDefaultTimeline(Arrays.stream(completedCommits)
        .map(ts -> (HoodieInstant) INSTANT_GENERATOR.createNewInstant(COMPLETED, COMMIT_ACTION, ts)), metaClient.getActiveTimeline());
  }

  private HoodieTimeline withTrailingClean(HoodieTimeline timeline, String cleanTs) {
    return TIMELINE_FACTORY.createDefaultTimeline(Stream.concat(timeline.getInstantsAsStream(),
        Stream.of(INSTANT_GENERATOR.createNewInstant(COMPLETED, CLEAN_ACTION, cleanTs))), metaClient.getActiveTimeline());
  }
}
