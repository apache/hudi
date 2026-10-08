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

import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineServiceClient;
import org.apache.hudi.common.table.timeline.dto.BaseFileDTO;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.PriorityBasedFileSystemView;
import org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieInstant.State.COMPLETED;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.INFLIGHT;
import static org.apache.hudi.common.table.timeline.HoodieInstant.State.REQUESTED;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLEAN_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.INVALID_INSTANT_TS;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.LOG_COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.TimelineServiceClientBase.RequestMethod.GET;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.BASEPATH_PARAM;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.LAST_INSTANT_TS;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.LATEST_PARTITION_DATA_FILES_URL;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.PARTITION_PARAM;
import static org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView.TIMELINE_HASH;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.TIMELINE_FACTORY;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestTimelineViewRefresh extends HoodieCommonTestHarness {
  private TimelineService server;
  private TimelineServiceClient client;
  private FileSystemViewManager manager;
  private SyncableFileSystemView view;

  @BeforeEach
  void setUp() throws IOException {
    metaClient = HoodieTestUtils.init(tempDir.toAbsolutePath().toString());
    basePath = metaClient.getBasePath().toString();
    manager = mock(FileSystemViewManager.class);
    view = mock(SyncableFileSystemView.class);
    when(manager.getFileSystemView(basePath)).thenAnswer(invocation -> view);
    when(view.getLatestBaseFiles(anyString())).thenAnswer(invocation -> Stream.empty());
    server = new TimelineService(metaClient.getStorageConf(), TimelineService.Config.builder().serverPort(0).build(), manager);
    server.startService();
    client = new TimelineServiceClient(FileSystemViewStorageConfig.newBuilder()
        .withRemoteServerHost("localhost").withRemoteServerPort(server.getServerPort()).build());
  }

  @AfterEach
  void tearDown() {
    server.close();
    view.close();
  }

  @Test
  void testConcurrentRequestsWithTrailingCleanDoNotSync() throws Exception {
    HoodieTimeline clientTimeline = timeline(instant(COMPLETED, COMMIT_ACTION, "001"));
    when(view.getTimeline()).thenReturn(timeline(instant(COMPLETED, COMMIT_ACTION, "001"), instant(COMPLETED, CLEAN_ACTION, "002")));
    ExecutorService executor = Executors.newFixedThreadPool(8);
    try {
      List<Future<List<String>>> requests = new ArrayList<>();
      for (int i = 0; i < 32; i++) {
        requests.add(executor.submit(() -> request(clientTimeline, "001", "partition")));
      }
      for (Future<List<String>> request : requests) {
        assertEquals(0, request.get(60, TimeUnit.SECONDS).size());
      }
    } finally {
      executor.shutdownNow();
    }
    verify(view, never()).sync();
  }

  @ParameterizedTest
  @ValueSource(strings = {COMMIT_ACTION, REPLACE_COMMIT_ACTION, COMPACTION_ACTION, LOG_COMPACTION_ACTION})
  void testOtherExtensionsStillSyncAndFailFinalCheck(String action) {
    HoodieTimeline clientTimeline = timeline(instant(COMPLETED, COMMIT_ACTION, "001"));
    when(view.getTimeline()).thenReturn(timeline(instant(COMPLETED, COMMIT_ACTION, "001"), instant(COMPLETED, action, "002")));
    assertThrows(IOException.class, () -> request(clientTimeline, "001", "partition"));
    verify(view).sync();
  }

  @Test
  void testAllActionsAtClientBoundaryAreCompared() throws IOException {
    HoodieInstant commit = instant(COMPLETED, COMMIT_ACTION, "001");
    HoodieInstant clean = instant(COMPLETED, CLEAN_ACTION, "001");
    HoodieTimeline clientTimeline = timeline(commit, clean);
    when(view.getTimeline()).thenReturn(timeline(commit, clean, instant(COMPLETED, CLEAN_ACTION, "002")));
    request(clientTimeline, "001", "partition");
    verify(view, never()).sync();
    when(view.getTimeline()).thenReturn(timeline(commit, instant(COMPLETED, CLEAN_ACTION, "002")));
    doAnswer(invocation -> {
      when(view.getTimeline()).thenReturn(clientTimeline);
      return null;
    }).when(view).sync();
    request(clientTimeline, "001", "partition");
    verify(view).sync();
  }

  @Test
  void testExtensionDuringRequestFailsFinalCheck() {
    HoodieTimeline clientTimeline = timeline(instant(COMPLETED, COMMIT_ACTION, "001"));
    when(view.getTimeline()).thenReturn(clientTimeline);
    when(view.getLatestBaseFiles(anyString())).thenAnswer(invocation -> {
      when(view.getTimeline()).thenReturn(timeline(instant(COMPLETED, COMMIT_ACTION, "001"), instant(COMPLETED, COMMIT_ACTION, "002")));
      return Stream.empty();
    });
    assertThrows(IOException.class, () -> request(clientTimeline, "001", "partition"));
    verify(view, never()).sync();
  }

  @Test
  void testDivergentPrefixesStillSync() throws IOException {
    HoodieInstant first = instant(COMPLETED, COMMIT_ACTION, "001");
    HoodieInstant boundary = instant(COMPLETED, CLEAN_ACTION, "003");
    HoodieInstant newer = instant(COMPLETED, CLEAN_ACTION, "004");
    HoodieTimeline clientTimeline = timeline(first, boundary);
    List<HoodieTimeline> divergent = Arrays.asList(
        timeline(first), // server behind
        timeline(boundary, newer), // archived first instant
        timeline(first, instant(COMPLETED, COMMIT_ACTION, "002"), boundary, newer), // out-of-order completion
        timeline(instant(COMPLETED, REPLACE_COMMIT_ACTION, "001"), boundary, newer), // action changed
        timeline(instant(INFLIGHT, COMPACTION_ACTION, "001"), boundary, newer));
    for (HoodieTimeline serverTimeline : divergent) {
      when(view.getTimeline()).thenReturn(serverTimeline);
      doAnswer(invocation -> {
        when(view.getTimeline()).thenReturn(clientTimeline);
        return null;
      }).when(view).sync();
      request(clientTimeline, "003", "partition");
    }
    verify(view, times(divergent.size())).sync();
  }

  @Test
  void testCompactionStateAndFiltering() throws IOException {
    HoodieInstant commit = instant(COMPLETED, COMMIT_ACTION, "001");
    HoodieInstant clean = instant(COMPLETED, CLEAN_ACTION, "003");
    for (String action : Arrays.asList(COMPACTION_ACTION, LOG_COMPACTION_ACTION)) {
      HoodieTimeline clientTimeline = timeline(commit, instant(REQUESTED, action, "002"), clean);
      when(view.getTimeline()).thenReturn(timeline(commit, instant(INFLIGHT, action, "002"), clean, instant(COMPLETED, COMMIT_ACTION, "004")));
      doAnswer(invocation -> {
        when(view.getTimeline()).thenReturn(clientTimeline);
        return null;
      }).when(view).sync();
      request(clientTimeline, "003", "partition");
    }
    verify(view, times(2)).sync();
    clearInvocations(view);
    HoodieTimeline rawClient = timeline(commit, instant(INFLIGHT, COMMIT_ACTION, "002"), clean);
    HoodieTimeline filteredClient = rawClient.filterCompletedAndCompactionInstants();
    when(view.getTimeline()).thenReturn(timeline(commit, instant(REQUESTED, CLEAN_ACTION, "002"), clean, instant(COMPLETED, COMMIT_ACTION, "004")));
    doAnswer(invocation -> {
      when(view.getTimeline()).thenReturn(filteredClient);
      return null;
    }).when(view).sync();
    request(filteredClient, "003", "partition");
    verify(view).sync();
    clearInvocations(view);

    // A pending log compaction is included by the server's existing filter but not the remote client's.
    // It must not be silently dropped from the prefix to manufacture a match.
    when(view.getTimeline()).thenReturn(timeline(commit, instant(REQUESTED, LOG_COMPACTION_ACTION, "002"), clean));
    doAnswer(invocation -> {
      when(view.getTimeline()).thenReturn(filteredClient);
      return null;
    }).when(view).sync();
    request(filteredClient, "003", "partition");
    verify(view).sync();
  }

  @Test
  void testInvalidOrMissingBoundaryDoesNotProveExtension() {
    HoodieTimeline clientTimeline = timeline(instant(COMPLETED, COMMIT_ACTION, "001"));
    when(view.getTimeline()).thenReturn(timeline(instant(COMPLETED, COMMIT_ACTION, "001"), instant(COMPLETED, COMMIT_ACTION, "003")));
    for (String boundary : Arrays.asList(null, "", INVALID_INSTANT_TS, "000", "002", "004", "invalid")) {
      assertThrows(IOException.class, () -> request(clientTimeline, boundary, "partition"));
    }
    verify(view, times(7)).sync();
  }

  @Test
  void testEmptyAndEqualTimelines() throws IOException {
    when(view.getTimeline()).thenReturn(timeline());
    request(timeline(), null, "partition");
    HoodieTimeline same = timeline(instant(COMPLETED, COMMIT_ACTION, "001"));
    when(view.getTimeline()).thenReturn(same);
    request(same, "001", "partition");
    verify(view, never()).sync();
    assertThrows(IOException.class, () -> request(same, "002", "partition"));
    verify(view).sync();
  }

  @Test
  void testDivergenceDuringRequestIsRejected() {
    HoodieTimeline clientTimeline = timeline(instant(COMPLETED, COMMIT_ACTION, "001"));
    when(view.getTimeline()).thenReturn(clientTimeline);
    when(view.getLatestBaseFiles(anyString())).thenAnswer(invocation -> {
      when(view.getTimeline()).thenReturn(timeline(instant(COMPLETED, REPLACE_COMMIT_ACTION, "001"), instant(COMPLETED, COMMIT_ACTION, "002")));
      return Stream.empty();
    });
    assertThrows(IOException.class, () -> request(clientTimeline, "001", "partition"));
    verify(view, never()).sync();
  }

  @Test
  void testRealViewRefreshesBeforeReturningNewBaseFile() throws Exception {
    FileCreateUtils.createCommit(metaClient, "001");
    FileCreateUtils.createBaseFile(metaClient, "partition", "001", "file");
    HoodieTimeline initial = metaClient.reloadActiveTimeline();
    view = spy(HoodieTableFileSystemView.fileListingBasedFileSystemView(new HoodieLocalEngineContext(metaClient.getStorageConf()), metaClient, initial));
    assertEquals(Arrays.asList("001"), request(initial, "001", "partition"));
    FileCreateUtils.createCommit(metaClient, "002");
    FileCreateUtils.createBaseFile(metaClient, "partition", "002", "file");
    HoodieTimeline updated = metaClient.reloadActiveTimeline();
    assertEquals(Arrays.asList("002"), request(updated, "002", "partition"));
    verify(view).sync();
  }

  @Test
  void testRealViewRefreshesAfterCommitIsDeleted() throws Exception {
    FileCreateUtils.createCommit(metaClient, "001");
    FileCreateUtils.createBaseFile(metaClient, "partition", "001", "file");
    FileCreateUtils.createCommit(metaClient, "002");
    FileCreateUtils.createBaseFile(metaClient, "partition", "002", "file");
    HoodieTimeline initial = metaClient.reloadActiveTimeline();
    view = spy(HoodieTableFileSystemView.fileListingBasedFileSystemView(new HoodieLocalEngineContext(metaClient.getStorageConf()), metaClient, initial));
    assertEquals(Arrays.asList("002"), request(initial, "002", "partition"));

    // Restore can delete completed instants without publishing each rollback. The server's
    // extra commit is now stale, even though the client's timeline is an exact prefix.
    FileCreateUtils.deleteCommit(metaClient, "002");
    HoodieTimeline restored = metaClient.reloadActiveTimeline();
    assertEquals(Arrays.asList("001"), request(restored, "001", "partition"));
    verify(view).sync();
  }

  @Test
  void testExactExtensionPreservesLocalFallback() throws Exception {
    FileCreateUtils.createCommit(metaClient, "001");
    FileCreateUtils.createBaseFile(metaClient, "partition", "001", "file");
    HoodieTimeline initial = metaClient.reloadActiveTimeline();
    HoodieLocalEngineContext context = new HoodieLocalEngineContext(metaClient.getStorageConf());
    RemoteHoodieTableFileSystemView remoteView = new RemoteHoodieTableFileSystemView("localhost", server.getServerPort(), metaClient);
    HoodieTableFileSystemView secondary = HoodieTableFileSystemView.fileListingBasedFileSystemView(context, metaClient, initial);
    FileCreateUtils.createCommit(metaClient, "002");
    FileCreateUtils.createBaseFile(metaClient, "partition", "002", "file");
    view = spy(HoodieTableFileSystemView.fileListingBasedFileSystemView(context, metaClient, metaClient.reloadActiveTimeline()));
    PriorityBasedFileSystemView priorityView = new PriorityBasedFileSystemView(remoteView, ignored -> secondary, context);
    try {
      assertEquals("001", secondary.getLatestBaseFile("partition", "file").get().getCommitTime());
      assertEquals("001", priorityView.getLatestBaseFile("partition", "file").get().getCommitTime());
      assertEquals(Arrays.asList("001"), priorityView.getLatestBaseFiles("partition")
          .map(HoodieBaseFile::getCommitTime).collect(Collectors.toList()));
      assertEquals("001", priorityView.getLatestFileSlice("partition", "file").get().getBaseInstantTime());
      assertEquals(Arrays.asList("001"), priorityView.getLatestFileSlices("partition")
          .map(slice -> slice.getBaseInstantTime()).collect(Collectors.toList()));
      assertEquals(Arrays.asList("001"), priorityView.getLatestBaseFilesBeforeOrOn("partition", "001")
          .map(HoodieBaseFile::getCommitTime).collect(Collectors.toList()));
      assertEquals(Arrays.asList("001"), priorityView.getLatestFileSlicesBeforeOrOn("partition", "001", false)
          .map(slice -> slice.getBaseInstantTime()).collect(Collectors.toList()));
      verify(view).sync();
    } finally {
      priorityView.close();
      secondary.close();
    }
  }

  @Test
  void testBoundedSelectionPreservesClientPendingCompactionView() throws Exception {
    FileCreateUtils.createCommit(metaClient, "001");
    FileCreateUtils.createBaseFile(metaClient, "partition", "001", "file");
    HoodieTimeline initial = metaClient.reloadActiveTimeline();
    HoodieLocalEngineContext context = new HoodieLocalEngineContext(metaClient.getStorageConf());
    RemoteHoodieTableFileSystemView remoteView = new RemoteHoodieTableFileSystemView("localhost", server.getServerPort(), metaClient);
    HoodieTableFileSystemView secondary = HoodieTableFileSystemView.fileListingBasedFileSystemView(context, metaClient, initial);
    metaClient.getActiveTimeline().saveToCompactionRequested(instant(REQUESTED, COMPACTION_ACTION, "002"),
        CompactionUtils.buildFromFileSlices(Arrays.asList(Pair.of("partition", secondary.getLatestFileSlice("partition", "file").get())),
            Option.empty(), Option.empty()));
    view = spy(HoodieTableFileSystemView.fileListingBasedFileSystemView(context, metaClient, metaClient.reloadActiveTimeline()));
    PriorityBasedFileSystemView priorityView = new PriorityBasedFileSystemView(remoteView, ignored -> secondary, context);
    try {
      assertEquals(1, secondary.getLatestFileSlicesBeforeOrOn("partition", "001", false).count());
      assertEquals(1, priorityView.getLatestFileSlicesBeforeOrOn("partition", "001", false).count());
      assertEquals(Arrays.asList("001"), priorityView.getLatestFileSlicesBeforeOrOn("partition", "001", true)
          .map(slice -> slice.getBaseInstantTime()).collect(Collectors.toList()));
      verify(view).sync();
    } finally {
      priorityView.close();
      secondary.close();
    }
  }

  @Test
  void testDivergentTimelineRetainsStickyFallback() throws Exception {
    FileCreateUtils.createCommit(metaClient, "001");
    FileCreateUtils.createBaseFile(metaClient, "partition", "001", "file");
    HoodieTimeline initial = metaClient.reloadActiveTimeline();
    HoodieLocalEngineContext context = new HoodieLocalEngineContext(metaClient.getStorageConf());
    RemoteHoodieTableFileSystemView remoteView = new RemoteHoodieTableFileSystemView("localhost", server.getServerPort(), metaClient);
    HoodieTableFileSystemView secondary = spy(HoodieTableFileSystemView.fileListingBasedFileSystemView(context, metaClient, initial));
    when(view.getTimeline()).thenReturn(timeline(instant(COMPLETED, REPLACE_COMMIT_ACTION, "001"), instant(COMPLETED, COMMIT_ACTION, "002")));
    PriorityBasedFileSystemView priorityView = new PriorityBasedFileSystemView(remoteView, ignored -> secondary, context);
    try {
      assertEquals(Arrays.asList("001"), priorityView.getLatestBaseFiles("partition")
          .map(HoodieBaseFile::getCommitTime).collect(Collectors.toList()));
      // Even if the server subsequently matches, the previous rejection keeps routing locally.
      when(view.getTimeline()).thenReturn(initial);
      assertEquals(Arrays.asList("001"), priorityView.getLatestBaseFiles("partition")
          .map(HoodieBaseFile::getCommitTime).collect(Collectors.toList()));
      verify(view).sync();
      verify(view).getLatestBaseFiles("partition");
      verify(secondary, times(2)).getLatestBaseFiles("partition");
    } finally {
      priorityView.close();
      secondary.close();
    }
  }

  @Test
  void testRealViewAvoidsRedundantReloadsAndListings() throws Exception {
    FileCreateUtils.createCommit(metaClient, "001");
    for (int i = 0; i < 8; i++) {
      FileCreateUtils.createBaseFile(metaClient, "partition-" + i, "001", "file");
    }
    metaClient.reloadActiveTimeline();
    RemoteHoodieTableFileSystemView remoteView = new RemoteHoodieTableFileSystemView("localhost", server.getServerPort(), metaClient);
    FileCreateUtils.createCleanFile(metaClient, "002", Option.empty(), null, true);
    HoodieTimeline serverTimeline = metaClient.reloadActiveTimeline();
    metaClient = spy(metaClient);
    HoodieStorage storage = spy(metaClient.getStorage());
    when(metaClient.getStorage()).thenReturn(storage);
    view = spy(HoodieTableFileSystemView.fileListingBasedFileSystemView(new HoodieLocalEngineContext(metaClient.getStorageConf()), metaClient, serverTimeline));
    for (int i = 0; i < 8; i++) {
      view.getLatestBaseFiles("partition-" + i).count();
    }
    clearInvocations(view, metaClient, storage);
    try {
      for (int i = 0; i < 32; i++) {
        assertEquals(Arrays.asList("001"), remoteView.getLatestFileSlices("partition-" + i % 8)
            .map(slice -> slice.getBaseFile().get().getCommitTime()).collect(Collectors.toList()));
      }
    } finally {
      remoteView.close();
    }
    assertAll(
        () -> verify(view, never()).sync(),
        () -> verify(metaClient, never()).reloadActiveTimeline(),
        () -> verify(storage, never()).listDirectEntries(any(StoragePath.class), any()));
  }

  @Test
  void testRealViewRefreshesOutOfOrderCompletion() throws Exception {
    FileCreateUtils.createCommit(metaClient, "001");
    FileCreateUtils.createBaseFile(metaClient, "partition", "001", "file");
    FileCreateUtils.createInflightCommit(metaClient, "002");
    FileCreateUtils.createCleanFile(metaClient, "003", Option.empty(), null, true);
    HoodieTimeline initial = metaClient.reloadActiveTimeline();
    view = spy(HoodieTableFileSystemView.fileListingBasedFileSystemView(new HoodieLocalEngineContext(metaClient.getStorageConf()), metaClient, initial));
    assertEquals(Arrays.asList("001"), request(initial.filterCompletedAndCompactionInstants(), "003", "partition"));
    FileCreateUtils.createCommit(metaClient, "002");
    FileCreateUtils.createBaseFile(metaClient, "partition", "002", "file");
    HoodieTimeline updated = metaClient.reloadActiveTimeline().filterCompletedAndCompactionInstants();
    assertEquals(Arrays.asList("002"), request(updated, "003", "partition"));
    verify(view).sync();
  }

  @Test
  void testStaleClientsShareASingleSync() throws Exception {
    // A concurrent writer leaves many executors pinned to an older timeline. Re-reading storage
    // cannot bring the server back to their hash, so only the first request should pay for a reload.
    HoodieTimeline staleClientTimeline = timeline(instant(COMPLETED, COMMIT_ACTION, "001"));
    HoodieTimeline serverTimeline =
        timeline(instant(COMPLETED, COMMIT_ACTION, "001"), instant(COMPLETED, COMMIT_ACTION, "002"));
    when(view.getTimeline()).thenReturn(serverTimeline);

    assertThrows(IOException.class, () -> request(staleClientTimeline, "001", "partition"));
    verify(view).sync();
    clearInvocations(view);

    ExecutorService executor = Executors.newFixedThreadPool(8);
    try {
      List<Future<?>> requests = new ArrayList<>();
      for (int i = 0; i < 32; i++) {
        requests.add(executor.submit(() -> assertThrows(IOException.class, () -> request(staleClientTimeline, "001", "partition"))));
      }
      for (Future<?> request : requests) {
        request.get(60, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
    }
    // The server timeline never changed, so repeating the reload could not have changed the outcome.
    verify(view, never()).sync();
  }

  @Test
  void testServerTimelineChangeStillSyncsForAKnownClient() throws Exception {
    HoodieTimeline staleClientTimeline = timeline(instant(COMPLETED, COMMIT_ACTION, "001"));
    when(view.getTimeline()).thenReturn(
        timeline(instant(COMPLETED, COMMIT_ACTION, "001"), instant(COMPLETED, COMMIT_ACTION, "002")));
    assertThrows(IOException.class, () -> request(staleClientTimeline, "001", "partition"));
    verify(view).sync();
    clearInvocations(view);

    assertThrows(IOException.class, () -> request(staleClientTimeline, "001", "partition"));
    verify(view, never()).sync();

    // The client now advances past what the server has, which the earlier record must not suppress.
    HoodieTimeline aheadClientTimeline = timeline(instant(COMPLETED, COMMIT_ACTION, "001"),
        instant(COMPLETED, COMMIT_ACTION, "002"), instant(COMPLETED, COMMIT_ACTION, "003"));
    assertThrows(IOException.class, () -> request(aheadClientTimeline, "003", "partition"));
    verify(view).sync();
  }

  @Test
  void testDeletedInstantsDropRecordedSyncs() throws Exception {
    HoodieTimeline staleClientTimeline = timeline(instant(COMPLETED, COMMIT_ACTION, "001"));
    HoodieTimeline serverTimeline =
        timeline(instant(COMPLETED, COMMIT_ACTION, "001"), instant(COMPLETED, COMMIT_ACTION, "002"));
    when(view.getTimeline()).thenReturn(serverTimeline);
    assertThrows(IOException.class, () -> request(staleClientTimeline, "001", "partition"));
    verify(view).sync();
    clearInvocations(view);

    // Restore deletes completed instants from storage, moving the server timeline backwards while
    // still differing from the client. The recorded reload described a timeline that no longer
    // exists, so it must not suppress the reload that recovers the server.
    when(view.getTimeline()).thenReturn(
        timeline(instant(COMPLETED, COMMIT_ACTION, "001"), instant(COMPLETED, COMMIT_ACTION, "003")));
    assertThrows(IOException.class, () -> request(staleClientTimeline, "001", "partition"));
    verify(view).sync();
  }

  @Test
  void testRecordedSyncsAreBounded() throws Exception {
    // Distinct client timelines must not accumulate without bound; the oldest records are dropped.
    HoodieTimeline serverTimeline = timeline(instant(COMPLETED, CLEAN_ACTION, "900"));
    when(view.getTimeline()).thenReturn(serverTimeline);
    List<HoodieTimeline> clientTimelines = new ArrayList<>();
    for (int i = 0; i < 12; i++) {
      clientTimelines.add(timeline(instant(COMPLETED, COMMIT_ACTION, String.format("%03d", i))));
    }
    for (HoodieTimeline clientTimeline : clientTimelines) {
      assertThrows(IOException.class, () -> request(clientTimeline, "900", "partition"));
    }
    verify(view, times(clientTimelines.size())).sync();
    clearInvocations(view);

    // The most recent entry is still remembered, the oldest has been evicted.
    assertThrows(IOException.class, () -> request(clientTimelines.get(11), "900", "partition"));
    verify(view, never()).sync();
    assertThrows(IOException.class, () -> request(clientTimelines.get(0), "900", "partition"));
    verify(view).sync();
  }

  private List<String> request(HoodieTimeline timeline, String boundary, String partition) throws IOException {
    Map<String, String> params = new HashMap<>();
    params.put(BASEPATH_PARAM, basePath);
    params.put(PARTITION_PARAM, partition);
    params.put(TIMELINE_HASH, timeline.getTimelineHash());
    if (boundary != null) {
      params.put(LAST_INSTANT_TS, boundary);
    }
    List<BaseFileDTO> result = client.makeRequest(TimelineServiceClient.Request.newBuilder(GET, LATEST_PARTITION_DATA_FILES_URL)
        .addQueryParams(params).build()).getDecodedContent(new TypeReference<List<BaseFileDTO>>() {});
    return result.stream().map(BaseFileDTO::toHoodieBaseFile).map(HoodieBaseFile::getCommitTime).collect(Collectors.toList());
  }

  private HoodieTimeline timeline(HoodieInstant... instants) {
    return TIMELINE_FACTORY.createDefaultTimeline(Arrays.stream(instants), metaClient.getActiveTimeline());
  }

  private HoodieInstant instant(HoodieInstant.State state, String action, String timestamp) {
    return INSTANT_GENERATOR.createNewInstant(state, action, timestamp);
  }
}
