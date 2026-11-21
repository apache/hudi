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

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SpillableMapBasedFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.timeline.service.handlers.BaseFileHandler;
import org.apache.hudi.timeline.service.handlers.Handler;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A table refresh that removes and closes the server-side view while a request works with it: the request never
 * reads a closed view and is answered from the view it holds, also when the refresh lands between the handler's view
 * lookup and its read or before the handler traverses its result.
 */
class TestRequestHandlerViewReplacement extends HoodieCommonTestHarness {

  private static final String PARTITION = "p";

  private TimelineService server;
  private FileSystemViewManager viewManager;

  @BeforeEach
  void setUp() throws IOException {
    metaClient = HoodieTestUtils.init(tempDir.toAbsolutePath().toString());
    basePath = metaClient.getBasePath().toString();
    Configuration configuration = new Configuration();
    FileSystemViewStorageConfig viewConfig = FileSystemViewStorageConfig.newBuilder()
        .withStorageType(FileSystemViewStorageType.MEMORY).withIncrementalTimelineSync(true).build();
    viewManager = FileSystemViewManager.createViewManager(new HoodieLocalEngineContext(new HadoopStorageConfiguration(configuration)),
        HoodieMetadataConfig.newBuilder().build(), viewConfig, HoodieCommonConfig.newBuilder().build());
    server = TimelineServiceTestHarness.newBuilder().build(configuration, TimelineService.Config.builder().serverPort(0).build(), viewManager);
    server.startService();
  }

  @AfterEach
  void tearDown() {
    server.close();
  }

  /**
   * A server-side view whose n-th timeline read pauses until the test lets it continue, and which records whether each
   * timeline read is made by its own sync and counts the timeline reads made after it was closed.
   */
  static final class PausingView extends HoodieTableFileSystemView {
    final int pauseAtRead;
    final AtomicInteger reads = new AtomicInteger();
    final List<Boolean> readInSync = Collections.synchronizedList(new ArrayList<>());
    final AtomicInteger readsAfterClose = new AtomicInteger();
    final CountDownLatch paused = new CountDownLatch(1);
    final CountDownLatch resume = new CountDownLatch(1);
    final CountDownLatch closing = new CountDownLatch(1);
    final CountDownLatch closedLatch = new CountDownLatch(1);
    volatile boolean closed;

    PausingView(HoodieTableMetaClient metaClient, int pauseAtRead) {
      super(new FileSystemBackedTableMetadata(new HoodieLocalEngineContext(metaClient.getStorageConf()), metaClient.getTableConfig(),
              metaClient.getStorage(), metaClient.getBasePath().toString()),
          metaClient, metaClient.getActiveTimeline().filterCompletedOrMajorOrMinorCompactionInstants(), true);
      this.pauseAtRead = pauseAtRead;
    }

    @Override
    public HoodieTimeline getTimeline() {
      readInSync.add(Arrays.stream(Thread.currentThread().getStackTrace()).anyMatch(f -> "maySyncIncrementally".equals(f.getMethodName())));
      if (reads.incrementAndGet() == pauseAtRead) {
        paused.countDown();
        try {
          resume.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      if (closed) {
        readsAfterClose.incrementAndGet();
      }
      return super.getTimeline();
    }

    @Override
    public void close() {
      closing.countDown();
      super.close();
      closed = true;
      closedLatch.countDown();
    }
  }

  /**
   * A completed commit whose metadata lists the base file it adds, so that an incremental sync of a view picks it up.
   */
  private void commitBaseFile(String instant) throws Exception {
    HoodieTestTable table = HoodieTestTable.of(metaClient).addInflightCommit(instant);
    String fileName = table.withBaseFilesInPartition(PARTITION, UUID.randomUUID().toString()).getRight().get(0);
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setFileId(FSUtils.getFileId(fileName));
    stat.setPath(PARTITION + "/" + fileName);
    metadata.addWriteStat(PARTITION, stat);
    metaClient.getActiveTimeline().saveAsComplete(
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instant), Option.of(metadata));
  }

  private RemoteHoodieTableFileSystemView remoteView() {
    metaClient.reloadActiveTimeline();
    return new RemoteHoodieTableFileSystemView(metaClient, FileSystemViewStorageConfig.newBuilder()
        .withRemoteServerHost("localhost").withRemoteServerPort(server.getServerPort()).withRemoteTimelineClientTimeoutSecs(60)
        .withRemoteTimelineClientRetry(false).build());
  }

  @SuppressWarnings("unchecked")
  private void installServerView(SyncableFileSystemView view) throws Exception {
    Field field = FileSystemViewManager.class.getDeclaredField("globalViewMap");
    field.setAccessible(true);
    ((Map<String, SyncableFileSystemView>) field.get(viewManager)).put(basePath, view);
  }

  /**
   * A refresh while the request is paused at each of its timeline reads outside the view's own sync: the freshness
   * checks before and after the handler. The positions are taken from a request that is not paused, so the test does
   * not depend on how many reads the freshness checks make.
   */
  @Test
  void refreshDuringARequestClosesNoViewTheRequestReads() throws Exception {
    HoodieTestTable.of(metaClient).addCommit("001").withBaseFilesInPartition(PARTITION, UUID.randomUUID().toString());
    assertEquals(1, remoteView().getLatestBaseFiles(PARTITION).count());
    PausingView probe = new PausingView(HoodieTestUtils.createMetaClient(metaClient.getStorageConf(), basePath), 0);
    assertEquals(1, probe.getLatestBaseFiles(PARTITION).count());
    installServerView(probe);
    commitBaseFile("002");
    assertEquals(2, remoteView().getLatestBaseFiles(PARTITION).count());
    List<Integer> readsOutsideTheSync = new ArrayList<>();
    for (int i = 0; i < probe.readInSync.size(); i++) {
      if (!probe.readInSync.get(i)) {
        readsOutsideTheSync.add(i + 1);
      }
    }
    assertTrue(readsOutsideTheSync.size() >= 2, "the request reads the timeline before and after its handler: " + probe.readInSync);

    long files = 2;
    for (int pauseAtRead : readsOutsideTheSync) {
      files++;
      refreshDuringARequestPausedAtRead(pauseAtRead, String.format("%03d", files), files);
    }
  }

  private void refreshDuringARequestPausedAtRead(int pauseAtRead, String instant, long expectedFiles) throws Exception {
    PausingView serverView = new PausingView(HoodieTestUtils.createMetaClient(metaClient.getStorageConf(), basePath), pauseAtRead);
    assertEquals(expectedFiles - 1, serverView.getLatestBaseFiles(PARTITION).count());
    installServerView(serverView);
    commitBaseFile(instant);
    RemoteHoodieTableFileSystemView client = remoteView();

    AtomicLong served = new AtomicLong(-1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread request = new Thread(() -> {
      try {
        served.set(client.getLatestBaseFiles(PARTITION).count());
      } catch (Throwable t) {
        failure.set(t);
      }
    });
    request.start();
    assertTrue(serverView.paused.await(30, TimeUnit.SECONDS), "the request reached timeline read " + pauseAtRead);
    refreshWithoutWaiting();
    assertEquals(1, serverView.closing.getCount(), "the view stays open while the request holds it, read " + pauseAtRead);
    serverView.resume.countDown();
    request.join(TimeUnit.SECONDS.toMillis(60));
    assertFalse(request.isAlive(), "the request finished");
    assertTrue(serverView.closedLatch.await(30, TimeUnit.SECONDS), "the view was closed when the request released it");

    assertNull(failure.get(), "the request failed: " + failure.get());
    assertEquals(expectedFiles, served.get(), "the request is answered with the current files");
    assertEquals(0, serverView.readsAfterClose.get(), "a request read the timeline of the closed view");
    assertTrue(serverView.closed, "the view the refresh removed was closed");
    assertNotSame(serverView, viewManager.getFileSystemView(basePath));
  }

  /**
   * The server-side view behind a proxy whose base file read pauses once, so that a refresh can close the view between
   * the handler's view lookup and its read.
   */
  private SyncableFileSystemView pausingOnBaseFileRead(SyncableFileSystemView view, CountDownLatch paused, CountDownLatch resume) {
    return (SyncableFileSystemView) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {SyncableFileSystemView.class},
        (proxy, method, args) -> {
          if ("getLatestBaseFiles".equals(method.getName()) && args != null && args.length == 1 && paused.getCount() > 0) {
            paused.countDown();
            resume.await(30, TimeUnit.SECONDS);
          }
          try {
            return method.invoke(view, args);
          } catch (InvocationTargetException e) {
            throw e.getCause();
          }
        });
  }

  @Test
  void refreshBetweenTheHandlersViewLookupAndItsReadLeavesTheViewToTheAnswer() throws Exception {
    HoodieTestTable.of(metaClient).addCommit("001").withBaseFilesInPartition(PARTITION, UUID.randomUUID().toString());
    assertEquals(1, remoteView().getLatestBaseFiles(PARTITION).count());
    PausingView serverView = new PausingView(HoodieTestUtils.createMetaClient(metaClient.getStorageConf(), basePath), 0);
    assertEquals(1, serverView.getLatestBaseFiles(PARTITION).count());
    CountDownLatch paused = new CountDownLatch(1);
    CountDownLatch resume = new CountDownLatch(1);
    installServerView(pausingOnBaseFileRead(serverView, paused, resume));
    RemoteHoodieTableFileSystemView client = remoteView();

    AtomicLong served = new AtomicLong(-1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread request = new Thread(() -> {
      try {
        served.set(client.getLatestBaseFiles(PARTITION).count());
      } catch (Throwable t) {
        failure.set(t);
      }
    });
    request.start();
    assertTrue(paused.await(30, TimeUnit.SECONDS), "the request reached the handler's base file read");
    refreshWithoutWaiting();
    assertEquals(1, serverView.closing.getCount(), "the view stays open while the request holds it");
    resume.countDown();
    request.join(TimeUnit.SECONDS.toMillis(60));
    assertFalse(request.isAlive(), "the request finished");
    assertTrue(serverView.closedLatch.await(30, TimeUnit.SECONDS), "the view was closed when the request released it");

    assertNull(failure.get(), "the request failed: " + failure.get());
    assertEquals(1, served.get(), "the request is answered from the view the handler resolved");
    assertTrue(serverView.closed, "the view was closed once the answer was complete");
    assertNotSame(serverView, viewManager.getFileSystemView(basePath));
  }

  /**
   * The server-side view behind a proxy that records its close and whose all-partitions base file stream pauses
   * once before its first element is read.
   */
  private SyncableFileSystemView pausingBeforeTraversal(SyncableFileSystemView view, CountDownLatch paused, CountDownLatch resume,
                                                        CountDownLatch closing, CountDownLatch closed) {
    return (SyncableFileSystemView) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {SyncableFileSystemView.class},
        (proxy, method, args) -> {
          if ("close".equals(method.getName())) {
            closing.countDown();
          }
          Object result;
          try {
            result = method.invoke(view, args);
          } catch (InvocationTargetException e) {
            throw e.getCause();
          }
          if ("close".equals(method.getName())) {
            closed.countDown();
          }
          if ("getLatestBaseFiles".equals(method.getName()) && args == null) {
            Spliterator<?> source = ((Stream<?>) result).spliterator();
            return StreamSupport.stream(new Spliterator<Object>() {
              @Override
              public boolean tryAdvance(Consumer<? super Object> action) {
                if (paused.getCount() > 0) {
                  paused.countDown();
                  try {
                    resume.await(30, TimeUnit.SECONDS);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                }
                return source.tryAdvance(action);
              }

              @Override
              public Spliterator<Object> trySplit() {
                return null;
              }

              @Override
              public long estimateSize() {
                return source.estimateSize();
              }

              @Override
              public int characteristics() {
                return source.characteristics() & ~Spliterator.SIZED & ~Spliterator.SUBSIZED;
              }
            }, false);
          }
          return result;
        });
  }

  @Test
  void refreshBeforeTheHandlerTraversesItsStreamLeavesTheAnswerComplete() throws Exception {
    commitBaseFile("001");
    commitBaseFile("002");
    assertEquals(2, remoteView().getLatestBaseFiles(PARTITION).count());
    HoodieTableMetaClient serverMetaClient = HoodieTestUtils.createMetaClient(metaClient.getStorageConf(), basePath);
    SpillableMapBasedFileSystemView serverView = new SpillableMapBasedFileSystemView(
        new FileSystemBackedTableMetadata(new HoodieLocalEngineContext(serverMetaClient.getStorageConf()), serverMetaClient.getTableConfig(),
            serverMetaClient.getStorage(), basePath),
        serverMetaClient, serverMetaClient.getActiveTimeline().filterCompletedOrMajorOrMinorCompactionInstants(),
        FileSystemViewStorageConfig.newBuilder().withStorageType(FileSystemViewStorageType.SPILLABLE_DISK).withIncrementalTimelineSync(true)
            .withBaseStoreDir(tempDir.resolve("server-spill").toString()).build(),
        HoodieCommonConfig.newBuilder().build());
    assertEquals(2, serverView.getLatestBaseFiles(PARTITION).count());
    CountDownLatch paused = new CountDownLatch(1);
    CountDownLatch resume = new CountDownLatch(1);
    CountDownLatch closing = new CountDownLatch(1);
    CountDownLatch closed = new CountDownLatch(1);
    installServerView(pausingBeforeTraversal(serverView, paused, resume, closing, closed));
    RemoteHoodieTableFileSystemView client = remoteView();

    AtomicReference<List<String>> served = new AtomicReference<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread request = new Thread(() -> {
      try {
        served.set(client.getLatestBaseFiles().map(HoodieBaseFile::getFileName).sorted().collect(Collectors.toList()));
      } catch (Throwable t) {
        failure.set(t);
      }
    });
    request.start();
    assertTrue(paused.await(30, TimeUnit.SECONDS), "the handler obtained its stream and is about to traverse it");
    refreshWithoutWaiting();
    assertEquals(1, closing.getCount(), "the view stays open while the request holds it");
    resume.countDown();
    request.join(TimeUnit.SECONDS.toMillis(60));
    assertFalse(request.isAlive(), "the request finished");
    assertTrue(closed.await(30, TimeUnit.SECONDS), "the view was closed when the request released it");

    assertNull(failure.get(), "the request failed: " + failure.get());
    List<String> expected = serverMetaClient.getStorage().listDirectEntries(new StoragePath(basePath, PARTITION)).stream()
        .map(e -> e.getPath().getName()).filter(n -> n.endsWith(".parquet")).sorted().collect(Collectors.toList());
    assertEquals(2, expected.size());
    assertEquals(expected, served.get(), "the answer holds every base file of the view the handler read");
    assertEquals(0, closing.getCount(), "the view was closed once the answer was complete");
  }

  /**
   * The manager's view map: the first lookup a handler makes pauses once, and the view created for it pauses once on
   * its first base file read.
   */
  final class PausingViewMap extends ConcurrentHashMap<String, SyncableFileSystemView> {
    final CountDownLatch lookupPaused = new CountDownLatch(1);
    final CountDownLatch lookupResume = new CountDownLatch(1);
    final CountDownLatch readPaused = new CountDownLatch(1);
    final CountDownLatch readResume = new CountDownLatch(1);

    @Override
    public SyncableFileSystemView computeIfAbsent(String key, Function<? super String, ? extends SyncableFileSystemView> creator) {
      if (!calledFromAHandler()) {
        return super.computeIfAbsent(key, creator);
      }
      if (lookupPaused.getCount() > 0) {
        lookupPaused.countDown();
        try {
          lookupResume.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      return super.computeIfAbsent(key, k -> pausingOnBaseFileRead(creator.apply(k), readPaused, readResume));
    }
  }

  private static boolean calledFromAHandler() {
    String handlers = Handler.class.getPackage().getName() + ".";
    return Arrays.stream(Thread.currentThread().getStackTrace()).anyMatch(frame -> frame.getClassName().startsWith(handlers));
  }

  @SuppressWarnings("unchecked")
  private void installViewMap(ConcurrentHashMap<String, SyncableFileSystemView> map) throws Exception {
    Field field = FileSystemViewManager.class.getDeclaredField("globalViewMap");
    field.setAccessible(true);
    map.putAll((Map<String, SyncableFileSystemView>) field.get(viewManager));
    field.set(viewManager, map);
  }

  /**
   * A refresh removes the request's view while a handler resolves the manager for a view of its own, and a second
   * refresh closes the view the handler resolved before its first read: the request must be answered from the view
   * it holds, without a handler resolving the manager.
   */
  @Test
  void handlerReadsTheViewTheRequestHolds() throws Exception {
    HoodieTestTable.of(metaClient).addCommit("001").withBaseFilesInPartition(PARTITION, UUID.randomUUID().toString());
    assertEquals(1, remoteView().getLatestBaseFiles(PARTITION).count());
    commitBaseFile("002");
    RemoteHoodieTableFileSystemView client = remoteView();
    viewManager.clearFileSystemView(basePath);
    AtomicLong served = new AtomicLong(-1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread request = new Thread(() -> {
      try {
        served.set(client.getLatestBaseFiles(PARTITION).count());
      } catch (Throwable t) {
        failure.set(t);
      }
    });
    PausingViewMap map = new PausingViewMap();
    installViewMap(map);
    request.start();

    assertTrue(waitUntil(() -> map.lookupPaused.getCount() == 0 || !request.isAlive()));
    if (map.lookupPaused.getCount() == 0) {
      Thread firstRefresh = new Thread(() -> viewManager.clearFileSystemView(basePath));
      firstRefresh.start();
      assertTrue(waitUntil(() -> !map.containsKey(basePath)), "the first refresh removed the request's view");
      map.lookupResume.countDown();
      if (map.readPaused.await(30, TimeUnit.SECONDS)) {
        viewManager.clearFileSystemView(basePath);
        map.readResume.countDown();
      }
      firstRefresh.join(TimeUnit.SECONDS.toMillis(60));
      assertFalse(firstRefresh.isAlive());
    }
    request.join(TimeUnit.SECONDS.toMillis(60));
    assertFalse(request.isAlive());
    map.lookupResume.countDown();
    map.readResume.countDown();

    assertNull(failure.get(), "the request failed: " + failure.get());
    assertEquals(2, served.get(), "the request is answered from the view it holds");
    assertEquals(1, map.lookupPaused.getCount(), "a handler resolved the manager for a view of its own");
  }

  private static boolean waitUntil(BooleanSupplier condition) throws InterruptedException {
    for (int i = 0; i < 300 && !condition.getAsBoolean(); i++) {
      Thread.sleep(10);
    }
    return condition.getAsBoolean();
  }

  /**
   * The server-side view behind a proxy whose base file read waits, once per request, until the given number of
   * requests read at the same time.
   */
  private SyncableFileSystemView meetingOnBaseFileRead(SyncableFileSystemView view, CyclicBarrier meeting, AtomicInteger met) {
    return (SyncableFileSystemView) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {SyncableFileSystemView.class},
        (proxy, method, args) -> {
          if ("getLatestBaseFiles".equals(method.getName()) && args != null && args.length == 1) {
            try {
              meeting.await(10, TimeUnit.SECONDS);
              met.incrementAndGet();
            } catch (TimeoutException | BrokenBarrierException expected) {
            }
          }
          try {
            return method.invoke(view, args);
          } catch (InvocationTargetException e) {
            throw e.getCause();
          }
        });
  }

  @Test
  void requestsOnOneViewRunTheirHandlersAtTheSameTime() throws Exception {
    HoodieTestTable.of(metaClient).addCommit("001").withBaseFilesInPartition(PARTITION, UUID.randomUUID().toString());
    assertEquals(1, remoteView().getLatestBaseFiles(PARTITION).count());
    PausingView serverView = new PausingView(HoodieTestUtils.createMetaClient(metaClient.getStorageConf(), basePath), 0);
    assertEquals(1, serverView.getLatestBaseFiles(PARTITION).count());
    CyclicBarrier meeting = new CyclicBarrier(2);
    AtomicInteger met = new AtomicInteger();
    installServerView(meetingOnBaseFileRead(serverView, meeting, met));
    List<RemoteHoodieTableFileSystemView> clients = Arrays.asList(remoteView(), remoteView());

    AtomicInteger served = new AtomicInteger();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    List<Thread> requests = clients.stream().map(client -> new Thread(() -> {
      try {
        assertEquals(1, client.getLatestBaseFiles(PARTITION).count());
        served.incrementAndGet();
      } catch (Throwable t) {
        failure.set(t);
      }
    })).collect(Collectors.toList());
    requests.forEach(Thread::start);
    for (Thread request : requests) {
      request.join(TimeUnit.SECONDS.toMillis(60));
      assertFalse(request.isAlive());
    }

    assertNull(failure.get(), "a request failed: " + failure.get());
    assertEquals(2, served.get());
    assertEquals(2, met.get(), "both handlers read the view at the same time");
  }

  /**
   * The server-side view behind a proxy whose first call of the given method pauses until the test lets it continue.
   */
  private SyncableFileSystemView pausingOnFirstCall(SyncableFileSystemView view, String methodName, CountDownLatch paused, CountDownLatch resume) {
    return (SyncableFileSystemView) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {SyncableFileSystemView.class},
        (proxy, method, args) -> {
          if (methodName.equals(method.getName()) && paused.getCount() > 0) {
            paused.countDown();
            resume.await(30, TimeUnit.SECONDS);
          }
          try {
            return method.invoke(view, args);
          } catch (InvocationTargetException e) {
            throw e.getCause();
          }
        });
  }

  private static void callEndpoint(RemoteHoodieTableFileSystemView client, String method) {
    switch (method) {
      case "getLastInstant":
        assertEquals("001", client.getLastInstant().get().requestedTime());
        break;
      case "getTimeline":
        assertEquals(1, client.getTimeline().countInstants());
        break;
      case "loadAllPartitions":
        client.loadAllPartitions();
        break;
      default:
        client.loadPartitions(Collections.singletonList(PARTITION));
        break;
    }
  }

  /**
   * The endpoints that read the view without the refresh check hold it as well: a refresh that lands between the
   * handler's lookup and its read leaves the view open until the answer is complete, and the handler reads no closed
   * view.
   */
  @ParameterizedTest
  @ValueSource(strings = {"getLastInstant", "getTimeline", "loadAllPartitions", "loadPartitions"})
  void endpointsWithoutTheRefreshCheckReadTheViewTheyHold(String method) throws Exception {
    HoodieTestTable.of(metaClient).addCommit("001").withBaseFilesInPartition(PARTITION, UUID.randomUUID().toString());
    PausingView serverView = new PausingView(HoodieTestUtils.createMetaClient(metaClient.getStorageConf(), basePath), 0);
    CountDownLatch paused = new CountDownLatch(1);
    CountDownLatch resume = new CountDownLatch(1);
    installServerView(pausingOnFirstCall(serverView, method, paused, resume));
    RemoteHoodieTableFileSystemView client = remoteView();

    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread request = new Thread(() -> {
      try {
        callEndpoint(client, method);
      } catch (Throwable t) {
        failure.set(t);
      }
    });
    request.start();
    assertTrue(paused.await(30, TimeUnit.SECONDS), "the request reached the handler's read");
    refreshWithoutWaiting();
    assertEquals(1, serverView.closing.getCount(), "the view stays open while the request holds it");
    resume.countDown();
    request.join(TimeUnit.SECONDS.toMillis(60));
    assertFalse(request.isAlive(), "the request finished");
    assertTrue(serverView.closedLatch.await(30, TimeUnit.SECONDS), "the view was closed when the request released it");

    assertNull(failure.get(), "the request failed: " + failure.get());
    assertEquals(0, serverView.readsAfterClose.get(), "the handler read the timeline of the closed view");
    assertTrue(serverView.closed, "the view was closed once the answer was complete");
  }

  /**
   * Refreshes the table on another thread and requires the refresh to return while a request holds the view.
   */
  private void refreshWithoutWaiting() throws InterruptedException {
    Thread refresh = new Thread(() -> viewManager.clearFileSystemView(basePath));
    refresh.start();
    refresh.join(TimeUnit.SECONDS.toMillis(10));
    assertFalse(refresh.isAlive(), "the refresh returned while a request held the view");
  }

  @Test
  void refreshReturnsWhileARequestHoldsTheViewAndLaterRequestsUseTheReplacement() throws Exception {
    HoodieTestTable.of(metaClient).addCommit("001").withBaseFilesInPartition(PARTITION, UUID.randomUUID().toString());
    assertEquals(1, remoteView().getLatestBaseFiles(PARTITION).count());
    PausingView serverView = new PausingView(HoodieTestUtils.createMetaClient(metaClient.getStorageConf(), basePath), 0);
    CountDownLatch paused = new CountDownLatch(1);
    CountDownLatch resume = new CountDownLatch(1);
    installServerView(pausingOnBaseFileRead(serverView, paused, resume));
    RemoteHoodieTableFileSystemView client = remoteView();

    AtomicLong served = new AtomicLong(-1);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread request = new Thread(() -> {
      try {
        served.set(client.getLatestBaseFiles(PARTITION).count());
      } catch (Throwable t) {
        failure.set(t);
      }
    });
    request.start();
    assertTrue(paused.await(30, TimeUnit.SECONDS), "the request reached the handler's base file read");
    refreshWithoutWaiting();
    assertEquals(1, serverView.closing.getCount(), "the view stays open while the request holds it");
    commitBaseFile("002");
    assertEquals(2, remoteView().getLatestBaseFiles(PARTITION).count(), "a later request is answered from the replacement");
    assertTrue(request.isAlive(), "the first request holds the removed view meanwhile");
    assertNotSame(serverView, viewManager.getFileSystemView(basePath));
    resume.countDown();
    request.join(TimeUnit.SECONDS.toMillis(60));
    assertFalse(request.isAlive(), "the request finished");

    assertNull(failure.get(), "the request failed: " + failure.get());
    assertEquals(1, served.get(), "the first request is answered from the view it holds");
    assertTrue(serverView.closedLatch.await(30, TimeUnit.SECONDS), "the removed view was closed with its last lease");
  }

  @Test
  void leaseHolderRefreshesAndClosesTheManagerWithoutWaiting() throws Exception {
    HoodieTestTable.of(metaClient).addCommit("001").withBaseFilesInPartition(PARTITION, UUID.randomUUID().toString());
    PausingView serverView = new PausingView(HoodieTestUtils.createMetaClient(metaClient.getStorageConf(), basePath), 0);
    installServerView(serverView);
    FileSystemViewManager.ViewLease lease = viewManager.leaseFileSystemView(basePath);
    assertSame(serverView, lease.getView());

    viewManager.clearFileSystemView(basePath);
    FileSystemViewManager.ViewLease replacement = viewManager.leaseFileSystemView(basePath);
    assertNotSame(serverView, replacement.getView());
    viewManager.close();
    assertFalse(serverView.closed, "a leased view stays open after the refresh and the manager's close");
    assertEquals(1, serverView.getLatestBaseFiles(PARTITION).count(), "the leased view serves its holder");

    lease.close();
    assertTrue(serverView.closed, "the view is closed with its last lease");
    lease.close();
    replacement.close();
  }

  @Test
  void retiredViewWithTwoLeasesClosesWithTheLastRelease() throws Exception {
    HoodieTestTable.of(metaClient).addCommit("001").withBaseFilesInPartition(PARTITION, UUID.randomUUID().toString());
    PausingView serverView = new PausingView(HoodieTestUtils.createMetaClient(metaClient.getStorageConf(), basePath), 0);
    installServerView(serverView);
    FileSystemViewManager.ViewLease first = viewManager.leaseFileSystemView(basePath);
    FileSystemViewManager.ViewLease second = viewManager.leaseFileSystemView(basePath);
    assertSame(serverView, first.getView());
    assertSame(serverView, second.getView());

    viewManager.clearFileSystemView(basePath);
    first.close();
    first.close();
    assertFalse(serverView.closed, "the view stays open while one lease is held");
    assertEquals(1, serverView.getLatestBaseFiles(PARTITION).count(), "the remaining holder reads the view");

    second.close();
    assertTrue(serverView.closed, "the view is closed with its last lease");
  }

  /**
   * A refresh that removes the view between the lease's lookup and the lease: the lease is taken on the replacement.
   */
  @Test
  void leaseTakenWhileARefreshRemovesTheViewHoldsTheReplacement() throws Exception {
    HoodieTestTable.of(metaClient).addCommit("001").withBaseFilesInPartition(PARTITION, UUID.randomUUID().toString());
    PausingView serverView = new PausingView(HoodieTestUtils.createMetaClient(metaClient.getStorageConf(), basePath), 0);
    installServerView(serverView);
    AtomicInteger lookups = new AtomicInteger();
    installViewMap(new ConcurrentHashMap<String, SyncableFileSystemView>() {
      @Override
      public SyncableFileSystemView computeIfAbsent(String key, Function<? super String, ? extends SyncableFileSystemView> creator) {
        SyncableFileSystemView view = super.computeIfAbsent(key, creator);
        if (lookups.incrementAndGet() == 1) {
          viewManager.clearFileSystemView(key);
        }
        return view;
      }
    });

    try (FileSystemViewManager.ViewLease lease = viewManager.leaseFileSystemView(basePath)) {
      assertEquals(2, lookups.get(), "the lease looked the view up again after the refresh");
      assertNotSame(serverView, lease.getView());
      assertTrue(serverView.closed, "the removed view was closed by the refresh");
      assertSame(viewManager.getFileSystemView(basePath), lease.getView());
      assertEquals(1, lease.getView().getLatestBaseFiles(PARTITION).count());
    }
  }

  @Test
  void failedCloseOfARemovedViewDoesNotFailItsLastRelease() throws Exception {
    HoodieTestTable.of(metaClient).addCommit("001").withBaseFilesInPartition(PARTITION, UUID.randomUUID().toString());
    PausingView serverView = new PausingView(HoodieTestUtils.createMetaClient(metaClient.getStorageConf(), basePath), 0);
    installServerView(failingOnClose(serverView));
    FileSystemViewManager.ViewLease lease = viewManager.leaseFileSystemView(basePath);

    viewManager.clearFileSystemView(basePath);
    assertDoesNotThrow(lease::close);
    assertTrue(serverView.closed, "the view was closed with its last lease");
    try (FileSystemViewManager.ViewLease replacement = viewManager.leaseFileSystemView(basePath)) {
      assertEquals(1, replacement.getView().getLatestBaseFiles(PARTITION).count());
    }
  }

  private SyncableFileSystemView failingOnClose(SyncableFileSystemView view) {
    return (SyncableFileSystemView) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {SyncableFileSystemView.class},
        (proxy, method, args) -> {
          Object result;
          try {
            result = method.invoke(view, args);
          } catch (InvocationTargetException e) {
            throw e.getCause();
          }
          if ("close".equals(method.getName())) {
            throw new IllegalStateException("close failed");
          }
          return result;
        });
  }

  /**
   * A handler reads the view the request holds, and the manager's view when it is called outside a request.
   */
  @Test
  void handlerCalledOutsideARequestReadsTheManagersView() throws Exception {
    HoodieTestTable.of(metaClient).withPartitionMetaFiles(PARTITION).addCommit("001")
        .withBaseFilesInPartition(PARTITION, UUID.randomUUID().toString());
    assertEquals(1, remoteView().getAllLatestBaseFilesBeforeOrOn("001").get(PARTITION).count());

    BaseFileHandler handler = new BaseFileHandler(metaClient.getStorageConf(), TimelineService.Config.builder().build(), viewManager);
    assertEquals(1, handler.getAllLatestDataFilesBeforeOrOn(basePath, "001").get(PARTITION).size());
  }
}
