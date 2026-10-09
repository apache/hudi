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

package org.apache.hudi.client.embedded;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.metrics.Registry;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.timeline.service.TimelineService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorageConf;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * These tests are mainly focused on testing the creation and reuse of the embedded timeline server.
 */
public class TestEmbeddedTimelineService extends HoodieCommonTestHarness {

  @AfterEach
  public void shutdownTimelineServices() {
    EmbeddedTimelineService.shutdownAllTimelineServers();
  }

  @Test
  public void sameBasePathIsReleasedOnlyAfterLastReference() throws Exception {
    long initialCount = runningServerCount();
    HoodieWriteConfig config = serviceConfig("shared_table", true);
    TimelineService server = Mockito.mock(TimelineService.class);
    EmbeddedTimelineService first = acquireService(config, server);
    EmbeddedTimelineService second = acquireService(config, server);
    assertSame(first, second);
    assertEquals(initialCount + 1, runningServerCount());
    verify(server, times(1)).startService();

    first.stopForBasePath("unknown_table");
    verify(server, never()).unregisterBasePath(any());
    first.stopForBasePath(config.getBasePath());
    verify(server, never()).unregisterBasePath(config.getBasePath());
    verify(server, never()).close();
    assertEquals(initialCount + 1, runningServerCount());

    second.stopForBasePath(config.getBasePath());
    second.stopForBasePath(config.getBasePath());
    verify(server, times(1)).unregisterBasePath(config.getBasePath());
    verify(server, times(1)).close();
    assertEquals(initialCount, runningServerCount());
  }

  @Test
  public void referencesAreTrackedPerTableAndCanBeReacquired() throws Exception {
    HoodieWriteConfig firstConfig = serviceConfig("first_table", true);
    HoodieWriteConfig secondConfig = serviceConfig("second_table", true);
    TimelineService server = Mockito.mock(TimelineService.class);
    EmbeddedTimelineService service = acquireService(firstConfig, server);
    assertSame(service, acquireService(firstConfig, server));
    assertSame(service, acquireService(secondConfig, server));

    service.stopForBasePath(firstConfig.getBasePath());
    verify(server, never()).unregisterBasePath(firstConfig.getBasePath());
    service.stopForBasePath(firstConfig.getBasePath());
    verify(server, times(1)).unregisterBasePath(firstConfig.getBasePath());
    verify(server, never()).close();

    assertSame(service, acquireService(firstConfig, server));
    service.stopForBasePath(secondConfig.getBasePath());
    verify(server, times(1)).unregisterBasePath(secondConfig.getBasePath());
    verify(server, never()).close();
    service.stopForBasePath(firstConfig.getBasePath());
    verify(server, times(2)).unregisterBasePath(firstConfig.getBasePath());
    verify(server, times(1)).close();
  }

  @Test
  public void independentServiceDoesNotRemoveReusableService() throws Exception {
    HoodieWriteConfig sharedConfig = serviceConfig("shared_table", true);
    HoodieWriteConfig independentConfig = serviceConfig("independent_table", false);
    TimelineService sharedServer = Mockito.mock(TimelineService.class);
    TimelineService independentServer = Mockito.mock(TimelineService.class);
    EmbeddedTimelineService shared = acquireService(sharedConfig, sharedServer);
    EmbeddedTimelineService independent = acquireService(independentConfig, independentServer);
    assertNotSame(shared, independent);

    independent.stopForBasePath(independentConfig.getBasePath());
    verify(independentServer, times(1)).close();
    assertSame(shared, acquireService(sharedConfig, sharedServer));
    verify(sharedServer, times(1)).startService();
    shared.stopForBasePath(sharedConfig.getBasePath());
    verify(sharedServer, never()).close();
    shared.stopForBasePath(sharedConfig.getBasePath());
    verify(sharedServer, times(1)).close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void unregisterFailureDoesNotPreventReleasingReferences(boolean hasOtherTable) throws Exception {
    long initialCount = runningServerCount();
    HoodieWriteConfig config = serviceConfig("failing_table", true);
    HoodieWriteConfig otherConfig = serviceConfig("other_table", true);
    TimelineService server = Mockito.mock(TimelineService.class);
    EmbeddedTimelineService service = acquireService(config, server);
    if (hasOtherTable) {
      assertSame(service, acquireService(otherConfig, server));
    }
    doThrow(new RuntimeException("view failed to close")).when(server).unregisterBasePath(config.getBasePath());

    assertDoesNotThrow(() -> service.stopForBasePath(config.getBasePath()));
    assertDoesNotThrow(() -> service.stopForBasePath(config.getBasePath()));
    verify(server, times(1)).unregisterBasePath(config.getBasePath());
    if (hasOtherTable) {
      verify(server, never()).close();
      service.stopForBasePath(otherConfig.getBasePath());
    }
    verify(server, times(1)).close();
    assertEquals(initialCount, runningServerCount());
  }

  @Test
  public void concurrentReferencesCloseServerOnlyOnce() throws Exception {
    long initialCount = runningServerCount();
    HoodieWriteConfig config = serviceConfig("shared_table", true);
    TimelineService server = Mockito.mock(TimelineService.class);
    EmbeddedTimelineService service = acquireService(config, server);
    assertSame(service, acquireService(config, server));
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch release = new CountDownLatch(1);
    try {
      Future<?> first = executor.submit(() -> {
        ready.countDown();
        assertTrue(release.await(10, TimeUnit.SECONDS));
        service.stopForBasePath(config.getBasePath());
        return null;
      });
      Future<?> second = executor.submit(() -> {
        ready.countDown();
        assertTrue(release.await(10, TimeUnit.SECONDS));
        service.stopForBasePath(config.getBasePath());
        return null;
      });
      assertTrue(ready.await(10, TimeUnit.SECONDS));
      release.countDown();
      first.get(10, TimeUnit.SECONDS);
      second.get(10, TimeUnit.SECONDS);
      verify(server, times(1)).unregisterBasePath(config.getBasePath());
      verify(server, times(1)).close();
      assertEquals(initialCount, runningServerCount());
    } finally {
      release.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @Test
  public void reacquisitionWaitsForPreviousViewCleanup() throws Exception {
    HoodieWriteConfig config = serviceConfig("reacquired_table", true);
    HoodieWriteConfig otherConfig = serviceConfig("other_table", true);
    TimelineService server = Mockito.mock(TimelineService.class);
    EmbeddedTimelineService service = acquireService(config, server);
    assertSame(service, acquireService(otherConfig, server));
    CountDownLatch cleanupStarted = new CountDownLatch(1);
    CountDownLatch finishCleanup = new CountDownLatch(1);
    CountDownLatch acquireStarted = new CountDownLatch(1);
    AtomicBoolean cleanupFinished = new AtomicBoolean();
    doAnswer(invocation -> {
      cleanupStarted.countDown();
      assertTrue(finishCleanup.await(10, TimeUnit.SECONDS));
      cleanupFinished.set(true);
      return null;
    }).when(server).unregisterBasePath(config.getBasePath());
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<?> release = executor.submit(() -> service.stopForBasePath(config.getBasePath()));
      assertTrue(cleanupStarted.await(10, TimeUnit.SECONDS));
      Future<EmbeddedTimelineService> acquire = executor.submit(() -> {
        acquireStarted.countDown();
        EmbeddedTimelineService acquired = acquireService(config, server);
        assertTrue(cleanupFinished.get(), "Acquisition must not finish before the old view is cleared");
        return acquired;
      });
      assertTrue(acquireStarted.await(10, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> acquire.get(100, TimeUnit.MILLISECONDS));
      finishCleanup.countDown();
      release.get(10, TimeUnit.SECONDS);
      assertSame(service, acquire.get(10, TimeUnit.SECONDS));
      verify(server, never()).close();

      service.stopForBasePath(otherConfig.getBasePath());
      verify(server, never()).close();
      service.stopForBasePath(config.getBasePath());
      verify(server, times(2)).unregisterBasePath(config.getBasePath());
      verify(server, times(1)).close();
    } finally {
      finishCleanup.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void detachedServerCannotBeClosedAgainOrRemoveReplacement(boolean forceShutdown) throws Exception {
    long initialCount = runningServerCount();
    HoodieWriteConfig config = serviceConfig("shared_table", true);
    TimelineService oldServer = Mockito.mock(TimelineService.class);
    EmbeddedTimelineService oldService = acquireService(config, oldServer);
    if (forceShutdown) {
      assertSame(oldService, acquireService(config, oldServer));
    }
    CountDownLatch closeStarted = new CountDownLatch(1);
    CountDownLatch finishClose = new CountDownLatch(1);
    doAnswer(invocation -> {
      closeStarted.countDown();
      assertTrue(finishClose.await(10, TimeUnit.SECONDS));
      return null;
    }).when(oldServer).close();
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<?> closing = executor.submit(() -> {
        if (forceShutdown) {
          EmbeddedTimelineService.shutdownAllTimelineServers();
        } else {
          oldService.stopForBasePath(config.getBasePath());
        }
      });
      assertTrue(closeStarted.await(10, TimeUnit.SECONDS));
      TimelineService newServer = Mockito.mock(TimelineService.class);
      Future<EmbeddedTimelineService> acquiring = executor.submit(() -> {
        // A second shutdown or an owner's release must not attempt to close the detached server.
        EmbeddedTimelineService.shutdownAllTimelineServers();
        EmbeddedTimelineService replacement = acquireService(config, newServer);
        oldService.stopForBasePath(config.getBasePath());
        oldService.stopForBasePath(config.getBasePath());
        assertSame(replacement, acquireService(config, newServer));
        return replacement;
      });
      EmbeddedTimelineService replacement = acquiring.get(10, TimeUnit.SECONDS);
      assertNotSame(oldService, replacement);
      assertEquals(initialCount + 2, runningServerCount());
      finishClose.countDown();
      closing.get(10, TimeUnit.SECONDS);
      verify(oldServer, times(1)).close();
      verify(oldServer, times(forceShutdown ? 0 : 1)).unregisterBasePath(config.getBasePath());
      assertEquals(initialCount + 1, runningServerCount());

      replacement.stopForBasePath(config.getBasePath());
      verify(newServer, never()).close();
      replacement.stopForBasePath(config.getBasePath());
      verify(newServer, times(1)).close();
      assertEquals(initialCount, runningServerCount());
    } finally {
      finishClose.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
    }
  }

  private HoodieWriteConfig serviceConfig(String tableName, boolean reuse) {
    return HoodieWriteConfig.newBuilder()
        .withPath(tempDir.resolve(tableName).toString())
        .withEmbeddedTimelineServerEnabled(true)
        .withEmbeddedTimelineServerReuseEnabled(reuse)
        .build();
  }

  private EmbeddedTimelineService acquireService(HoodieWriteConfig config, TimelineService server) throws IOException {
    return EmbeddedTimelineService.getOrStartEmbeddedTimelineService(
        new HoodieLocalEngineContext(getDefaultStorageConf()), null, config, (storageConf, serviceConfig, viewManager) -> server);
  }

  private long runningServerCount() {
    return Registry.getRegistry("TimelineService").getAllCounts(false).getOrDefault("numEmbeddedTimelineServers", 0L);
  }

  @Test
  public void timelineServiceIdentifierConsidersAllFieldsWhenHostIsNull() {
    EmbeddedTimelineService.TimelineServiceIdentifier identifier =
        new EmbeddedTimelineService.TimelineServiceIdentifier(null, MarkerType.DIRECT, true, false, false);
    EmbeddedTimelineService.TimelineServiceIdentifier sameIdentifier =
        new EmbeddedTimelineService.TimelineServiceIdentifier(null, MarkerType.DIRECT, true, false, false);

    assertEquals(identifier, sameIdentifier);
    assertEquals(identifier.hashCode(), sameIdentifier.hashCode());
    assertNotEquals(identifier,
        new EmbeddedTimelineService.TimelineServiceIdentifier(null, MarkerType.TIMELINE_SERVER_BASED, true, false, false));
    assertNotEquals(identifier,
        new EmbeddedTimelineService.TimelineServiceIdentifier(null, MarkerType.DIRECT, false, false, false));
    assertNotEquals(identifier,
        new EmbeddedTimelineService.TimelineServiceIdentifier(null, MarkerType.DIRECT, true, true, false));
    assertNotEquals(identifier,
        new EmbeddedTimelineService.TimelineServiceIdentifier(null, MarkerType.DIRECT, true, false, true));
  }

  @Test
  public void embeddedTimelineServiceReused() throws Exception {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig1 = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.resolve("table1").toString())
        .withEmbeddedTimelineServerEnabled(true)
        .withEmbeddedTimelineServerReuseEnabled(true)
        .build();
    EmbeddedTimelineService.TimelineServiceCreator mockCreator = Mockito.mock(EmbeddedTimelineService.TimelineServiceCreator.class);
    TimelineService mockService = Mockito.mock(TimelineService.class);
    when(mockCreator.create(any(), any(), any())).thenReturn(mockService);
    when(mockService.startService()).thenReturn(123);
    EmbeddedTimelineService service1 = EmbeddedTimelineService.getOrStartEmbeddedTimelineService(engineContext, null, writeConfig1, mockCreator);

    HoodieWriteConfig writeConfig2 = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.resolve("table2").toString())
        .withEmbeddedTimelineServerEnabled(true)
        .withEmbeddedTimelineServerReuseEnabled(true)
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteTimelineClientRetry(true)
            .build())
        .build();
    EmbeddedTimelineService.TimelineServiceCreator mockCreator2 = Mockito.mock(EmbeddedTimelineService.TimelineServiceCreator.class);
    // do not mock the create method since that should never be called
    EmbeddedTimelineService service2 = EmbeddedTimelineService.getOrStartEmbeddedTimelineService(engineContext, null, writeConfig2, mockCreator2);
    assertSame(service1, service2);

    // Test client properties are not overridden
    assertFalse(service1.getRemoteFileSystemViewConfig(writeConfig1).isRemoteTimelineClientRetryEnabled());
    assertTrue(service1.getRemoteFileSystemViewConfig(writeConfig2).isRemoteTimelineClientRetryEnabled());

    // test shutdown happens after the last path is removed
    service1.stopForBasePath(writeConfig2.getBasePath());
    verify(mockService, never()).close();
    verify(mockService, times(1)).unregisterBasePath(writeConfig2.getBasePath());

    service2.stopForBasePath(writeConfig1.getBasePath());
    verify(mockService, times(1)).unregisterBasePath(writeConfig1.getBasePath());
    verify(mockService, times(1)).close();
  }

  @Test
  public void embeddedTimelineServiceCreatedForDifferentMetadataConfig() throws Exception {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig1 = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.resolve("table1").toString())
        .withEmbeddedTimelineServerEnabled(true)
        .withEmbeddedTimelineServerReuseEnabled(true)
        .build();
    EmbeddedTimelineService.TimelineServiceCreator mockCreator = Mockito.mock(EmbeddedTimelineService.TimelineServiceCreator.class);
    TimelineService mockService = Mockito.mock(TimelineService.class);
    when(mockCreator.create(any(), any(), any())).thenReturn(mockService);
    when(mockService.startService()).thenReturn(321);
    EmbeddedTimelineService service1 = EmbeddedTimelineService.getOrStartEmbeddedTimelineService(engineContext, null, writeConfig1, mockCreator);

    HoodieWriteConfig writeConfig2 = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.resolve("table2").toString())
        .withEmbeddedTimelineServerEnabled(true)
        .withEmbeddedTimelineServerReuseEnabled(true)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .enable(false)
            .build())
        .build();
    EmbeddedTimelineService.TimelineServiceCreator mockCreator2 = Mockito.mock(EmbeddedTimelineService.TimelineServiceCreator.class);
    TimelineService mockService2 = Mockito.mock(TimelineService.class);
    when(mockCreator2.create(any(), any(), any())).thenReturn(mockService2);
    when(mockService2.startService()).thenReturn(456);
    EmbeddedTimelineService service2 = EmbeddedTimelineService.getOrStartEmbeddedTimelineService(engineContext, null, writeConfig2, mockCreator2);
    assertNotSame(service1, service2);

    // test shutdown happens immediately since each server has only one path associated with it
    service1.stopForBasePath(writeConfig1.getBasePath());
    verify(mockService, times(1)).close();

    service2.stopForBasePath(writeConfig2.getBasePath());
    verify(mockService2, times(1)).close();
  }

  @Test
  public void embeddedTimelineServerNotReusedIfReuseDisabled() throws Exception {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig1 = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.resolve("table1").toString())
        .withEmbeddedTimelineServerEnabled(true)
        .withEmbeddedTimelineServerReuseEnabled(true)
        .build();
    EmbeddedTimelineService.TimelineServiceCreator mockCreator = Mockito.mock(EmbeddedTimelineService.TimelineServiceCreator.class);
    TimelineService mockService = Mockito.mock(TimelineService.class);
    when(mockCreator.create(any(), any(), any())).thenReturn(mockService);
    when(mockService.startService()).thenReturn(789);
    EmbeddedTimelineService service1 = EmbeddedTimelineService.getOrStartEmbeddedTimelineService(engineContext, null, writeConfig1, mockCreator);

    HoodieWriteConfig writeConfig2 = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.resolve("table2").toString())
        .withEmbeddedTimelineServerEnabled(true)
        .withEmbeddedTimelineServerReuseEnabled(false)
        .build();
    EmbeddedTimelineService.TimelineServiceCreator mockCreator2 = Mockito.mock(EmbeddedTimelineService.TimelineServiceCreator.class);
    TimelineService mockService2 = Mockito.mock(TimelineService.class);
    when(mockCreator2.create(any(), any(), any())).thenReturn(mockService2);
    when(mockService2.startService()).thenReturn(987);
    EmbeddedTimelineService service2 = EmbeddedTimelineService.getOrStartEmbeddedTimelineService(engineContext, null, writeConfig2, mockCreator2);
    assertNotSame(service1, service2);

    // test shutdown happens immediately since each server has only one path associated with it
    service1.stopForBasePath(writeConfig1.getBasePath());
    verify(mockService, times(1)).unregisterBasePath(writeConfig1.getBasePath());
    verify(mockService, times(1)).close();

    service2.stopForBasePath(writeConfig2.getBasePath());
    verify(mockService2, times(1)).unregisterBasePath(writeConfig2.getBasePath());
    verify(mockService2, times(1)).close();
  }

  @Test
  public void embeddedTimelineServerIsNotReusedAfterStopped() throws Exception {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig1 = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.resolve("table1").toString())
        .withEmbeddedTimelineServerEnabled(true)
        .withEmbeddedTimelineServerReuseEnabled(true)
        .build();
    EmbeddedTimelineService.TimelineServiceCreator mockCreator = Mockito.mock(EmbeddedTimelineService.TimelineServiceCreator.class);
    TimelineService mockService = Mockito.mock(TimelineService.class);
    when(mockCreator.create(any(), any(), any())).thenReturn(mockService);
    when(mockService.startService()).thenReturn(555);
    EmbeddedTimelineService service1 = EmbeddedTimelineService.getOrStartEmbeddedTimelineService(engineContext, null, writeConfig1, mockCreator);

    service1.stopForBasePath(writeConfig1.getBasePath());

    HoodieWriteConfig writeConfig2 = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.resolve("table2").toString())
        .withEmbeddedTimelineServerEnabled(true)
        .withEmbeddedTimelineServerReuseEnabled(true)
        .build();
    EmbeddedTimelineService.TimelineServiceCreator mockCreator2 = Mockito.mock(EmbeddedTimelineService.TimelineServiceCreator.class);
    TimelineService mockService2 = Mockito.mock(TimelineService.class);
    when(mockCreator2.create(any(), any(), any())).thenReturn(mockService2);
    when(mockService2.startService()).thenReturn(111);
    EmbeddedTimelineService service2 = EmbeddedTimelineService.getOrStartEmbeddedTimelineService(engineContext, null, writeConfig2, mockCreator2);
    // a new service will be started since the original was shutdown already
    assertNotSame(service1, service2);

    // test shutdown happens immediately since each server has only one path associated with it
    service1.stopForBasePath(writeConfig1.getBasePath());
    verify(mockService, times(1)).unregisterBasePath(writeConfig1.getBasePath());
    verify(mockService, times(1)).close();

    service2.stopForBasePath(writeConfig2.getBasePath());
    verify(mockService2, times(1)).unregisterBasePath(writeConfig2.getBasePath());
    verify(mockService2, times(1)).close();
  }

  /**
   * A timeline service whose close() throws must still leave this instance releasable.
   *
   * <p>The discriminating input is a TimelineService that throws from close(): the rows returned
   * and the number of services created are identical either way, so only the state left behind
   * after a failed close separates the two behaviours. Before the guard, the throw propagated out
   * of stopForBasePath and left `server` non-null, so the instance could never be closed.
   */
  @Test
  public void stopForBasePathReleasesServerWhenCloseThrows() throws Exception {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.resolve("table_close_throws").toString())
        .withEmbeddedTimelineServerEnabled(true)
        .build();
    EmbeddedTimelineService.TimelineServiceCreator mockCreator =
        Mockito.mock(EmbeddedTimelineService.TimelineServiceCreator.class);
    TimelineService mockService = Mockito.mock(TimelineService.class);
    when(mockCreator.create(any(), any(), any())).thenReturn(mockService);
    when(mockService.startService()).thenReturn(456);
    doThrow(new RuntimeException("jetty refused to stop")).when(mockService).close();

    EmbeddedTimelineService service = EmbeddedTimelineService.getOrStartEmbeddedTimelineService(
        engineContext, null, writeConfig, mockCreator);

    // The failing close must not escape.
    assertDoesNotThrow(() -> service.stopForBasePath(writeConfig.getBasePath()));
    verify(mockService, times(1)).close();

    // And the reference must have been released, so a second stop does not re-enter the close
    // branch. Without the guard `server` is still set here and close() would be called again.
    assertDoesNotThrow(() -> service.stopForBasePath(writeConfig.getBasePath()));
    verify(mockService, times(1)).close();
  }

  /**
   * One server that fails to close must not abandon the others in the registry.
   *
   * <p>Discriminating input: two registered services where the close of one throws. Before the
   * guard the forEach aborted on the first throw, so the second server was never closed and
   * RUNNING_SERVICES was never cleared.
   */
  @Test
  public void shutdownAllContinuesPastAFailingServer() throws Exception {
    HoodieEngineContext engineContext = new HoodieLocalEngineContext(getDefaultStorageConf());
    HoodieWriteConfig throwingConfig = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.resolve("table_throwing").toString())
        .withEmbeddedTimelineServerEnabled(true)
        .withEmbeddedTimelineServerReuseEnabled(true)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build())
        .build();
    EmbeddedTimelineService.TimelineServiceCreator throwingCreator =
        Mockito.mock(EmbeddedTimelineService.TimelineServiceCreator.class);
    TimelineService throwingService = Mockito.mock(TimelineService.class);
    when(throwingCreator.create(any(), any(), any())).thenReturn(throwingService);
    when(throwingService.startService()).thenReturn(654);
    doThrow(new RuntimeException("jetty refused to stop")).when(throwingService).close();
    EmbeddedTimelineService.getOrStartEmbeddedTimelineService(
        engineContext, null, throwingConfig, throwingCreator);

    // A different identifier, so this lands as a separate entry in RUNNING_SERVICES.
    HoodieWriteConfig healthyConfig = HoodieWriteConfig.newBuilder()
        .withPath(tempDir.resolve("table_healthy").toString())
        .withEmbeddedTimelineServerEnabled(true)
        .withEmbeddedTimelineServerReuseEnabled(true)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .build();
    EmbeddedTimelineService.TimelineServiceCreator healthyCreator =
        Mockito.mock(EmbeddedTimelineService.TimelineServiceCreator.class);
    TimelineService healthyService = Mockito.mock(TimelineService.class);
    when(healthyCreator.create(any(), any(), any())).thenReturn(healthyService);
    when(healthyService.startService()).thenReturn(655);
    EmbeddedTimelineService.getOrStartEmbeddedTimelineService(
        engineContext, null, healthyConfig, healthyCreator);

    assertDoesNotThrow(EmbeddedTimelineService::shutdownAllTimelineServers);

    // Both were attempted regardless of iteration order.
    verify(throwingService, times(1)).close();
    verify(healthyService, times(1)).close();
  }
}
