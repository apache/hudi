/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.hive.ddl;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;

import java.io.File;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for how {@link HiveQueryDDLExecutor} handles the SessionState it starts. A
 * HiveSyncTool, and therefore an executor and its session, is built per sync, so anything close()
 * skips accumulates for the life of the JVM.
 */
class TestHiveQueryDDLExecutorSession {

  private ClassLoader testLoader;

  @BeforeEach
  void rememberTestLoader() {
    testLoader = Thread.currentThread().getContextClassLoader();
  }

  /**
   * These tests attach sessions, and attaching one swaps the thread's class loader, so a test that
   * fails mid-way would otherwise hand the next one a thread it does not expect.
   */
  @AfterEach
  void detachTestSession() {
    SessionState.detachSession();
    Thread.currentThread().setContextClassLoader(testLoader);
  }

  /**
   * Hive reclaims a session's four scratch directory roots only in SessionState.close(), and the
   * session is the executor's to close: nothing else holds a reference to it.
   */
  @Test
  void closeClosesTheSessionAfterTheDriver() throws Exception {
    Driver driver = mock(Driver.class);
    SessionState sessionState = mock(SessionState.class);
    HiveQueryDDLExecutor executor = executorWith(driver, sessionState);

    executor.close();

    // Driver.destroy() releases locks through SessionState.get(), which SessionState.close()
    // detaches, so the session has to go last.
    InOrder inOrder = inOrder(driver, sessionState);
    inOrder.verify(driver).close();
    inOrder.verify(driver).destroy();
    inOrder.verify(sessionState).close();
  }

  /**
   * The Driver close is allowed to propagate, so without a finally the session -- and its scratch
   * directories -- would leak exactly when teardown is already going wrong.
   */
  @Test
  void closeClosesTheSessionEvenWhenTheDriverCloseThrows() throws Exception {
    Driver driver = mock(Driver.class);
    when(driver.close()).thenThrow(new RuntimeException("close failed"));
    SessionState sessionState = mock(SessionState.class);
    HiveQueryDDLExecutor executor = executorWith(driver, sessionState);

    assertThrows(RuntimeException.class, executor::close);

    verify(driver, times(1)).destroy();
    verify(sessionState, times(1)).close();
  }

  /**
   * close() is reached from HoodieHiveSyncClient.close() while other resources still need
   * releasing, so a session that fails to close must not become the caller's problem.
   */
  @Test
  void closeSwallowsSessionCloseFailure() throws Exception {
    Driver driver = mock(Driver.class);
    SessionState sessionState = mock(SessionState.class);
    doThrow(new RuntimeException("session close failed")).when(sessionState).close();
    HiveQueryDDLExecutor executor = executorWith(driver, sessionState);

    executor.close();

    verify(sessionState, times(1)).close();
  }

  /**
   * Hive resolves the session from a thread local, so an executor cannot assume the session it
   * started is still the current one: another executor on the same thread may have started its own
   * or closed one, and Driver.compile() dereferences SessionState.get() unconditionally.
   */
  @Test
  void sqlRunsUnderTheSessionThisExecutorStarted() throws Exception {
    SessionState sessionState = mock(SessionState.class);
    Driver driver = mock(Driver.class);
    List<SessionState> sessionsSeenByDriver = new ArrayList<>();
    List<ClassLoader> loadersSeenByDriver = new ArrayList<>();
    when(driver.run(anyString())).thenAnswer(invocation -> {
      sessionsSeenByDriver.add(SessionState.get());
      loadersSeenByDriver.add(Thread.currentThread().getContextClassLoader());
      return null;
    });
    HiveQueryDDLExecutor executor = executorWith(driver, sessionState);
    SessionState.detachSession();
    ClassLoader callerLoader = Thread.currentThread().getContextClassLoader();

    executor.runSQL("SHOW TABLES");

    assertEquals(Collections.singletonList(sessionState), sessionsSeenByDriver,
        "The executor must run under its own session");
    assertEquals(Collections.singletonList(sessionState.getConf().getClassLoader()), loadersSeenByDriver,
        "Binding the session swaps the thread's class loader for the session's, which is what has to be undone");
    assertNull(SessionState.get(), "A thread that held no session must be left holding none");
    assertSame(callerLoader, Thread.currentThread().getContextClassLoader(),
        "detachSession() leaves the session's class loader on the thread, so the operation must take it off");
  }

  /**
   * The thread belongs to the caller, which may be another executor or an application that embeds
   * the sync and holds a session of its own. Leaving this executor's session behind would silently
   * run the caller's later Hive work under our database, configuration and transaction state.
   */
  @Test
  void sqlHandsTheThreadBackToTheSessionItFound() throws Exception {
    SessionState otherSession = mock(SessionState.class);
    when(otherSession.getConf()).thenReturn(new HiveConf());
    SessionState sessionState = mock(SessionState.class);
    Driver driver = mock(Driver.class);
    HiveQueryDDLExecutor executor = executorWith(driver, sessionState);
    SessionState.setCurrentSessionState(otherSession);
    ClassLoader callerLoader = Thread.currentThread().getContextClassLoader();

    executor.runSQL("SHOW TABLES");

    assertSame(otherSession, SessionState.get(), "The session held before the statements must be put back");
    assertSame(callerLoader, Thread.currentThread().getContextClassLoader(),
        "The class loader held before the statements must be put back");
    verify(driver, times(1)).run("SHOW TABLES");
  }

  /**
   * Driver.close() clears the current session's lineage state and Driver.destroy() takes its
   * transaction manager to release locks, so both have to see the session that owns the Driver.
   * SessionState.close() then detaches whatever is attached, which would leave an executor
   * constructed later on this thread with no session at all.
   */
  @Test
  void closeTearsDownUnderItsOwnSessionAndLeavesTheOtherAttached() throws Exception {
    SessionState otherSession = mock(SessionState.class);
    when(otherSession.getConf()).thenReturn(new HiveConf());
    SessionState sessionState = mock(SessionState.class);
    Driver driver = mock(Driver.class);
    List<SessionState> sessionsSeenByDriver = new ArrayList<>();
    when(driver.close()).thenAnswer(invocation -> {
      sessionsSeenByDriver.add(SessionState.get());
      return 0;
    });
    doAnswer(invocation -> {
      sessionsSeenByDriver.add(SessionState.get());
      return null;
    }).when(driver).destroy();
    HiveQueryDDLExecutor executor = executorWith(driver, sessionState);
    SessionState.setCurrentSessionState(otherSession);

    executor.close();

    assertEquals(Arrays.asList(sessionState, sessionState), sessionsSeenByDriver,
        "Driver teardown must run under the session that owns the Driver");
    assertSame(otherSession, SessionState.get(), "The session attached before close() must be put back");
    verify(otherSession, never()).close();
  }

  /**
   * The teardown binds the owned session first, but a session that cannot be bound still has to be
   * closed and its Driver still destroyed: those are the shutdown hook and the scratch directories
   * this teardown exists to reclaim.
   */
  @Test
  void closeTearsDownEvenWhenTheSessionCannotBeBound() throws Exception {
    Driver driver = mock(Driver.class);
    SessionState sessionState = mock(SessionState.class);
    HiveQueryDDLExecutor executor = executorWith(driver, sessionState);
    when(sessionState.getConf()).thenThrow(new RuntimeException("conf unavailable"));

    executor.close();

    InOrder inOrder = inOrder(driver, sessionState);
    inOrder.verify(driver).close();
    inOrder.verify(driver).destroy();
    inOrder.verify(sessionState).close();
  }

  /**
   * SessionState.start() displaces the session the calling thread already holds, so construction
   * has to put it back: the tests above bind explicitly for every statement and for the teardown,
   * which is only a complete story if the thread is the caller's between those operations.
   */
  @Test
  void constructionGivesTheThreadBackToTheCallersSession(@TempDir Path tempDir) throws Exception {
    SessionState callerSession = new SessionState(new HiveConf());
    SessionState.setCurrentSessionState(callerSession);
    ClassLoader callerLoader = Thread.currentThread().getContextClassLoader();

    HiveQueryDDLExecutor executor =
        new HiveQueryDDLExecutor(hiveSyncConfig(tempDir), mock(IMetaStoreClient.class));
    try {
      assertSame(callerSession, SessionState.get(), "Construction must not keep the caller's thread");
      assertSame(callerLoader, Thread.currentThread().getContextClassLoader(),
          "Construction must not leave its session's class loader on the caller's thread");
    } finally {
      executor.close();
    }

    assertSame(callerSession, SessionState.get(), "close() must not take the caller's session with it");
    // SessionState.close() closes the loader its session carries, so a thread left pointing at it
    // could no longer load classes.
    assertSame(callerLoader, Thread.currentThread().getContextClassLoader(),
        "close() must not leave the closed loader of its session on the caller's thread");
  }

  /**
   * The failed constructor closes the session it started, and SessionState.close() detaches
   * whatever is attached, so the caller would lose its session to a failure it did not cause.
   */
  @Test
  void failedConstructionGivesTheThreadBackToTheCallersSession(@TempDir Path tempDir) throws Exception {
    // SessionState.start() attaches before it creates the session directories, so a scratch dir
    // that cannot be created fails construction with our session attached.
    HiveSyncConfig config = hiveSyncConfig(tempDir);
    config.getHiveConf().setVar(HiveConf.ConfVars.SCRATCHDIR,
        Files.createFile(tempDir.resolve("not-a-directory")).toUri().toString() + "/scratch");
    SessionState callerSession = new SessionState(new HiveConf());
    SessionState.setCurrentSessionState(callerSession);
    ClassLoader callerLoader = Thread.currentThread().getContextClassLoader();

    assertThrows(HoodieHiveSyncException.class,
        () -> new HiveQueryDDLExecutor(config, mock(IMetaStoreClient.class)));

    assertSame(callerSession, SessionState.get(), "A failed construction must not take the caller's session");
    assertSame(callerLoader, Thread.currentThread().getContextClassLoader(),
        "A failed construction must not take the caller's class loader");
  }

  /**
   * A config whose Hive session directories all land under the test's temp dir, so starting a real
   * SessionState needs neither a metastore nor a writable /tmp/hive.
   */
  private static HiveSyncConfig hiveSyncConfig(Path tempDir) {
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.defaultFS", "file:///");
    HiveSyncConfig config = new HiveSyncConfig(new Properties(), hadoopConf);
    HiveConf hiveConf = config.getHiveConf();
    hiveConf.setVar(HiveConf.ConfVars.SCRATCHDIR, new File(tempDir.toFile(), "scratch").toURI().toString());
    hiveConf.setVar(HiveConf.ConfVars.LOCALSCRATCHDIR, new File(tempDir.toFile(), "local").getAbsolutePath());
    hiveConf.setVar(HiveConf.ConfVars.DOWNLOADED_RESOURCES_DIR,
        new File(tempDir.toFile(), "resources").getAbsolutePath());
    return config;
  }

  /**
   * Builds an executor without running its constructor, which would need a live metastore. A null
   * metaStoreClient keeps close() away from Hive.closeCurrent() and its static thread-local state.
   */
  private static HiveQueryDDLExecutor executorWith(Driver driver, SessionState sessionState) throws Exception {
    // SessionState.setCurrentSessionState() swaps the thread's context class loader for the one on
    // the session's conf. A real SessionState puts its own UDFClassLoader there, so give the stub a
    // loader of its own too, otherwise the conf would carry this thread's loader and the swap would
    // be invisible.
    HiveConf sessionConf = new HiveConf();
    sessionConf.setClassLoader(new URLClassLoader(new URL[0], sessionConf.getClassLoader()));
    when(sessionState.getConf()).thenReturn(sessionConf);
    HiveQueryDDLExecutor executor = mock(HiveQueryDDLExecutor.class, CALLS_REAL_METHODS);
    setField(executor, "driverPool", Option.empty());
    setField(executor, "metaStoreClientPool", Option.empty());
    setField(executor, "metaStoreClient", null);
    setField(executor, "hiveDriver", driver);
    setField(executor, "sessionState", sessionState);
    return executor;
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    Class<?> type = target.getClass();
    while (type != null) {
      try {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
        return;
      } catch (NoSuchFieldException e) {
        type = type.getSuperclass();
      }
    }
    throw new NoSuchFieldException(name);
  }
}
