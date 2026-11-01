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

import org.apache.hudi.common.serialization.CustomSerializer;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.exception.HoodieException;

import lombok.Value;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.rocksdb.AbstractImmutableNativeReference;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests RocksDB manager {@link RocksDBDAO}.
 */
public class TestRocksDBDAO {

  private RocksDBDAO dbManager;

  @BeforeEach
  public void setUpClass() {
    dbManager = new RocksDBDAO("/dummy/path/" + UUID.randomUUID().toString(),
        FileSystemViewStorageConfig.newBuilder().build().newBuilder().build().getRocksdbBasePath());
  }

  @AfterEach
  public void tearDownClass() {
    if (dbManager != null) {
      dbManager.close();
      dbManager = null;
    }
  }

  /**
   * close() must release the RocksDB {@link org.rocksdb.Logger} and its {@link DBOptions}. The native
   * LoggerJniCallback holds a JNI global reference to the Logger, so leaving it open pins the object
   * for the life of the JVM and no GC can reclaim it.
   */
  @Test
  void testCloseReleasesNativeLoggerAndOptions() {
    org.rocksdb.Logger logger = dbManager.getLogger();
    DBOptions dbOptions = dbManager.getDbOptions();
    assertTrue(logger.isOwningHandle());
    assertTrue(dbOptions.isOwningHandle());

    dbManager.close();

    assertFalse(logger.isOwningHandle(), "logger handle must be closed so it releases its shared_ptr to the native callback");
    assertFalse(dbOptions.isOwningHandle(), "dbOptions must be closed so it drops its shared_ptr to the logger");
    assertNull(dbManager.getLogger());
    assertNull(dbManager.getDbOptions());
  }

  /**
   * The general contract: every native handle reachable from the DAO before close() must be
   * released by it. A field-by-field list only catches what someone remembered to list -- the
   * column-family descriptors hold their own native options inside a Map, which is exactly the
   * shape such a list misses.
   */
  @Test
  void testCloseReleasesEveryReachableNativeHandle() throws Exception {
    dbManager.addColumnFamily("family");
    dbManager.put("family", "key", "value");
    Map<AbstractImmutableNativeReference, String> openBeforeClose = reachableOpenHandles(dbManager);
    assertFalse(openBeforeClose.isEmpty(), "expected the DAO to hold native handles before close()");

    dbManager.close();

    List<String> stillOpen = openBeforeClose.entrySet().stream()
        .filter(entry -> entry.getKey().isOwningHandle())
        .map(Map.Entry::getValue)
        .sorted()
        .collect(Collectors.toList());
    assertTrue(stillOpen.isEmpty(), "native handles still open after close(): " + stillOpen);
  }

  /**
   * Breadth-first over the DAO's own object graph, naming each handle by the path it was found on
   * so a failure says which one leaked. Native references are not walked into -- their innards are
   * RocksDB's business -- and the walk stays inside hudi and rocksdb classes so it never reflects
   * into JDK internals.
   */
  private static Map<AbstractImmutableNativeReference, String> reachableOpenHandles(Object root)
      throws IllegalAccessException {
    Map<AbstractImmutableNativeReference, String> found = new IdentityHashMap<>();
    Set<Object> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    Deque<Object[]> queue = new ArrayDeque<>();
    queue.add(new Object[] {root, root.getClass().getSimpleName()});

    while (!queue.isEmpty()) {
      Object[] current = queue.poll();
      Object value = current[0];
      String path = (String) current[1];
      if (value == null || !visited.add(value)) {
        continue;
      }
      if (value instanceof AbstractImmutableNativeReference) {
        AbstractImmutableNativeReference handle = (AbstractImmutableNativeReference) value;
        if (handle.isOwningHandle()) {
          found.put(handle, path + " (" + handle.getClass().getSimpleName() + ")");
        }
        continue;
      }
      if (value instanceof Map) {
        ((Map<?, ?>) value).forEach((key, mapValue) -> queue.add(new Object[] {mapValue, path + "[" + key + "]"}));
        continue;
      }
      if (value instanceof Iterable) {
        int index = 0;
        for (Object element : (Iterable<?>) value) {
          queue.add(new Object[] {element, path + "[" + index++ + "]"});
        }
        continue;
      }
      String className = value.getClass().getName();
      if (!className.startsWith("org.apache.hudi") && !className.startsWith("org.rocksdb")) {
        continue;
      }
      for (Field field : value.getClass().getDeclaredFields()) {
        if (Modifier.isStatic(field.getModifiers()) || field.getType().isPrimitive()) {
          continue;
        }
        field.setAccessible(true);
        queue.add(new Object[] {field.get(value), path + "." + field.getName()});
      }
    }
    return found;
  }

  /**
   * The column-family descriptors each own a native ColumnFamilyOptions that no weak-reference test
   * can see: the Java object is collectable either way, and rocksdbjni does not free the native
   * struct on GC, so an unclosed one leaks silently per column family.
   */
  @Test
  void testCloseReleasesColumnFamilyDescriptorOptions() throws Exception {
    dbManager.addColumnFamily("family");
    List<ColumnFamilyOptions> descriptorOptions = descriptorOptionsOf(dbManager);
    assertFalse(descriptorOptions.isEmpty(), "expected at least one column-family descriptor");
    assertTrue(descriptorOptions.stream().allMatch(ColumnFamilyOptions::isOwningHandle));

    dbManager.close();

    long stillOpen = descriptorOptions.stream().filter(ColumnFamilyOptions::isOwningHandle).count();
    assertEquals(0, stillOpen, stillOpen + " column-family descriptor options are still open after close()");
  }

  /**
   * Dropping a column family discards its descriptor, so it has to release the options with it.
   */
  @Test
  void testDropColumnFamilyReleasesDescriptorOptions() throws Exception {
    dbManager.addColumnFamily("family");
    List<ColumnFamilyOptions> descriptorOptions = descriptorOptionsOf(dbManager);

    dbManager.dropColumnFamily("family");

    long stillOpen = descriptorOptions.stream().filter(ColumnFamilyOptions::isOwningHandle).count();
    assertEquals(descriptorOptions.size() - 1, stillOpen,
        "dropColumnFamily must close the dropped descriptor's options");
  }

  @SuppressWarnings("unchecked")
  private static List<ColumnFamilyOptions> descriptorOptionsOf(RocksDBDAO dao) throws Exception {
    Field field = RocksDBDAO.class.getDeclaredField("managedDescriptorMap");
    field.setAccessible(true);
    Map<String, ColumnFamilyDescriptor> descriptors = (Map<String, ColumnFamilyDescriptor>) field.get(dao);
    return descriptors.values().stream().map(ColumnFamilyDescriptor::getOptions).collect(Collectors.toList());
  }

  /**
   * The end-to-end claim: once every holder of the callback's shared_ptr is released, the native
   * LoggerJniCallback is destroyed and deletes its JNI global reference, so the Logger -- and with
   * it anything it reaches -- becomes collectable. Handle state alone does not show this; a single
   * surviving copy of the shared_ptr (the Options built in loadManagedColumnFamilies is one) keeps
   * the object pinned for the life of the JVM with every handle reading as closed.
   */
  @Test
  void testCloseMakesLoggerCollectable() throws InterruptedException {
    WeakReference<org.rocksdb.Logger> loggerRef = new WeakReference<>(dbManager.getLogger());

    dbManager.close();
    dbManager = null;

    for (int attempt = 0; attempt < 30 && loggerRef.get() != null; attempt++) {
      System.gc();
      Thread.sleep(50);
    }
    assertNull(loggerRef.get(), "logger is still strongly reachable after close(), so a JNI global reference survived");
  }

  /**
   * The production symptom was DAOs accumulating, not just Loggers: 458 instances retaining
   * 9.51 GB on a driver after 13 days. One surviving JNI global reference per DAO is all it takes,
   * so the contract is that a closed DAO -- and everything it reaches -- becomes collectable, over
   * repeated create/close cycles rather than a single one.
   */
  @Test
  void testClosedDaosDoNotAccumulate() throws InterruptedException {
    List<WeakReference<?>> refs = new ArrayList<>();
    String rocksDBBasePath = FileSystemViewStorageConfig.newBuilder().build().getRocksdbBasePath();
    for (int cycle = 0; cycle < 20; cycle++) {
      RocksDBDAO dao = new RocksDBDAO("/dummy/path/" + UUID.randomUUID(), rocksDBBasePath);
      dao.addColumnFamily("family");
      dao.put("family", "key", "value");
      refs.add(new WeakReference<>(dao));
      refs.add(new WeakReference<>(dao.getLogger()));
      refs.add(new WeakReference<>(dao.getDbOptions()));
      dao.close();
      dao = null;
    }
    assertAllCollectable(refs);
  }

  private static void assertAllCollectable(List<WeakReference<?>> refs) throws InterruptedException {
    for (int attempt = 0; attempt < 50; attempt++) {
      if (refs.stream().allMatch(ref -> ref.get() == null)) {
        return;
      }
      System.gc();
      Thread.sleep(50);
    }
    long alive = refs.stream().filter(ref -> ref.get() != null).count();
    fail(alive + " of " + refs.size() + " objects are still strongly reachable after close(); "
        + "a native reference is still pinning them");
  }

  /**
   * init() runs from the constructor, so a failure there leaves no reference for anyone to call
   * close() on. Fails before RocksDB.open().
   */
  @Test
  void testInitFailureBeforeOpenReleasesNativeHandles() {
    CAPTURED.clear();
    assertThrows(HoodieException.class, () -> newFailingDao(FailurePoint.BEFORE_OPEN));

    assertFalse(CAPTURED.logger.isOwningHandle(), "logger must be closed when init() fails");
    assertFalse(CAPTURED.dbOptions.isOwningHandle(), "dbOptions must be closed when init() fails");
  }

  /**
   * The same contract for a failure raised after RocksDB.open() has handed back an open DB.
   */
  @Test
  void testInitFailureAfterOpenReleasesDatabaseAndHandles() {
    CAPTURED.clear();
    assertThrows(IllegalStateException.class, () -> newFailingDao(FailurePoint.AFTER_OPEN));

    assertFalse(CAPTURED.rocksDB.isOwningHandle(), "an opened RocksDB must be closed when init() fails after it");
    assertFalse(CAPTURED.logger.isOwningHandle(), "logger must be closed when init() fails");
    assertFalse(CAPTURED.dbOptions.isOwningHandle(), "dbOptions must be closed when init() fails");
  }

  private static RocksDBDAO newFailingDao(FailurePoint failurePoint) {
    CAPTURED.failurePoint = failurePoint;
    return new FailingRocksDBDAO("/dummy/path/" + UUID.randomUUID(),
        FileSystemViewStorageConfig.newBuilder().build().getRocksdbBasePath());
  }

  private enum FailurePoint {
    BEFORE_OPEN, AFTER_OPEN
  }

  /**
   * Static, including the failure point: the overrides run from the superclass constructor, before
   * any instance field of the subclass has been assigned.
   */
  private static final Captured CAPTURED = new Captured();

  private static final class Captured {
    private org.rocksdb.Logger logger;
    private DBOptions dbOptions;
    private RocksDB rocksDB;
    private FailurePoint failurePoint;

    void clear() {
      logger = null;
      dbOptions = null;
      rocksDB = null;
      failurePoint = null;
    }
  }

  /**
   * Injects a failure into init(), capturing the native objects the DAO opened first -- they are
   * unreachable afterwards, since a throwing constructor hands back no reference.
   */
  private static final class FailingRocksDBDAO extends RocksDBDAO {

    private FailingRocksDBDAO(String basePath, String rocksDBBasePath) {
      super(basePath, rocksDBBasePath);
    }

    @Override
    List<ColumnFamilyDescriptor> loadManagedColumnFamilies(DBOptions dbOptions) throws RocksDBException {
      CAPTURED.logger = getLogger();
      CAPTURED.dbOptions = dbOptions;
      if (FailurePoint.BEFORE_OPEN == CAPTURED.failurePoint) {
        throw new RocksDBException("injected init failure before open");
      }
      return super.loadManagedColumnFamilies(dbOptions);
    }

    @Override
    void registerColumnFamilies(List<ColumnFamilyDescriptor> managedColumnFamilies,
                                List<ColumnFamilyHandle> managedHandles) throws RocksDBException {
      CAPTURED.rocksDB = getRocksDB();
      if (FailurePoint.AFTER_OPEN == CAPTURED.failurePoint) {
        throw new IllegalStateException("injected init failure after open");
      }
      super.registerColumnFamilies(managedColumnFamilies, managedHandles);
    }
  }


  /**
   * RocksDB's own levels must land on the log levels an operator greps for -- a FATAL_LEVEL
   * emitted at INFO would hide a corrupt DB behind routine chatter.
   */
  @Test
  void testLoggerMapsRocksDbLevelsToLogLevels() {
    CapturingAppender appender = new CapturingAppender();
    Logger daoLogger = (Logger) LogManager.getLogger(RocksDBDAO.class);
    Level originalLevel = daoLogger.getLevel();
    RocksDBDAO.RocksDBLogger rocksLogger = (RocksDBDAO.RocksDBLogger) dbManager.getLogger();
    try {
      appender.start();
      daoLogger.addAppender(appender);
      Configurator.setLevel(RocksDBDAO.class.getName(), Level.DEBUG);

      rocksLogger.log(InfoLogLevel.DEBUG_LEVEL, "rocksdb-level-debug");
      rocksLogger.log(InfoLogLevel.WARN_LEVEL, "rocksdb-level-warn");
      rocksLogger.log(InfoLogLevel.ERROR_LEVEL, "rocksdb-level-error");
      rocksLogger.log(InfoLogLevel.FATAL_LEVEL, "rocksdb-level-fatal");
      rocksLogger.log(InfoLogLevel.INFO_LEVEL, "rocksdb-level-info");
      rocksLogger.log(InfoLogLevel.HEADER_LEVEL, "rocksdb-level-header");

      assertEquals(Level.DEBUG, appender.levelOf("rocksdb-level-debug"));
      assertEquals(Level.WARN, appender.levelOf("rocksdb-level-warn"));
      assertEquals(Level.ERROR, appender.levelOf("rocksdb-level-error"));
      assertEquals(Level.ERROR, appender.levelOf("rocksdb-level-fatal"));
      assertEquals(Level.INFO, appender.levelOf("rocksdb-level-info"));
      assertEquals(Level.INFO, appender.levelOf("rocksdb-level-header"));
    } finally {
      daoLogger.removeAppender(appender);
      Configurator.setLevel(RocksDBDAO.class.getName(), originalLevel);
    }
  }

  @Test
  public void testRocksDBManager() {
    String prefix1 = "prefix1_";
    String prefix2 = "prefix2_";
    String prefix3 = "prefix3_";
    String prefix4 = "prefix4_";
    List<String> prefixes = Arrays.asList(prefix1, prefix2, prefix3, prefix4);
    String family1 = "family1";
    String family2 = "family2";
    List<String> colFamilies = Arrays.asList(family1, family2);

    final List<Payload<String>> payloads = new ArrayList<>();
    IntStream.range(0, 100).forEach(index -> {
      String prefix = prefixes.get(index % 4);
      String key = prefix + UUID.randomUUID();
      String family = colFamilies.get(index % 2);
      String val = "VALUE_" + UUID.randomUUID();
      payloads.add(new Payload(prefix, key, val, family));
    });

    colFamilies.forEach(family -> dbManager.dropColumnFamily(family));
    colFamilies.forEach(family -> dbManager.addColumnFamily(family));

    Map<String, Map<String, Integer>> countsMap = new HashMap<>();
    payloads.forEach(payload -> {
      dbManager.put(payload.getFamily(), payload.getKey(), payload);

      if (!countsMap.containsKey(payload.family)) {
        countsMap.put(payload.family, new HashMap<>());
      }
      Map<String, Integer> c = countsMap.get(payload.family);
      if (!c.containsKey(payload.prefix)) {
        c.put(payload.prefix, 0);
      }
      int currCount = c.get(payload.prefix);
      c.put(payload.prefix, currCount + 1);
    });

    colFamilies.forEach(family -> {
      prefixes.forEach(prefix -> {
        List<Pair<String, Payload>> gotPayloads =
            dbManager.<Payload>prefixSearch(family, prefix).collect(Collectors.toList());
        Integer expCount = countsMap.get(family).get(prefix);
        assertEquals(expCount == null ? 0L : expCount.longValue(), gotPayloads.size(),
            "Size check for prefix (" + prefix + ") and family (" + family + ")");
        gotPayloads.forEach(p -> {
          assertEquals(p.getRight().getFamily(), family);
          assertTrue(p.getRight().getKey().toString().startsWith(prefix));
        });
      });
    });

    payloads.stream().filter(p -> !p.getPrefix().equalsIgnoreCase(prefix1)).forEach(payload -> {
      Payload p = dbManager.get(payload.getFamily(), payload.getKey());
      assertEquals(payload, p, "Retrieved correct payload for key :" + payload.getKey());

      dbManager.delete(payload.getFamily(), payload.getKey());

      Payload p2 = dbManager.get(payload.getFamily(), payload.getKey());
      assertNull(p2, "Retrieved correct payload for key :" + payload.getKey());
    });

    colFamilies.forEach(family -> {
      long countBeforeDeletion = dbManager.prefixSearch(family, prefix1).count();
      dbManager.prefixDelete(family, prefix1);
      if (countBeforeDeletion > 0) {
        long countAfterDeletion = dbManager.prefixSearch(family, prefix1).count();
        assertEquals(0, countAfterDeletion,
                "Expected prefixDelete to remove all items for family: " + family);
      }
    });

    payloads.stream().filter(p -> !p.getPrefix().equalsIgnoreCase(prefix1)).forEach(payload -> {
      Payload p2 = dbManager.get(payload.getFamily(), payload.getKey());
      assertNull(p2, "Retrieved correct payload for key :" + payload.getKey());
    });

    // Now do a prefix search
    colFamilies.forEach(family -> {
      prefixes.stream().filter(p -> !p.equalsIgnoreCase(prefix1)).forEach(prefix -> {
        List<Pair<String, Payload>> gotPayloads =
            dbManager.<Payload>prefixSearch(family, prefix).collect(Collectors.toList());
        assertEquals(0, gotPayloads.size(),
            "Size check for prefix (" + prefix + ") and family (" + family + ")");
      });
    });

    String rocksDBBasePath = dbManager.getRocksDBBasePath();
    dbManager.close();
    assertFalse(new File(rocksDBBasePath).exists());
  }

  @Test
  public void testPrefixSearchHandler() throws IOException {
    String family = "prefix_handler";
    AtomicInteger deserialized = new AtomicInteger();
    ConcurrentHashMap<String, CustomSerializer<?>> serializers = new ConcurrentHashMap<>();
    serializers.put(family, new CustomSerializer<byte[]>() {
      @Override
      public byte[] serialize(byte[] value) {
        return value;
      }

      @Override
      public byte[] deserialize(byte[] bytes) {
        deserialized.incrementAndGet();
        return bytes;
      }
    });
    RocksDBDAO dao = new RocksDBDAO("/prefix-handler", dbManager.getRocksDBBasePath(), serializers);
    try {
      dao.addColumnFamily(family);
      dao.put(family, "key_1", new byte[] {1});
      dao.put(family, "key_2", new byte[] {2});
      dao.put(family, "key_other", new byte[] {3});
      dao.put(family, "other", new byte[] {4});
      List<String> keys = new ArrayList<>();
      dao.<byte[], IOException>prefixSearch(family, "key_", (key, value) -> {
        keys.add(key);
        assertEquals(keys.size(), deserialized.get(), "Values must be consumed during the scan");
        assertEquals(keys.size(), value[0]);
      });
      assertEquals(Arrays.asList("key_1", "key_2", "key_other"), keys);
      assertEquals(keys, dao.prefixSearch(family, "key_").map(Pair::getKey).collect(Collectors.toList()));
      dao.prefixSearch(family, "missing", (key, value) -> {
        throw new AssertionError("No entries should match");
      });
      IOException failure = new IOException("handler failed");
      deserialized.set(0);
      assertSame(failure, assertThrows(IOException.class, () -> dao.prefixSearch(family, "key_", (key, value) -> {
        throw failure;
      })));
      assertEquals(1, deserialized.get(), "A failed handler must stop the scan immediately");
      assertEquals(4, dao.prefixSearch(family, "").count());
    } finally {
      dao.close();
    }
  }

  @Test
  public void testWithSerializableKey() {
    String prefix1 = "prefix1_";
    String prefix2 = "prefix2_";
    String prefix3 = "prefix3_";
    String prefix4 = "prefix4_";
    List<String> prefixes = Arrays.asList(prefix1, prefix2, prefix3, prefix4);
    String family1 = "family1";
    String family2 = "family2";
    List<String> colFamilies = Arrays.asList(family1, family2);

    final List<Payload<PayloadKey>> payloads = new ArrayList<>();
    IntStream.range(0, 100).forEach(index -> {
      String prefix = prefixes.get(index % 4);
      String key = prefix + UUID.randomUUID().toString();
      String family = colFamilies.get(index % 2);
      String val = "VALUE_" + UUID.randomUUID().toString();
      payloads.add(new Payload(prefix, new PayloadKey((key)), val, family));
    });

    colFamilies.forEach(family -> dbManager.dropColumnFamily(family));
    colFamilies.forEach(family -> dbManager.addColumnFamily(family));

    Map<String, Map<String, Integer>> countsMap = new HashMap<>();
    dbManager.writeBatch(batch -> {
      payloads.forEach(payload -> {
        dbManager.putInBatch(batch, payload.getFamily(), payload.getKey(), payload);

        if (!countsMap.containsKey(payload.family)) {
          countsMap.put(payload.family, new HashMap<>());
        }
        Map<String, Integer> c = countsMap.get(payload.family);
        if (!c.containsKey(payload.prefix)) {
          c.put(payload.prefix, 0);
        }
        int currCount = c.get(payload.prefix);
        c.put(payload.prefix, currCount + 1);
      });
    });

    Iterator<List<Payload<PayloadKey>>> payloadSplits = payloads.stream()
        .collect(Collectors.partitioningBy(s -> payloads.indexOf(s) > payloads.size() / 2)).values()
        .iterator();

    payloads.forEach(payload -> {
      Payload p = dbManager.get(payload.getFamily(), payload.getKey());
      assertEquals(payload, p, "Retrieved correct payload for key :" + payload.getKey());
    });

    payloadSplits.next().forEach(payload -> {
      dbManager.delete(payload.getFamily(), payload.getKey());
      Payload want = dbManager.get(payload.getFamily(), payload.getKey());
      assertNull(want, "Verify deleted during single delete for key :" + payload.getKey());
    });

    dbManager.writeBatch(batch -> {
      payloadSplits.next().forEach(payload -> {
        dbManager.deleteInBatch(batch, payload.getFamily(), payload.getKey());
        Payload want = dbManager.get(payload.getFamily(), payload.getKey());
        assertEquals(payload, want, "Verify not deleted during batch delete in progress for key :" + payload.getKey());
      });
    });

    payloads.forEach(payload -> {
      Payload want = dbManager.get(payload.getFamily(), payload.getKey());
      assertNull(want, "Verify delete for key :" + payload.getKey());
    });

    // Now do a prefix search
    colFamilies.forEach(family -> {
      prefixes.forEach(prefix -> {
        List<Pair<String, Payload>> gotPayloads =
            dbManager.<Payload>prefixSearch(family, prefix).collect(Collectors.toList());
        assertEquals(0, gotPayloads.size(),
            "Size check for prefix (" + prefix + ") and family (" + family + ")");
      });
    });

    String rocksDBBasePath = dbManager.getRocksDBBasePath();
    dbManager.close();
    assertFalse(new File(rocksDBBasePath).exists());
  }

  /**
   * Test that concurrent access to RocksDBDAO does not cause ConcurrentModificationException.
   * This test verifies the thread-safety of the columnFamilySerializers map which is accessed
   * via getSerializerForColumnFamily() during get/put operations.
   */
  @Test
  public void testConcurrentAccess() throws InterruptedException {
    int numThreads = 10;
    int numOperationsPerThread = 100;
    int numColumnFamilies = 5;

    List<String> columnFamilies = new ArrayList<>();
    for (int i = 0; i < numColumnFamilies; i++) {
      String family = "concurrent_family_" + i;
      columnFamilies.add(family);
      dbManager.addColumnFamily(family);
    }

    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    try {
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch doneLatch = new CountDownLatch(numThreads);
      AtomicReference<Throwable> error = new AtomicReference<>(null);

      // Spawn threads that concurrently access different column families
      for (int t = 0; t < numThreads; t++) {
        final int threadId = t;
        executor.submit(() -> {
          try {
            // Wait for all threads to be ready
            startLatch.await();

            for (int i = 0; i < numOperationsPerThread; i++) {
              // Each thread accesses different column families to trigger
              // concurrent calls to getSerializerForColumnFamily()
              String family = columnFamilies.get((threadId + i) % numColumnFamilies);
              String key = "key_" + threadId + "_" + i;
              String value = "value_" + threadId + "_" + i;

              dbManager.put(family, key, value);
              String retrieved = dbManager.get(family, key);
              assertEquals(value, retrieved, "Value mismatch for key: " + key);
            }
          } catch (Throwable t1) {
            error.compareAndSet(null, t1);
          } finally {
            doneLatch.countDown();
          }
        });
      }

      startLatch.countDown();

      // Wait for all threads to complete
      boolean completed = doneLatch.await(60, TimeUnit.SECONDS);

      assertTrue(completed, "Test timed out - threads did not complete in time");
      assertNull(error.get(), "Concurrent access caused an exception: "
          + (error.get() != null ? error.get().getMessage() : ""));
    } finally {
      executor.shutdownNow();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDisableWalAtDaoLevel(boolean disableWAL) {
    RocksDBDAO dbManager = new RocksDBDAO("/dummy/path/" + UUID.randomUUID(),
        FileSystemViewStorageConfig.newBuilder().build().newBuilder().build().getRocksdbBasePath(),
        new ConcurrentHashMap<>(),
        disableWAL);

    String family = "family_disable_wal";
    dbManager.dropColumnFamily(family);
    dbManager.addColumnFamily(family);

    String key1 = "key1";
    String key2 = "key2";
    String key3 = "key3";
    String value1 = "value1";
    String value2 = "value2";
    String value3 = "value3";

    dbManager.put(family, key1, value1);
    dbManager.writeBatch(batch -> {
      dbManager.putInBatch(batch, family, key2, value2);
      dbManager.putInBatch(batch, family, key3, value3);
    });
    dbManager.delete(family, key2);

    assertEquals(value1, dbManager.get(family, key1));
    assertNull(dbManager.get(family, key2));
    assertEquals(value3, dbManager.get(family, key3));

    File rocksDbDir = new File(dbManager.getRocksDBBasePath());
    File[] walFiles = rocksDbDir.listFiles((dir, name) -> name.matches("\\d+\\.log"));
    long walFileSize = walFiles == null ? 0 : Arrays.stream(walFiles).mapToLong(File::length).sum();
    assertEquals(disableWAL, walFileSize == 0, "WAL log total size should be 0 when disableWAL=true");
  }

  @Test
  public void testColumnFamilyExistsAndListColumnFamilies() {
    String family = "new_family";
    assertFalse(dbManager.columnFamilyExists(family));
    assertFalse(dbManager.listColumnFamilies().contains(family));

    dbManager.addColumnFamily(family);
    assertTrue(dbManager.columnFamilyExists(family));
    assertTrue(dbManager.listColumnFamilies().contains(family));

    // Adding again should be a no-op and not affect existence/listing.
    dbManager.addColumnFamily(family);
    assertTrue(dbManager.columnFamilyExists(family));
    assertEquals(1, dbManager.listColumnFamilies().stream().filter(family::equals).count());

    dbManager.dropColumnFamily(family);
    assertFalse(dbManager.columnFamilyExists(family));
    assertFalse(dbManager.listColumnFamilies().contains(family));
  }

  @Test
  public void testListColumnFamiliesTracksMultipleFamilies() {
    List<String> families = Arrays.asList("family_a", "family_b", "family_c");
    families.forEach(family -> dbManager.addColumnFamily(family));

    assertTrue(dbManager.listColumnFamilies().containsAll(families));

    dbManager.dropColumnFamily(families.get(1));
    assertTrue(dbManager.listColumnFamilies().contains(families.get(0)));
    assertFalse(dbManager.listColumnFamilies().contains(families.get(1)));
    assertTrue(dbManager.listColumnFamilies().contains(families.get(2)));
  }

  /**
   * Payload key object.
   */
  public static class PayloadKey implements Serializable {
    private String key;

    public PayloadKey(String key) {
      this.key = key;
    }

    @Override
    public String toString() {
      return key;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PayloadKey that = (PayloadKey) o;
      return Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key);
    }
  }

  /**
   * A payload definition for {@link TestRocksDBDAO}.
   */
  @Value
  public static class Payload<T> implements Serializable {

    String prefix;
    T key;
    String val;
    String family;
  }

  /**
   * RocksDB logs from its own background threads, so the captured list must tolerate concurrent
   * appends and events are matched by marker rather than by position.
   */
  private static final class CapturingAppender extends AbstractAppender {

    private final List<LogEvent> events = new CopyOnWriteArrayList<>();

    private CapturingAppender() {
      super(UUID.randomUUID().toString(), null, null, false, null);
    }

    @Override
    public void append(LogEvent event) {
      events.add(event.toImmutable());
    }

    Level levelOf(String marker) {
      return events.stream()
          .filter(event -> event.getMessage().getFormattedMessage().contains(marker))
          .map(LogEvent::getLevel)
          .findFirst()
          .orElseThrow(() -> new AssertionError("no log event captured for " + marker));
    }
  }

}
