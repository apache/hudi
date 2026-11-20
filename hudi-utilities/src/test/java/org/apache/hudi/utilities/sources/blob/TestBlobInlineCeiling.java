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

package org.apache.hudi.utilities.sources.blob;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.io.hadoop.HoodieBaseParquetWriter;
import org.apache.hudi.utilities.sources.helpers.CloudObjectMetadata;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskStart;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The per-task inline ceiling of the blob table ({@link BlobInlineCeiling}): how the slots, heap and reserve are read from the
 * Spark conf, the refusal (both numbers, never clamped), the enforce=false hatch, the job-start check of the blob source ({@link BlobSourceSetup#validate}) and
 * the re-check in the task before its first inline read.
 */
@Tag("functional")
class TestBlobInlineCeiling extends UtilitiesTestBase {

  private static final long MIB = 1024L * 1024L;
  private static final long GIB = 1024L * MIB;

  @TempDir
  Path tempDir;

  private CapturingAppender logs;
  private Logger ceilingLogger;
  private Level previousLevel;

  @BeforeAll
  static void initClass() throws Exception {
    UtilitiesTestBase.initTestServices();
  }

  @AfterAll
  static void cleanupClass() {
    UtilitiesTestBase.cleanUpUtilitiesTestServices();
  }

  @BeforeEach
  void captureLogs() {
    logs = new CapturingAppender();
    ceilingLogger = (Logger) LogManager.getLogger(BlobInlineCeiling.class);
    previousLevel = ceilingLogger.getLevel();
    // addAppender rebuilds the logger from its LoggerConfig (the root's level under -Pwarn-log), so raise the level after it
    ceilingLogger.addAppender(logs);
    ceilingLogger.setLevel(Level.ALL);
  }

  @AfterEach
  void releaseLogs() {
    ceilingLogger.removeAppender(logs);
    ceilingLogger.setLevel(previousLevel);
    logs.stop();
  }

  private static SparkConf conf(String master, String... keyValues) {
    SparkConf conf = new SparkConf(false).set(BlobInlineCeiling.SPARK_MASTER, master);
    for (int i = 0; i < keyValues.length; i += 2) {
      conf.set(keyValues[i], keyValues[i + 1]);
    }
    return conf;
  }

  @Test
  void executorCoresComeFromTheMasterOrTheExecutorConf() {
    assertEquals(1, BlobInlineCeiling.executorCores(conf("local"), "local"));
    assertEquals(3, BlobInlineCeiling.executorCores(conf("local[3]"), "local[3]"));
    assertEquals(4, BlobInlineCeiling.executorCores(conf("local[4,1]"), "local[4,1]"));
    // Spark accepts spaces around the comma; such a master is still local
    assertEquals(4, BlobInlineCeiling.executorCores(conf("local[4, 1]"), "local[4, 1]"));
    assertTrue(BlobInlineCeiling.isLocal("local[8 , 1]"));
    assertEquals(Runtime.getRuntime().availableProcessors(), BlobInlineCeiling.executorCores(conf("local[*, 2]"), "local[*, 2]"));
    // what the pattern still turns away: not a Spark local master
    assertFalse(BlobInlineCeiling.isLocal("local[4,]"));
    assertFalse(BlobInlineCeiling.isLocal("local[]"));
    assertFalse(BlobInlineCeiling.isLocal("localhost"));
    assertEquals(Runtime.getRuntime().availableProcessors(), BlobInlineCeiling.executorCores(conf("local[*]"), "local[*]"));
    assertEquals(Runtime.getRuntime().availableProcessors(), BlobInlineCeiling.executorCores(conf("local[*,2]"), "local[*,2]"));
    assertEquals(3, BlobInlineCeiling.executorCores(conf("local-cluster[2,3,1024]"), "local-cluster[2,3,1024]"));
    assertEquals(5, BlobInlineCeiling.executorCores(conf("yarn", "spark.executor.cores", "5"), "yarn"));
    assertEquals(1, BlobInlineCeiling.executorCores(conf("yarn"), "yarn"));
    // a local master ignores spark.executor.cores: the master's thread count is what runs tasks
    assertEquals(2, BlobInlineCeiling.executorCores(conf("local[2]", "spark.executor.cores", "8"), "local[2]"));
  }

  @Test
  void slotsAreExecutorCoresOverTaskCpus() {
    assertEquals(4, BlobInlineCeiling.onDriver(conf("yarn", "spark.executor.cores", "8", "spark.task.cpus", "2"), 0.5).slots());
    assertEquals(2, BlobInlineCeiling.onDriver(conf("yarn", "spark.executor.cores", "5", "spark.task.cpus", "2"), 0.5).slots());
    assertEquals(1, BlobInlineCeiling.onDriver(conf("yarn", "spark.executor.cores", "1", "spark.task.cpus", "2"), 0.5).slots());
    assertEquals(3, BlobInlineCeiling.onDriver(conf("local[3]"), 0.5).slots());
  }

  @Test
  void reserveFollowsSparksReservedSystemMemoryRule() {
    assertEquals(300 * MIB, BlobInlineCeiling.reservedBytes(conf("yarn")));
    assertEquals(0L, BlobInlineCeiling.reservedBytes(conf("yarn", "spark.testing", "true")));
    assertEquals(123L, BlobInlineCeiling.reservedBytes(conf("yarn", "spark.testing", "true", "spark.testing.reservedMemory", "123")));
    assertEquals(123L, BlobInlineCeiling.reservedBytes(conf("yarn", "spark.testing.reservedMemory", "123")));
    // a negative reserve would raise the share above the heap
    assertEquals(0L, BlobInlineCeiling.reservedBytes(conf("yarn", "spark.testing.reservedMemory", "-1073741824")));
  }

  @Test
  void driverCeilingUsesExecutorMemoryOnAClusterAndThisHeapInLocalMode() {
    BlobInlineCeiling cluster = BlobInlineCeiling.onDriver(
        conf("yarn", "spark.executor.memory", "4g", "spark.executor.cores", "4"), 1.0 / 64);
    assertEquals(4 * GIB, cluster.getExecutorHeapBytes());
    assertEquals((4 * GIB - 300 * MIB) / 4, cluster.shareBytes());
    assertEquals((long) Math.floor((4 * GIB - 300 * MIB) / 4 / 64.0), cluster.ceilingBytes());
    // spark.executor.memory without a unit is MiB, as Spark reads it
    assertEquals(2 * GIB, BlobInlineCeiling.onDriver(conf("yarn", "spark.executor.memory", "2048"), 0.5).getExecutorHeapBytes());
    assertEquals(GIB, BlobInlineCeiling.onDriver(conf("yarn"), 0.5).getExecutorHeapBytes());

    BlobInlineCeiling local = BlobInlineCeiling.onDriver(conf("local[4]", "spark.executor.memory", "64g"), 1.0 / 64);
    assertEquals(Runtime.getRuntime().maxMemory(), local.getExecutorHeapBytes());
    assertEquals((Runtime.getRuntime().maxMemory() - 300 * MIB) / 4, local.shareBytes());
    // a heap smaller than the reserve leaves no share and so a ceiling of 0
    assertEquals(0L, new BlobInlineCeiling(0.5, 100 * MIB, "test", 300 * MIB, 1, 1).ceilingBytes());
  }

  @Test
  void thresholdAtTheCeilingPassesAndOneByteAboveIsRefusedNamingBothNumbers() {
    BlobInlineCeiling ceiling = new BlobInlineCeiling(1.0 / 64, 4 * GIB, "spark.executor.memory", 300 * MIB, 4, 1);
    long c = ceiling.ceilingBytes();
    assertEquals(15_548_416L, c);
    ceiling.check(c, true, "here");
    ceiling.check(0, true, "here");
    HoodieValidationException refused = assertThrows(HoodieValidationException.class, () -> ceiling.check(c + 1, true, "here"));
    String message = refused.getMessage();
    assertTrue(message.startsWith("hoodie.blob.table.inline.max.bytes=" + (c + 1) + " is above the per-task inline ceiling " + c
        + " bytes (checked here)"), message);
    assertTrue(message.contains("hoodie.blob.table.inline.max.heap.fraction 0.015625 x per-task heap share 995098624 bytes"),
        message);
    assertTrue(message.contains("executor heap 4294967296 bytes [spark.executor.memory] - reserved 314572800 bytes) / 4 task slots "
        + "(executor cores 4 / spark.task.cpus 1)"), message);
    assertTrue(message.contains("hoodie.blob.table.inline.ceiling.enforce=false"), message);
  }

  @Test
  void enforceFalseWarnsAndLetsTheThresholdThrough() {
    BlobInlineCeiling ceiling = new BlobInlineCeiling(1.0 / 64, 4 * GIB, "spark.executor.memory", 300 * MIB, 4, 1);
    ceiling.check(GIB, false, "here");
    String text = logs.text();
    assertTrue(text.startsWith("WARN hoodie.blob.table.inline.ceiling.enforce is false"), text);
    assertTrue(text.contains("hoodie.blob.table.inline.max.bytes=" + GIB + " is above the per-task inline ceiling 15548416 bytes"),
        text);
  }

  @ParameterizedTest
  @ValueSource(strings = {"0", "-0.5", "1.5", "NaN", "Infinity", "abc"})
  void fractionOutsideZeroToOneIsRejected(String fraction) {
    TypedProperties props = new TypedProperties();
    props.setProperty(BlobTableConfig.INLINE_MAX_HEAP_FRACTION.key(), fraction);
    assertThrows(IllegalArgumentException.class, () -> BlobInlineCeiling.fraction(props));
  }

  @Test
  void defaultFractionIsOneSixtyFourthAndOneIsAllowed() {
    assertEquals(1.0 / 64, BlobInlineCeiling.fraction(new TypedProperties()));
    TypedProperties props = new TypedProperties();
    props.setProperty(BlobTableConfig.INLINE_MAX_HEAP_FRACTION.key(), "1");
    assertEquals(1.0, BlobInlineCeiling.fraction(props));
  }

  @Test
  void checkOnDriverReadsTheJobProperties() {
    SparkConf yarn = conf("yarn", "spark.executor.memory", "1g", "spark.executor.cores", "1");
    TypedProperties props = new TypedProperties();
    // default threshold 1 MiB under the default ceiling floor((1 GiB - 300 MiB) / 64) = 11,862,016
    BlobInlineCeiling.checkOnDriver(props, yarn);
    props.setProperty(BlobTableConfig.INLINE_MAX_BYTES.key(), String.valueOf(16 * MIB));
    if (HoodieBaseParquetWriter.supportsColumnStatisticsSwitch()) {
      HoodieValidationException refused = assertThrows(HoodieValidationException.class,
          () -> BlobInlineCeiling.checkOnDriver(props, yarn));
      assertTrue(refused.getMessage().contains("is above the per-task inline ceiling 11862016 bytes (checked on the driver at job "
          + "start)"), refused.getMessage());
    } else {
      // without the per-column statistics switch the ceiling checks the value used: 16 MiB is capped to 81,920
      BlobInlineCeiling.checkOnDriver(props, yarn);
    }
    props.setProperty(BlobTableConfig.INLINE_CEILING_ENFORCE.key(), "false");
    BlobInlineCeiling.checkOnDriver(props, yarn);
  }

  /** The streamer with a threshold above the ceiling fails at the source's first fetch: no task runs, nothing is written. */
  @Test
  void streamerRefusesAThresholdAboveTheCeilingBeforeAnyTask() throws Exception {
    BlobJobHarness harness = BlobJobHarness.under(tempDir.resolve("src")).install(jsc.hadoopConfiguration());
    harness.announce(sparkSession, Collections.singletonList(harness.put("docs/a.pdf", randomBytes(4096, 1))));
    String tableBasePath = tempDir.resolve("refused").toUri().toString();
    TypedProperties props = harness.jobProperties(4096);
    // a ceiling of a few bytes on any test heap
    props.setProperty(BlobTableConfig.INLINE_MAX_HEAP_FRACTION.key(), "1e-12");
    AtomicInteger tasks = new AtomicInteger();
    SparkListener counter = new SparkListener() {
      @Override
      public void onTaskStart(SparkListenerTaskStart taskStart) {
        tasks.incrementAndGet();
      }
    };
    jsc.sc().listenerBus().waitUntilEmpty();
    jsc.sc().addSparkListener(counter);
    Throwable thrown;
    try {
      HoodieStreamer.Config cfg = BlobJobHarness.streamerConfig(tableBasePath);
      thrown = assertThrows(Throwable.class, () -> syncNewStreamer(cfg, props));
      jsc.sc().listenerBus().waitUntilEmpty();
    } finally {
      // the context is shared by the class: no listener outlives its test
      jsc.sc().removeSparkListener(counter);
    }

    String message = refusalMessage(thrown);
    assertNotNull(message, String.valueOf(thrown));
    assertTrue(message.startsWith("hoodie.blob.table.inline.max.bytes=4096 is above the per-task inline ceiling 0 bytes (checked "
        + "on the driver at job start)"), message);
    assertEquals(0, tasks.get(), "no task before the refusal");
    assertFalse(Files.exists(tempDir.resolve("refused").resolve(".hoodie").resolve(".blob")));
    assertTrue(completedCommits(tableBasePath).isEmpty());
  }

  /** enforce=false: the same threshold is used as configured (never clamped): the file above the ceiling is stored INLINE. */
  @Test
  void streamerWithEnforceOffStoresTheFileInlineAsConfigured() throws Exception {
    BlobJobHarness harness = BlobJobHarness.under(tempDir.resolve("src")).install(jsc.hadoopConfiguration());
    byte[] bytes = randomBytes(4096, 2);
    harness.announce(sparkSession, Collections.singletonList(harness.put("docs/a.pdf", bytes)));
    String tableBasePath = tempDir.resolve("hatch").toUri().toString();
    TypedProperties props = harness.jobProperties(4096);
    props.setProperty(BlobTableConfig.INLINE_MAX_HEAP_FRACTION.key(), "1e-12");
    props.setProperty(BlobTableConfig.INLINE_CEILING_ENFORCE.key(), "false");

    new HoodieStreamer(BlobJobHarness.streamerConfig(tableBasePath), jsc, Option.of(props)).sync();

    List<Row> rows = sqlContext.read().format("hudi").load(tableBasePath).select("blob.type", "blob.data").collectAsList();
    assertEquals(1, rows.size());
    assertEquals(HoodieSchema.Blob.INLINE, rows.get(0).getString(0));
    assertArrayEquals(bytes, (byte[]) rows.get(0).get(1));
    assertEquals(1, completedCommits(tableBasePath).size());
    String text = logs.text();
    assertTrue(text.contains("WARN hoodie.blob.table.inline.ceiling.enforce is false") && text.contains("on the driver at job start"),
        text);
    assertTrue(text.contains("in the task before its first inline read"), text);
  }

  /**
   * The task's own re-check (the driver's job-start check bypassed by building the rows directly): a partition reaching an inline
   * file is refused before reading it; a partition of out-of-line files only is not affected.
   */
  @Test
  void taskRefusesBeforeItsFirstInlineRead() throws Exception {
    Path sourceRoot = Files.createDirectories(tempDir.resolve("src"));
    Path small = sourceRoot.resolve("docs/small.pdf");
    Path big = sourceRoot.resolve("docs/big.pdf");
    BlobJobHarness.writeFile(small, randomBytes(100, 3));
    BlobJobHarness.writeFile(big, randomBytes(8192, 4));
    CloudObjectMetadata smallMeta = new CloudObjectMetadata(BlobTestPaths.canonicalPath(small.toUri()), 100, "e-small");
    CloudObjectMetadata bigMeta = new CloudObjectMetadata(BlobTestPaths.canonicalPath(big.toUri()), 8192, "e-big");
    TypedProperties props = BlobJobHarness.under(sourceRoot).jobProperties(4096);
    props.setProperty(BlobTableConfig.INLINE_MAX_HEAP_FRACTION.key(), "1e-12");
    BlobRowBuilder builder = new BlobRowBuilder(props, HadoopFSUtils.getStorageConf(new Configuration()));

    // out of line only: no inline read, no check
    List<Row> outOfLine = jsc.parallelize(Collections.singletonList(bigMeta), 1).mapPartitions(builder::build).collect();
    assertEquals(HoodieSchema.Blob.OUT_OF_LINE, outOfLine.get(0).getStruct(BlobTableSchema.schema().fieldIndex(BlobTableSchema.BLOB)).getString(0));

    SparkException failed = assertThrows(SparkException.class,
        () -> jsc.parallelize(Arrays.asList(bigMeta, smallMeta), 1).mapPartitions(builder::build).collect());
    String message = refusalMessage(failed);
    assertNotNull(message, String.valueOf(failed));
    assertTrue(message.startsWith("hoodie.blob.table.inline.max.bytes=4096 is above the per-task inline ceiling 0 bytes (checked in "
        + "the task before its first inline read)"), message);
    assertTrue(message.contains("[the executor JVM's max heap]"), message);
  }

  /** Outside a Spark task (no TaskContext in the thread) a builder has no task heap to check and inlines as configured. */
  @Test
  void builderOutsideATaskIsNotChecked() throws Exception {
    Path file = tempDir.resolve("src/docs/small.pdf");
    byte[] bytes = randomBytes(100, 5);
    BlobJobHarness.writeFile(file, bytes);
    TypedProperties props = BlobJobHarness.under(tempDir.resolve("src")).jobProperties(4096);
    props.setProperty(BlobTableConfig.INLINE_MAX_HEAP_FRACTION.key(), "1e-12");
    BlobRowBuilder builder = new BlobRowBuilder(props, HadoopFSUtils.getStorageConf(new Configuration()));
    Row row = builder.buildRow(new CloudObjectMetadata(BlobTestPaths.canonicalPath(file.toUri()), 100, "e-file"));
    assertArrayEquals(bytes, (byte[]) row.getStruct(BlobTableSchema.schema().fieldIndex(BlobTableSchema.BLOB)).get(1));
  }

  static String refusalMessage(Throwable t) {
    if (t == null) {
      return null;
    }
    String message = t.getMessage();
    if (message != null && message.contains("per-task inline ceiling")) {
      int key = message.indexOf(BlobTableConfig.INLINE_MAX_BYTES.key() + "=");
      return key < 0 ? message : message.substring(key);
    }
    for (Throwable suppressed : t.getSuppressed()) {
      String found = refusalMessage(suppressed);
      if (found != null) {
        return found;
      }
    }
    return t.getCause() == t ? null : refusalMessage(t.getCause());
  }

  private static List<String> completedCommits(String basePath) {
    if (!Files.exists(Path.of(java.net.URI.create(basePath)).resolve(".hoodie"))) {
      return Collections.emptyList();
    }
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConf(new Configuration())).setBasePath(basePath).build();
    List<String> commits = new ArrayList<>();
    metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().getInstants()
        .forEach(i -> commits.add(i.requestedTime()));
    return commits;
  }

  private static byte[] randomBytes(int size, int seed) {
    byte[] bytes = new byte[size];
    new Random(seed).nextBytes(bytes);
    return bytes;
  }

  private static final class CapturingAppender extends AbstractAppender {
    private final List<String> lines = Collections.synchronizedList(new ArrayList<>());

    private CapturingAppender() {
      super(UUID.randomUUID().toString(), null, null, false, null);
      start();
    }

    @Override
    public void append(LogEvent event) {
      lines.add(event.getLevel() + " " + event.getMessage().getFormattedMessage());
    }

    String text() {
      return String.join("\n", lines);
    }
  }

  /** Builds a streamer on the class's context and runs one sync. */
  private void syncNewStreamer(HoodieStreamer.Config cfg, TypedProperties props) throws Exception {
    new HoodieStreamer(cfg, jsc, Option.of(props)).sync();
  }
}
