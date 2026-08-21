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

package org.apache.hudi.utilities.functional;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.testutils.KafkaTestUtils;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * P2 Kafka-replay crash harness for the v9 Oracle FILL_UNCHANGED ingestion path. Unlike the spark-side
 * TestOracleCdcCrashHarness (a DataFrame writer), this runs the REAL streamer pipeline end-to-end:
 * a HoodieStreamer subprocess ingests Oracle-Debezium envelopes from a Kafka topic through the
 * OracleDebeziumTransformer into a v9 FILL_UNCHANGED MOR table, with source-checkpointed at-least-once
 * delivery. Crashing (SIGKILL) and restarting the streamer exercises the two properties the single-JVM
 * tests cannot: (INV8) a crash mid-commit leaves only committed data visible, and (INV7) the source
 * redelivers uncommitted events on restart and the merge is idempotent (no dup / no loss).
 *
 * Fidelity note: events are JSON on the wire (JsonKafkaSource) rather than schema-registry Avro. A
 * subprocess cannot reach an in-JVM mock schema registry, and the wire format is orthogonal to the
 * replay/idempotency invariants under test -- the transformer, payload, v9 merge, checkpoint and
 * crash/restart are all the real production code paths.
 *
 * Opt-in soak only: enable with -Dhudi.crash.harness=true. Needs a Docker daemon (testcontainers Kafka,
 * confluentinc/cp-kafka:7.7.1). The probe validates the whole pipeline before the kill variants run.
 */
public class TestOracleCdcKafkaReplayCrashHarness extends SparkClientFunctionalTestHarness {

  /** Oracle Debezium sentinel for an unchanged/unavailable column value (mirrors PostgresDebeziumAvroPayload). */
  private static final String TOASTED = "__debezium_unavailable_value";

  private void assumeEnabled() {
    assumeTrue("true".equalsIgnoreCase(System.getProperty("hudi.crash.harness", System.getenv("HUDI_CRASH_HARNESS"))),
        "opt-in crash/soak harness; enable with -Dhudi.crash.harness=true");
  }

  /**
   * Provides the incoming envelope schema as the SOURCE schema (so JSON parses into nested rows) but a
   * NULL target schema, which makes StreamSync deduce the writer schema from the transformer's flattened
   * output instead of the (nested) source schema. Instantiated by the streamer via reflection.
   */
  public static class SourceOnlySchemaProvider extends FilebasedSchemaProvider {
    public SourceOnlySchemaProvider(TypedProperties props, JavaSparkContext jssc) {
      super(props, jssc);
    }

    @Override
    public Schema getTargetSchema() {
      return null;
    }
  }

  private static String insertEvent(int id, String name, String notes, long scn) {
    return String.format("{\"op\":\"c\",\"ts_ms\":%d,\"before\":null,"
        + "\"after\":{\"id\":%d,\"name\":\"%s\",\"notes\":\"%s\"},"
        + "\"source\":{\"name\":\"ora\",\"ts_ms\":%d,\"txId\":%d,\"scn\":\"%d\",\"commit_scn\":\"%d\"}}",
        scn, id, name, notes, scn, scn, scn, scn);
  }

  private static String updateEvent(int id, String beforeName, String beforeNotes,
                                    String afterName, String afterNotes, long scn) {
    return String.format("{\"op\":\"u\",\"ts_ms\":%d,"
        + "\"before\":{\"id\":%d,\"name\":\"%s\",\"notes\":\"%s\"},"
        + "\"after\":{\"id\":%d,\"name\":\"%s\",\"notes\":\"%s\"},"
        + "\"source\":{\"name\":\"ora\",\"ts_ms\":%d,\"txId\":%d,\"scn\":\"%d\",\"commit_scn\":\"%d\"}}",
        scn, id, beforeName, beforeNotes, id, afterName, afterNotes, scn, scn, scn, scn);
  }

  private String localBase() {
    return basePath().replaceFirst("^file:", ""); // basePath is a file: URI; the subprocess + File need a plain path
  }

  private Process launch(String tablePath, String bootstrap, String topic, String group,
                         String mode, String log) throws IOException {
    String javaBin = System.getProperty("java.home") + "/bin/java";
    String cp = System.getProperty("java.class.path");
    ProcessBuilder pb = new ProcessBuilder(javaBin, "-cp", cp,
        OracleCdcKafkaStreamer.class.getName(),
        tablePath, bootstrap, topic, group, mode);
    pb.redirectErrorStream(true);
    pb.redirectOutput(new File(log));
    return pb.start();
  }

  private static String tail(String log) {
    try {
      List<String> lines = Files.readAllLines(Paths.get(log));
      return String.join("\n", lines.subList(Math.max(0, lines.size() - 30), lines.size()));
    } catch (IOException e) {
      return "(no log: " + e.getMessage() + ")";
    }
  }

  private void runOnceToCompletion(String tablePath, String bootstrap, String topic, String group, String log)
      throws Exception {
    Process p = launch(tablePath, bootstrap, topic, group, "once", log);
    boolean done = p.waitFor(300, TimeUnit.SECONDS);
    if (!done) {
      p.destroyForcibly();
    }
    assertTrue(done, "streamer subprocess did not finish in 300s. log tail:\n" + tail(log));
    assertEquals(0, p.exitValue(), "streamer subprocess exited non-zero. log tail:\n" + tail(log));
  }

  private Row readRow(String tablePath) {
    return spark().read().format("hudi").load(tablePath)
        .select("name", "notes").where("id = 1").collectAsList().get(0);
  }

  /** Current name of id=1, or null if the table/row is not yet readable (tolerates mid-write reads). */
  private String currentName(String tablePath) {
    try {
      List<Row> rows = spark().read().format("hudi").load(tablePath)
          .select("name").where("id = 1").collectAsList();
      return rows.isEmpty() ? null : rows.get(0).getAs("name");
    } catch (Exception e) {
      return null;
    }
  }

  /** Poll until id=1 has the target name (or the writer dies / deadline). */
  private void awaitName(String tablePath, String target, long timeoutMs, Process p) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline && p.isAlive()) {
      if (target.equals(currentName(tablePath))) {
        return;
      }
      Thread.sleep(300);
    }
  }

  @Test
  public void probeKafkaStreamerPipeline() throws Exception {
    assumeEnabled();
    KafkaTestUtils kafka = new KafkaTestUtils();
    kafka.setup();
    try {
      String topic = "oracle_cdc_probe";
      kafka.createTopic(topic, 1);
      String tablePath = localBase() + "/kafka_probe";
      String logDir = System.getProperty("hudi.crash.harness.log", localBase());
      String bootstrap = kafka.brokerAddress();

      // Commit 1: the base insert (alice, notes=orig-notes).
      kafka.sendMessages(topic, new String[] {insertEvent(1, "alice", "orig-notes", 100)});
      runOnceToCompletion(tablePath, bootstrap, topic, "probe-grp", logDir + "/probe1.log");

      // Commit 2: the update (name -> bob; notes carries the toasted sentinel). The streamer resumes
      // from its committed Kafka checkpoint and reads only this new event, then FILL_UNCHANGED merges it
      // against the committed base -- notes must stay orig-notes, not the placeholder.
      kafka.sendMessages(topic, new String[] {updateEvent(1, "alice", "orig-notes", "bob", TOASTED, 200)});
      runOnceToCompletion(tablePath, bootstrap, topic, "probe-grp", logDir + "/probe2.log");

      Row r = readRow(tablePath);
      assertEquals("bob", r.getAs("name"), "the update must be applied through the transformer + merge");
      assertEquals("orig-notes", r.getAs("notes"),
          "FILL_UNCHANGED must preserve the unchanged (toasted) notes column, not persist the placeholder");
    } finally {
      kafka.teardown();
    }
  }

  @Test
  public void crashDuringKafkaIngestionReplaysIdempotently() throws Exception {
    assumeEnabled();
    KafkaTestUtils kafka = new KafkaTestUtils();
    kafka.setup();
    try {
      String topic = "oracle_cdc_crash";
      kafka.createTopic(topic, 1);
      String tablePath = localBase() + "/kafka_crash";
      String bootstrap = kafka.brokerAddress();
      String logDir = System.getProperty("hudi.crash.harness.log", localBase());

      // A monotonic-SCN sequence on id=1. Every update carries the toasted sentinel for notes, so
      // FILL_UNCHANGED must keep orig-notes at every step; the terminal name is dave (highest SCN).
      // Because the merge orders by SCN, the outcome is deterministic no matter how many events get
      // reprocessed on restart -- reprocessing an already-applied SCN is a no-op (idempotent).
      kafka.sendMessages(topic, new String[] {
          insertEvent(1, "alice", "orig-notes", 100),
          updateEvent(1, "alice", "orig-notes", "bob", TOASTED, 200),
          updateEvent(1, "bob", "orig-notes", "carol", TOASTED, 300),
          updateEvent(1, "carol", "orig-notes", "dave", TOASTED, 400)
      });

      // Continuous streamer, one event per commit. Kill it once the base is committed and a couple more
      // events are flowing -- a real SIGKILL mid-stream, leaving uncommitted events in Kafka to replay.
      Process p1 = launch(tablePath, bootstrap, topic, "crash-grp", "continuous", logDir + "/crash1.log");
      awaitName(tablePath, "alice", 180000, p1); // base committed
      Thread.sleep(1500); // let a couple more per-event commits flow before the crash
      p1.destroyForcibly();
      p1.waitFor(60, TimeUnit.SECONDS);

      // INV8: after the crash the table is readable and shows a committed prefix, never torn and never a
      // placeholder leak. (currentName tolerates a transient mid-write read by retrying via the poll.)
      String midName = null;
      for (int i = 0; i < 20 && midName == null; i++) {
        midName = currentName(tablePath);
        if (midName == null) {
          Thread.sleep(200);
        }
      }
      assertTrue(Arrays.asList("alice", "bob", "carol", "dave").contains(midName),
          "crash left a non-committed / torn name for id=1: " + midName);
      assertEquals("orig-notes", readRow(tablePath).getAs("notes"),
          "notes must never be the toasted placeholder, even mid-stream");

      // Restart: resume from the committed Kafka checkpoint, replay the uncommitted tail (at-least-once),
      // drain to the terminal event.
      Process p2 = launch(tablePath, bootstrap, topic, "crash-grp", "continuous", logDir + "/crash2.log");
      awaitName(tablePath, "dave", 240000, p2);
      p2.destroyForcibly();
      p2.waitFor(60, TimeUnit.SECONDS);

      // INV7: idempotent replay -- exactly one row for the key (no duplication), terminal state reached
      // (no loss), and FILL_UNCHANGED preserved across the crash + replay.
      long count = spark().read().format("hudi").load(tablePath).where("id = 1").count();
      assertEquals(1L, count, "at-least-once replay must not duplicate the record key");
      Row fin = readRow(tablePath);
      assertEquals("dave", fin.getAs("name"), "all events drained in SCN order after the replay (no loss)");
      assertEquals("orig-notes", fin.getAs("notes"), "FILL_UNCHANGED preserved through the crash + replay");
    } finally {
      kafka.teardown();
    }
  }
}
