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

package org.apache.hudi.functional

import org.apache.hudi.common.model.debezium.OracleDebeziumAvroPayload
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.concurrent.TimeUnit

/**
 * Subprocess writer for the multi-process crash harness (P1). Run as a real OS process via `java -cp`
 * so a parent test can SIGKILL it (Process.destroyForcibly). Writes a v9 Oracle FILL_UNCHANGED MOR
 * table at args(0): a committed base, then a marker file (args(1)) to signal "about to commit the
 * update", then the update commit. The parent kills it around the marker to hit the commit window.
 *
 * args: (0) tablePath  (1) markerFile
 */
object OracleCdcCrashWriter {
  private val cols = Seq("id", "name", "amount", "_changed_columns", "_hoodie_is_deleted", "_event_ordering")
  private def ord(n: Int): String = "00000000000000000000." + "%020d".format(n)

  def main(args: Array[String]): Unit = {
    val path = args(0)
    val marker = args(1)
    val mode = if (args.length > 2) args(2) else "full" // "full" = base+update; "retry" = update only (append)
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("oracle-cdc-crash-writer")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    try {
      def row(id: Int, name: String, amount: Long, changed: String, ordering: String) =
        spark.createDataFrame(Seq((id, name, amount, changed, false, ordering))).toDF(cols: _*)
      def writer(df: org.apache.spark.sql.DataFrame) = df.write.format("hudi")
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.ordering.fields", "_event_ordering")
        .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
        .option("hoodie.table.name", "crash")
        .option("hoodie.compact.inline", "false")
      if (mode == "full") {
        // committed base (alice, amount=100)
        writer(row(1, "alice", 100L, null, ord(100)))
          .option("hoodie.datasource.write.operation", "insert")
          .option("hoodie.write.table.version", "9")
          .option("hoodie.datasource.write.payload.class", classOf[OracleDebeziumAvroPayload].getName)
          .mode(SaveMode.Overwrite).save(path)
        Files.write(Paths.get(marker), "base-committed".getBytes) // signal: base done, update about to start
      }
      // the update (name->bob; amount unchanged placeholder must not win). In "retry" this drains it
      // against a possibly-crashed table (Hudi rolls back any pending inflight first).
      writer(row(1, "bob", 0L, "name", ord(200)))
        .option("hoodie.datasource.write.operation", "upsert")
        .mode(SaveMode.Append).save(path)
      Files.write(Paths.get(marker), "update-committed".getBytes)
    } finally {
      spark.stop()
    }
  }
}

/**
 * P1 crash harness. The `probe` first validates the risky assumption -- that a real Spark-writing
 * subprocess launches with a usable classpath in this sandbox. Only once that's green is the
 * kill variant worth building.
 */
class TestOracleCdcCrashHarness extends SparkClientFunctionalTestHarness {

  // Opt-in soak gate. These launch a real Spark subprocess (slow ~15s/launch) and depend on the
  // parent's `java.class.path` being replayable to a child JVM + on kill timing -- both fragile in a
  // CI matrix. Skip by default; a nightly/soak lane runs them with -Dhudi.crash.harness=true.
  private def assumeCrashHarnessEnabled(): Unit =
    assumeTrue("true".equalsIgnoreCase(System.getProperty("hudi.crash.harness", System.getenv("HUDI_CRASH_HARNESS"))),
      "opt-in crash/soak harness; enable with -Dhudi.crash.harness=true")

  private def launchWriter(tablePath: String, marker: String, mode: String, logFile: String): Process = {
    val javaBin = System.getProperty("java.home") + "/bin/java"
    val cp = System.getProperty("java.class.path")
    val pb = new ProcessBuilder(javaBin, "-cp", cp,
      "org.apache.hudi.functional.OracleCdcCrashWriter", tablePath, marker, mode)
    pb.redirectErrorStream(true)
    pb.redirectOutput(new File(logFile))
    pb.start()
  }

  private def readRow(tablePath: String): Array[org.apache.spark.sql.Row] =
    spark.read.format("hudi").load(tablePath).select("name", "amount").where("id = 1").collect()

  @Test
  def probeSubprocessWritesV9OracleTable(): Unit = {
    assumeCrashHarnessEnabled()
    val local = basePath.replaceFirst("^file:", "") // basePath is a file: URI; java.io.File needs a plain path
    val tablePath = s"$local/crash_probe"
    val marker = s"$local/marker"
    val log = s"$local/writer.log"
    val proc = launchWriter(tablePath, marker, "full", log)
    val finished = proc.waitFor(300, TimeUnit.SECONDS)
    if (!finished) proc.destroyForcibly()
    val logTail = if (new File(log).exists) scala.io.Source.fromFile(log).getLines().toList.takeRight(25).mkString("\n") else "(no log)"
    assertTrue(finished, s"writer subprocess did not finish in 300s. log tail:\n$logTail")
    assertEquals(0, proc.exitValue(), s"writer subprocess exited non-zero. log tail:\n$logTail")
    val out = readRow(tablePath)(0)
    assertEquals("bob", out.getAs[String]("name"), "subprocess update must be committed + readable")
    assertEquals(100L, out.getAs[Long]("amount"), "unchanged amount preserved by the subprocess FILL_UNCHANGED merge")
  }

  @Test
  def crashDuringUpdateLeavesAtomicStateAndRecovers(): Unit = {
    assumeCrashHarnessEnabled()
    // P1 real-crash test (INV8): launch the writer as a real OS process, SIGKILL it (destroyForcibly)
    // right after the base commits, around the update-commit window. After a genuine mid-flight kill
    // the table must be readable and reflect an ATOMIC state -- either the base (alice) or the fully
    // committed update (bob), never a torn/half-merged/duplicated row. Then a retry must drain the
    // update (Hudi rolls back any pending inflight) to the correct final state -- recovery + idempotency.
    val local = basePath.replaceFirst("^file:", "")

    // Each attempt is an INDEPENDENT sample on a FRESH table dir. Reusing one dir across attempts is
    // wrong: attempt N's SIGKILL leaves a stale lock/pending-rollback that stalls attempt N+1's writer.
    val attempts = 2
    for (attempt <- 1 to attempts) {
      val tablePath = s"$local/crash_tbl_$attempt"
      val marker = s"$local/crash_marker_$attempt"

      val p = launchWriter(tablePath, marker, "full", s"$local/full_$attempt.log")
      // wait until the base is committed (update is now imminent), then kill immediately -> race the update commit
      val deadline = System.currentTimeMillis() + 180000
      var baseDone = false
      while (!baseDone && System.currentTimeMillis() < deadline && p.isAlive) {
        baseDone = new File(marker).exists() && new String(Files.readAllBytes(Paths.get(marker))).startsWith("base")
        if (!baseDone) Thread.sleep(50)
      }
      val tail = scala.io.Source.fromFile(s"$local/full_$attempt.log").getLines().toList.takeRight(20).mkString("\n")
      assertTrue(baseDone, s"attempt $attempt: base never committed (writer alive=${p.isAlive}). log tail:\n$tail")
      p.destroyForcibly() // SIGKILL -- no shutdown hooks, a true crash
      p.waitFor(60, TimeUnit.SECONDS)

      // INV8: the crashed table is readable and atomic (alice XOR bob), never torn.
      val rows = readRow(tablePath)
      assertEquals(1, rows.length, s"attempt $attempt: expected exactly one row (no dup/torn), got ${rows.length}")
      val name = rows(0).getAs[String]("name")
      assertTrue(name == "alice" || name == "bob",
        s"attempt $attempt: crash left a non-atomic state: name=$name (must be committed alice or bob, never a half-merge)")
      assertEquals(100L, rows(0).getAs[Long]("amount"),
        s"attempt $attempt: amount must be the committed 100 in either atomic state, never a placeholder leak")

      // recovery: a retry drains the update against THIS crashed table (Hudi rolls back the pending inflight).
      val retry = launchWriter(tablePath, marker, "retry", s"$local/retry_$attempt.log")
      assertTrue(retry.waitFor(300, TimeUnit.SECONDS), s"attempt $attempt: retry did not finish")
      assertEquals(0, retry.exitValue(), s"attempt $attempt: retry writer failed")
      val fin = readRow(tablePath)(0)
      assertEquals("bob", fin.getAs[String]("name"), s"attempt $attempt recovery: retry drains the update to the correct final state")
      assertEquals(100L, fin.getAs[Long]("amount"), s"attempt $attempt recovery: unchanged amount preserved, no duplication/corruption")
    }
  }
}
