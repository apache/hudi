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
 * Subprocess writer for the multi-process crash harness. Run as a real OS process via `java -cp` so a
 * parent test can SIGKILL it (Process.destroyForcibly) mid-commit. Writes a v9 Oracle FILL_UNCHANGED
 * MOR table at args(0): a committed base (alice, amount=100), a marker file (args(1)) signalling the
 * base is done, then the update (name -> bob; amount carries a placeholder that FILL_UNCHANGED must
 * discard). The base insert + update give two delta commits.
 *
 * args: (0) tablePath  (1) markerFile  (2) mode
 *   full          base insert + update upsert (no compaction)
 *   retry         update upsert only (append) -- drains against a possibly-crashed table
 *   compact       full, but with inline compaction after 2 delta commits (the update triggers it)
 *   retry-compact retry, with inline compaction on (completes a pending/crashed compaction)
 * The update always re-uses the same _event_ordering, so a retry re-delivering it is idempotent.
 */
object OracleCdcCrashWriter {
  private val cols = Seq("id", "name", "amount", "_changed_columns", "_hoodie_is_deleted", "_event_ordering")
  private def ord(n: Int): String = "00000000000000000000." + "%020d".format(n)

  def main(args: Array[String]): Unit = {
    val path = args(0)
    val marker = args(1)
    val mode = if (args.length > 2) args(2) else "full"
    val compact = mode.contains("compact")
    val doBase = !mode.startsWith("retry")
    // "full-big" pads the update with filler rows so the post-inflight data-write phase lasts seconds,
    // widening the AFTER_INFLIGHT_BEFORE_COMPLETION window enough that a SIGKILL reliably lands inside
    // it (a 1-row commit completes sub-millisecond after inflight, too fast to catch without a hook).
    val fillerRows = if (mode == "full-big") 100000 else 0
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("oracle-cdc-crash-writer")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
    try {
      def row(id: Int, name: String, amount: Long, changed: String, ordering: String) =
        spark.createDataFrame(Seq((id, name, amount, changed, false, ordering))).toDF(cols: _*)
      def writer(df: org.apache.spark.sql.DataFrame) = {
        var w = df.write.format("hudi")
          .option("hoodie.datasource.write.recordkey.field", "id")
          .option("hoodie.datasource.write.ordering.fields", "_event_ordering")
          .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
          .option("hoodie.table.name", "crash")
          .option("hoodie.compact.inline", compact.toString)
        if (compact) {
          w = w.option("hoodie.compact.inline.max.delta.commits", "2")
        }
        w
      }
      if (doBase) {
        writer(row(1, "alice", 100L, null, ord(100)))
          .option("hoodie.datasource.write.operation", "insert")
          .option("hoodie.write.table.version", "9")
          .option("hoodie.datasource.write.payload.class", classOf[OracleDebeziumAvroPayload].getName)
          .mode(SaveMode.Overwrite).save(path)
        Files.write(Paths.get(marker), "base-committed".getBytes) // base done; update about to start
      }
      val bob = row(1, "bob", 0L, "name", ord(200))
      val updateDf = if (fillerRows > 0) {
        val filler = spark.range(2, 2 + fillerRows).selectExpr(
          "cast(id as int) as id", "'x' as name", "cast(0 as bigint) as amount",
          "'name' as _changed_columns", "false as _hoodie_is_deleted", s"'${ord(200)}' as _event_ordering")
        bob.unionByName(filler)
      } else {
        bob
      }
      writer(updateDf)
        .option("hoodie.datasource.write.operation", "upsert")
        .mode(SaveMode.Append).save(path)
      Files.write(Paths.get(marker), "update-committed".getBytes)
    } finally {
      spark.stop()
    }
  }
}

/**
 * Multi-process crash harness for the v9 Oracle FILL_UNCHANGED writer/merge (INV8: a crash mid-commit
 * must never leave a half-applied merge visible). Unlike the single-JVM `revertToInflight` stand-in in
 * TestOracleDebeziumV9ReadMerge, these launch a REAL Spark subprocess and SIGKILL it, exercising
 * genuinely torn files, rollback-of-incomplete-instant on restart, and crash-during-recovery.
 *
 * P2 additions over the initial P1 kill: (a) the kill is now timed on the delta-commit *inflight*
 * appearing on the timeline -- the AFTER_INFLIGHT_BEFORE_COMPLETION window -- rather than a coarse
 * "base committed" marker; (b) a crash-during-inline-compaction nemesis; (c) a double-fault that kills
 * the recovery writer while it is rolling back the first crash.
 *
 * Opt-in soak only (subprocess launches are slow + timing/classpath-fragile in a CI matrix): enable
 * with -Dhudi.crash.harness=true. The probe validates the subprocess-launch assumption first.
 */
class TestOracleCdcCrashHarness extends SparkClientFunctionalTestHarness {

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

  private def tailOf(log: String): String =
    if (new File(log).exists) scala.io.Source.fromFile(log).getLines().toList.takeRight(20).mkString("\n") else "(no log)"

  /** true if any file under `<tablePath>/.hoodie` (recursively) contains `substr` in its name. */
  private def timelineHas(dir: File, substr: String): Boolean = {
    val kids = dir.listFiles()
    kids != null && kids.exists(f => if (f.isDirectory) timelineHas(f, substr) else f.getName.contains(substr))
  }

  /** Poll the timeline until a file whose name contains `substr` appears, the writer dies, or deadline. */
  private def waitForTimelineFile(tablePath: String, substr: String, deadlineMs: Long, p: Process): Boolean = {
    val hoodie = new File(tablePath + "/.hoodie")
    var seen = false
    while (!seen && System.currentTimeMillis() < deadlineMs && p.isAlive) {
      seen = timelineHas(hoodie, substr)
      if (!seen) Thread.sleep(20)
    }
    seen || timelineHas(hoodie, substr)
  }

  private def awaitMarker(marker: String, prefix: String, p: Process, timeoutMs: Long, log: String): Unit = {
    val deadline = System.currentTimeMillis() + timeoutMs
    var done = false
    while (!done && System.currentTimeMillis() < deadline && p.isAlive) {
      done = new File(marker).exists() && new String(Files.readAllBytes(Paths.get(marker))).startsWith(prefix)
      if (!done) Thread.sleep(50)
    }
    assertTrue(done, s"marker '$prefix' never appeared (writer alive=${p.isAlive}). log tail:\n${tailOf(log)}")
  }

  /** INV8: after a crash the table is readable and atomic -- one row, a committed name, amount never torn. */
  private def assertAtomic(tablePath: String, attempt: Int): Unit = {
    val rows = readRow(tablePath)
    assertEquals(1, rows.length, s"attempt $attempt: expected exactly one row (no dup/torn), got ${rows.length}")
    val name = rows(0).getAs[String]("name")
    assertTrue(name == "alice" || name == "bob",
      s"attempt $attempt: crash left a non-atomic state: name=$name (must be committed alice or bob, never a half-merge)")
    assertEquals(100L, rows(0).getAs[Long]("amount"),
      s"attempt $attempt: amount must be the committed 100 in either atomic state, never a placeholder leak")
  }

  /** Recovery: a fresh writer drains the pending update to the correct final state (bob, amount preserved). */
  private def recoverAndAssert(tablePath: String, marker: String, mode: String, attempt: Int, log: String): Unit = {
    val r = launchWriter(tablePath, marker, mode, log)
    assertTrue(r.waitFor(300, TimeUnit.SECONDS), s"attempt $attempt: recovery ($mode) did not finish")
    assertEquals(0, r.exitValue(), s"attempt $attempt: recovery ($mode) failed. log tail:\n${tailOf(log)}")
    val fin = readRow(tablePath)(0)
    assertEquals("bob", fin.getAs[String]("name"), s"attempt $attempt recovery: retry must drain the update to bob")
    assertEquals(100L, fin.getAs[Long]("amount"), s"attempt $attempt recovery: unchanged amount preserved, no corruption")
  }

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
    assertTrue(finished, s"writer subprocess did not finish in 300s. log tail:\n${tailOf(log)}")
    assertEquals(0, proc.exitValue(), s"writer subprocess exited non-zero. log tail:\n${tailOf(log)}")
    val out = readRow(tablePath)(0)
    assertEquals("bob", out.getAs[String]("name"), "subprocess update must be committed + readable")
    assertEquals(100L, out.getAs[Long]("amount"), "unchanged amount preserved by the subprocess FILL_UNCHANGED merge")
  }

  @Test
  def crashAtUpdateInflightIsAtomicAndRecovers(): Unit = {
    assumeCrashHarnessEnabled()
    // Precise INV8 kill: SIGKILL the writer the instant the update's delta-commit *inflight* lands on
    // the timeline (data being written, completion marker not yet there -- AFTER_INFLIGHT_BEFORE_COMPLETION).
    // Independent samples on FRESH dirs: attempt N's kill leaves a stale lock that would stall attempt N+1.
    val local = basePath.replaceFirst("^file:", "")
    val attempts = 2
    for (attempt <- 1 to attempts) {
      val tablePath = s"$local/inflight_$attempt"
      val marker = s"$local/inflight_marker_$attempt"
      val log = s"$local/inflight_$attempt.log"
      val p = launchWriter(tablePath, marker, "full", log)
      awaitMarker(marker, "base", p, 180000, log) // base committed; its inflight is already renamed away
      waitForTimelineFile(tablePath, ".deltacommit.inflight", System.currentTimeMillis() + 60000, p) // the update's inflight
      p.destroyForcibly() // SIGKILL -- no shutdown hooks, a true crash
      p.waitFor(60, TimeUnit.SECONDS)
      assertAtomic(tablePath, attempt)
      recoverAndAssert(tablePath, marker, "retry", attempt, s"$local/inflight_retry_$attempt.log")
    }
  }

  @Test
  def crashDuringInlineCompactionRecovers(): Unit = {
    assumeCrashHarnessEnabled()
    // Nemesis: crash while inline compaction (triggered by the 2nd delta commit) is in flight. The
    // FILL_UNCHANGED base produced by a half-done compaction must never be visible; recovery must
    // roll it back / re-run and converge. Assert the nemesis actually armed (a compaction instant was
    // seen) so a pass can't be silent when compaction never scheduled.
    val local = basePath.replaceFirst("^file:", "")
    val attempts = 2
    var compactionArmed = 0
    for (attempt <- 1 to attempts) {
      val tablePath = s"$local/compact_$attempt"
      val marker = s"$local/compact_marker_$attempt"
      val log = s"$local/compact_$attempt.log"
      val p = launchWriter(tablePath, marker, "compact", log)
      awaitMarker(marker, "base", p, 180000, log)
      waitForTimelineFile(tablePath, ".compaction.inflight", System.currentTimeMillis() + 60000, p)
      p.destroyForcibly()
      p.waitFor(60, TimeUnit.SECONDS)
      if (timelineHas(new File(tablePath + "/.hoodie"), ".compaction")) {
        compactionArmed += 1
      }
      assertAtomic(tablePath, attempt)
      recoverAndAssert(tablePath, marker, "retry-compact", attempt, s"$local/compact_retry_$attempt.log")
    }
    assertTrue(compactionArmed >= 1,
      s"compaction nemesis never armed in $attempts attempts (no .compaction instant observed) -- " +
        "lower hoodie.compact.inline.max.delta.commits or add more updates")
  }

  @Test
  def doubleFaultCrashDuringRollbackStillRecovers(): Unit = {
    assumeCrashHarnessEnabled()
    // Double fault (unreachable in a single JVM): crash 1 leaves a pending update inflight; the recovery
    // writer starts rolling that back; crash 2 kills it DURING the rollback; a third writer must still
    // converge. Assert a rollback was actually observed so the crash-during-recovery path really ran.
    val local = basePath.replaceFirst("^file:", "")
    val attempts = 2
    var rollbackArmed = 0
    for (attempt <- 1 to attempts) {
      val tablePath = s"$local/df_$attempt"
      val marker = s"$local/df_marker_$attempt"
      val log1 = s"$local/df1_$attempt.log"
      // "full-big": a large update so the kill reliably lands mid-inflight, leaving a pending inflight
      // for the recovery writer to roll back (a 1-row update commits too fast to leave one).
      val p1 = launchWriter(tablePath, marker, "full-big", log1)
      awaitMarker(marker, "base", p1, 180000, log1)
      waitForTimelineFile(tablePath, ".deltacommit.inflight", System.currentTimeMillis() + 60000, p1)
      Thread.sleep(300) // let the executors get into the post-inflight data-write before the kill
      p1.destroyForcibly()
      p1.waitFor(60, TimeUnit.SECONDS)

      val p2 = launchWriter(tablePath, marker, "retry", s"$local/df2_$attempt.log")
      waitForTimelineFile(tablePath, ".rollback", System.currentTimeMillis() + 120000, p2) // rollback of crash 1
      p2.destroyForcibly() // crash 2: killed during/after the rollback, before committing bob
      p2.waitFor(60, TimeUnit.SECONDS)
      if (timelineHas(new File(tablePath + "/.hoodie"), ".rollback")) {
        rollbackArmed += 1
      }
      assertAtomic(tablePath, attempt) // readable + atomic even after a crash during recovery
      recoverAndAssert(tablePath, marker, "retry", attempt, s"$local/df3_$attempt.log")
    }
    assertTrue(rollbackArmed >= 1,
      s"double-fault never armed in $attempts attempts (no .rollback instant observed) -- crash 1 left no pending inflight")
  }
}
