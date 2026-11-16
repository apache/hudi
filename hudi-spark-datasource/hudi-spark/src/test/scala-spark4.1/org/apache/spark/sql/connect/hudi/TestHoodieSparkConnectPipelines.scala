/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connect.hudi

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.connect.service.SparkConnectService
import org.apache.spark.util.Utils
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import java.io.{File, IOException}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.TimeUnit

/** Real pipelines CLI against a Connect service in the test JVM, without building a bundle. */
class TestHoodieSparkConnectPipelines extends FunSuite with BeforeAndAfterAll {
  private val python = sys.env.getOrElse("HUDI_SDP_PYTHON", "python3")
  private val logs = new File("target/sdp-layer2", UUID.randomUUID().toString)
  private var spark: SparkSession = _
  private var root: File = _
  private var started = false

  private lazy val pythonAvailable: Boolean = {
    logs.mkdirs()
    val probe = "import pyspark; from pyspark.pipelines import cli; " +
      "from pyspark.sql.connect.session import SparkSession; " +
      "assert pyspark.__version__ == '4.1.1', pyspark.__version__"
    try {
      runPython(Seq("-c", probe), "python-probe", 30)._1 == 0
    } catch {
      case _: IOException => false
    }
  }

  private def startServer(): Unit = {
    if (sys.env.get("HUDI_SDP_REQUIRE_PYTHON").contains("true")) {
      assert(pythonAvailable, s"CI requires PySpark 4.1.1 pipelines/connect; see $logs")
    } else {
      assume(pythonAvailable, "Install the SDP requirements and set HUDI_SDP_PYTHON to enable this suite")
    }
    if (!started) {
      root = Utils.createTempDir(namePrefix = "hudi-sdp-layer2")
      spark = SparkSession.builder().master("local[2]").appName("Hudi SDP CLI tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", new File(root, "warehouse").toURI.toString)
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
        .config("spark.connect.grpc.binding.address", "127.0.0.1")
        .config("spark.connect.grpc.binding.port", "0")
        .getOrCreate()
      SparkConnectService.start(spark.sparkContext)
      started = true
    }
  }

  test("CLI materialized view and streaming table write exact Hudi rows") {
    withPipeline { (dir, database) =>
      write(dir, "flows.sql",
        """CREATE MATERIALIZED VIEW mv USING hudi
          |TBLPROPERTIES ('type'='cow', 'primaryKey'='id', 'preCombineField'='ts')
          |AS SELECT 2 AS id, 2L AS ts, 'mv' AS value;
          |CREATE STREAMING TABLE events USING hudi
          |TBLPROPERTIES ('type'='cow', 'primaryKey'='id', 'preCombineField'='ts')
          |AS SELECT * FROM STREAM input_events;
          |""".stripMargin)
      assertSuccess(runPipeline(dir, database))
      assert(rows(database, "mv") == Seq(Row(2, 2L, "mv")))
      assert(rows(database, "events") == Seq(Row(1, 1L, "old")))
    }
  }

  test("flow-scoped SQL settings independently select insert and upsert") {
    withPipeline { (dir, database) =>
      write(dir, "flows.sql", Seq("insert", "upsert").map { operation =>
        s"""SET spark.hoodie.datasource.write.operation=$operation;
           |CREATE STREAMING TABLE ${operation}_events USING hudi
           |TBLPROPERTIES ('type'='cow', 'primaryKey'='id', 'preCombineField'='ts')
           |AS SELECT * FROM STREAM input_events;
           |""".stripMargin
      }.mkString("\n"))
      assertSuccess(runPipeline(dir, database))
      assert(rows(database, "insert_events") == Seq.fill(2)(Row(1, 1L, "old")))
      assert(rows(database, "upsert_events") == Seq(Row(1, 1L, "old")))
    }
  }

  test("a failing CLI flow reports the key error and leaves Connect usable") {
    withPipeline { (dir, database) =>
      write(dir, "flows.sql",
        """CREATE STREAMING TABLE bad_events USING hudi
          |TBLPROPERTIES ('type'='cow', 'primaryKey'='id', 'preCombineField'='ts')
          |AS SELECT CAST(NULL AS INT) AS id, ts, value FROM STREAM input_events;
          |""".stripMargin)
      val (exit, output) = runPipeline(dir, database)
      assert(exit != 0, "The null-key flow unexpectedly succeeded")
      assert(output.contains("recordKey") && output.contains("null"), output.takeRight(12000))
      val probe = "from pyspark.sql import SparkSession; " +
        s"s = SparkSession.builder.remote('$remote').getOrCreate(); " +
        "assert s.range(3).count() == 3; s.stop()"
      assertSuccess(runPython(Seq("-c", probe), s"$database-after-failure", 60))
    }
  }

  private def remote: String = s"sc://127.0.0.1:${SparkConnectService.localPort}"

  private def withPipeline(f: (File, String) => Unit): Unit = {
    startServer()
    val database = "sdp_" + UUID.randomUUID().toString.replace("-", "")
    val dir = new File(root, database)
    dir.mkdirs()
    spark.sql(s"CREATE DATABASE $database").collect()
    val input = new File(dir, "input")
    input.mkdirs()
    write(input, "events.json", """{"id":1,"ts":1,"value":"old"}
      |{"id":1,"ts":1,"value":"old"}
      |""".stripMargin)
    write(dir, "source.py",
      """from pyspark import pipelines as dp
        |from pyspark.sql import SparkSession
        |spark = SparkSession.active()
        |@dp.temporary_view(name="input_events")
        |def input_events():
        |    return spark.readStream.schema("id INT, ts LONG, value STRING").json(spark.conf.get("hudi.test.input"))
        |""".stripMargin)
    try f(dir, database) finally {
      spark.sql(s"DROP DATABASE $database CASCADE").collect()
    }
  }

  private def runPipeline(dir: File, database: String): (Int, String) = {
    def quoted(value: String): String = "'" + value.replace("'", "''") + "'"
    write(dir, "spark-pipeline.yml",
      s"""name: $database
         |catalog: spark_catalog
         |database: $database
         |storage: ${quoted(new File(dir, "checkpoints").toURI.toString)}
         |libraries:
         |  - glob: {include: source.py}
         |  - glob: {include: flows.sql}
         |configuration:
         |  hudi.test.input: ${quoted(new File(dir, "input").toURI.toString)}
         |  spark.sql.shuffle.partitions: '2'
         |  spark.sql.pipelines.maxFlowRetryAttempts: '0'
         |  spark.hoodie.metadata.enable: 'true'
         |""".stripMargin)
    Seq("source.py", "flows.sql", "spark-pipeline.yml").foreach { name =>
      Files.copy(new File(dir, name).toPath, new File(logs, s"$database-$name").toPath)
    }
    runPython(Seq("-m", "pyspark.pipelines.cli", "run",
      "--spec", new File(dir, "spark-pipeline.yml").getAbsolutePath), database, 180)
  }

  private def rows(database: String, table: String): Seq[Row] = {
    spark.table(s"$database.$table").select("id", "ts", "value").orderBy("id", "ts").collect().toSeq
  }

  private def write(dir: File, name: String, value: String): Unit = {
    Files.write(new File(dir, name).toPath, value.getBytes(UTF_8))
  }

  private def assertSuccess(result: (Int, String)): Unit = {
    assert(result._1 == 0, result._2.takeRight(12000))
  }

  private def runPython(args: Seq[String], name: String, timeout: Long): (Int, String) = {
    val log = new File(logs, s"$name.log")
    val builder = new ProcessBuilder((Seq(python) ++ args): _*)
      .redirectErrorStream(true).redirectOutput(log)
    Seq("SPARK_REMOTE", "SPARK_CONNECT_MODE_ENABLED", "SPARK_LOCAL_REMOTE", "SPARK_CONNECT_MODE",
      "PYSPARK_GATEWAY_PORT", "PYSPARK_GATEWAY_SECRET", "PYTHONPATH").foreach(builder.environment().remove)
    builder.environment().put("PYTHONUNBUFFERED", "1")
    // The spark-pipelines launcher translates --remote into SPARK_REMOTE before invoking cli.py.
    if (started) builder.environment().put("SPARK_REMOTE", remote)
    val process = builder.start()
    try {
      assert(process.waitFor(timeout, TimeUnit.SECONDS), s"CLI timed out after ${timeout}s; see $log")
      (process.exitValue(), new String(Files.readAllBytes(log.toPath), UTF_8))
    } finally {
      process.destroy()
      if (!process.waitFor(5, TimeUnit.SECONDS)) {
        process.destroyForcibly()
        process.waitFor(5, TimeUnit.SECONDS)
      }
    }
  }

  override protected def afterAll(): Unit = {
    try {
      if (spark != null) {
        try SparkConnectService.stop() finally spark.stop()
      }
    } finally {
      if (root != null) Utils.deleteRecursively(root)
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      super.afterAll()
    }
  }
}
