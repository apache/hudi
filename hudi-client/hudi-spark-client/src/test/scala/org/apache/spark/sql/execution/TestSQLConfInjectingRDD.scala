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

package org.apache.spark.sql.execution

import org.apache.hudi.HoodieSparkUtils.injectSQLConf

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
import org.junit.jupiter.api.{AfterAll, BeforeAll, Test, TestInstance}
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotSame, assertSame, assertTrue}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}

/**
 * Pins down how [[SQLConfInjectingRDD]] ships its captured SQL configs.
 *
 * Clustering wraps every group's RDD in its own [[SQLConfInjectingRDD]] and unions all groups into one stage, so each
 * task deserializes every group's RDD. Java serialization writes an instance shared by several objects once per
 * stream, but writes one copy of each distinct instance: a config map built per RDD is rebuilt once per group in
 * every task (measured on a 4,000-group production plan: 16,000 maps of ~120 entries, ~97 MB per executor with
 * 4 tasks running). Equal configs must therefore share one map instance.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestSQLConfInjectingRDD {

  private val Groups = 12
  private val ConcurrentRounds = 500
  private var spark: SparkSession = _

  @BeforeAll
  def start(): Unit = {
    spark = SparkSession.builder().master("local[2]").appName("sqlconf-injecting-rdd")
      .config("spark.ui.enabled", "false").getOrCreate()
  }

  @AfterAll
  def stop(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  @Test
  def groupsCapturingEqualConfsShareOneConfigMap(): Unit = {
    val conf = spark.sessionState.conf
    val groups = perGroupRdds(conf)
    val first = sqlConfigsOf(groups.head)
    groups.foreach(rdd => assertSame(first, sqlConfigsOf(rdd), "equal configs must share one map instance"))
    assertEquals(conf.getAllConfs, first)
  }

  @Test
  def concurrentlyCreatedGroupsShareOneConfigMap(): Unit = {
    // Clustering creates the groups' RDDs on a thread pool. Each round starts from a different captured conf, so all
    // threads miss at once; the race window is small, hence many rounds.
    val conf = spark.sessionState.conf
    val other = conf.clone()
    other.setConfString(SQLConf.SHUFFLE_PARTITIONS.key, "13")
    val pool = Executors.newFixedThreadPool(Groups)
    try {
      (0 until ConcurrentRounds).foreach { round =>
        SQLConfInjectingRDD.sharedConfigs(other)
        val start = new CountDownLatch(1)
        val futures = (0 until Groups).map { _ =>
          pool.submit(new Callable[Map[String, String]] {
            override def call(): Map[String, String] = {
              start.await()
              SQLConfInjectingRDD.sharedConfigs(conf)
            }
          })
        }
        start.countDown()
        val maps = futures.map(f => System.identityHashCode(f.get(30, TimeUnit.SECONDS))).distinct
        assertEquals(1, maps.size, s"round $round: concurrent groups must share one config map (got ${maps.size})")
      }
    } finally {
      pool.shutdownNow()
    }
  }

  @Test
  def oneTaskStreamDeserializesTheSharedConfigMapOnce(): Unit = {
    // What a task binary does: all groups' RDDs written to, and read back from, a single stream.
    val deserialized = roundTrip(perGroupRdds(spark.sessionState.conf))
    val distinctMaps = deserialized.map(rdd => System.identityHashCode(sqlConfigsOf(rdd))).distinct
    assertEquals(1, distinctMaps.size, s"one config map per task, not one per group (got ${distinctMaps.size})")
  }

  @Test
  def changedConfGetsItsOwnMapNeverAStaleOne(): Unit = {
    val base = spark.sessionState.conf
    val shared = sqlConfigsOf(injectSQLConf(spark.sparkContext.parallelize(Seq(1), 1), base))

    val changed = base.clone()
    changed.setConfString(SQLConf.SHUFFLE_PARTITIONS.key, "7")
    val changedConfigs = sqlConfigsOf(injectSQLConf(spark.sparkContext.parallelize(Seq(1), 1), changed))
    assertNotSame(shared, changedConfigs)
    assertEquals("7", changedConfigs(SQLConf.SHUFFLE_PARTITIONS.key))

    // Back to the original configs: the captured content must match them exactly.
    val again = sqlConfigsOf(injectSQLConf(spark.sparkContext.parallelize(Seq(1), 1), base))
    assertEquals(base.getAllConfs, again)
  }

  @Test
  def tasksRunWithTheCapturedConf(): Unit = {
    val conf = spark.sessionState.conf.clone()
    conf.setConfString(SQLConf.SHUFFLE_PARTITIONS.key, "7")
    // The captured conf is in effect while compute() builds the parent's iterator (like Spark's SQLExecutionRDD),
    // not while that iterator is consumed: read it in the mapPartitions body, which runs during compute().
    val seen = injectSQLConf(
      spark.sparkContext.parallelize(Seq(1), 1).mapPartitions { _ =>
        Iterator(SQLConf.get.getConfString(SQLConf.SHUFFLE_PARTITIONS.key))
      },
      conf).collect()
    assertTrue(seen.sameElements(Seq("7")), s"task saw ${seen.mkString(",")}")
  }

  /** One wrapped RDD per clustering group, each built from the same captured conf, as clustering does. */
  private def perGroupRdds(conf: SQLConf): Seq[RDD[Int]] =
    (0 until Groups).map(g => injectSQLConf(spark.sparkContext.parallelize(Seq(g), 1).map(identity), conf))

  private def sqlConfigsOf(rdd: RDD[_]): Map[String, String] = {
    val field = classOf[SQLConfInjectingRDD[_]].getDeclaredField("sqlConfigs")
    field.setAccessible(true)
    field.get(rdd).asInstanceOf[Map[String, String]]
  }

  private def roundTrip[T](value: T): T = {
    val bytes = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bytes)
    out.writeObject(value)
    out.close()
    new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray)).readObject().asInstanceOf[T]
  }
}
