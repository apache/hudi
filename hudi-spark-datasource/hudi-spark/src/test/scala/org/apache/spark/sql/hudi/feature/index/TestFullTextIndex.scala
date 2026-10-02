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

package org.apache.spark.sql.hudi.feature.index

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, FullTextRawKey, HoodieFileIndex}
import org.apache.hudi.common.config.{HoodieMetadataConfig, RecordMergeMode}
import org.apache.hudi.common.data.HoodieListData
import org.apache.hudi.common.engine.HoodieLocalEngineContext
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.metadata.{FullTextIndexUtils, HoodieBackedTableMetadata}
import org.apache.hudi.metadata.index.fulltext.FullTextIndexer

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, Concat, Expression, GreaterThan, Literal, Not, Or}
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.fulltext.{HudiHasAllTokens, HudiHasAnyTokens, HudiHasPhrase, HudiHasToken, HudiHasTokenPrefix}
import org.apache.spark.sql.types.{LongType, StringType}

import scala.collection.JavaConverters._
import scala.util.Random

class TestFullTextIndex extends HoodieSparkSqlTestBase {

  override protected def beforeAll(): Unit = {
    spark.sql("set hoodie.write.lock.provider = org.apache.hudi.client.transaction.lock.InProcessLockProvider")
  }

  private val msg = AttributeReference("msg", StringType, nullable = true)()
  private val ts = AttributeReference("ts", LongType, nullable = true)()

  private def createTable(tableName: String, basePath: String, extraOptions: String = "", primaryKey: Boolean = true): Unit = {
    val pk = if (primaryKey) "primaryKey = 'id', preCombineField = 'ts'," else ""
    spark.sql(
      s"""
         |create table $tableName (id int, msg string, ts long, p string) using hudi
         | options (
         |  $pk
         |  type = 'cow',
         |  hoodie.metadata.enable = 'true',
         |  hoodie.parquet.small.file.limit = '0'
         |  $extraOptions
         | )
         | partitioned by (p)
         | location '$basePath'
       """.stripMargin)
  }

  private def metaClient(basePath: String): HoodieTableMetaClient =
    HoodieTableMetaClient.builder().setBasePath(basePath).setConf(HoodieTestUtils.getDefaultStorageConf).build()

  /** File names the file index keeps for the filter, with or without data skipping. */
  private def filesRead(basePath: String, filter: Expression, skipping: Boolean = true): Set[String] = {
    val opts = Map(
      "path" -> basePath,
      DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> skipping.toString,
      HoodieMetadataConfig.ENABLE.key -> "true")
    val fileIndex = HoodieFileIndex(spark, metaClient(basePath), None, opts, includeLogFiles = true)
    try {
      fileIndex.listFiles(Seq(), Seq(filter)).flatMap(_.files).map(_.getPath.getName).toSet
    } finally {
      fileIndex.close()
    }
  }

  /** File names that hold at least one row matching the filter, from a full scan. */
  private def filesWithMatches(basePath: String, predicateSql: String): Set[String] =
    spark.read.format("hudi").load(basePath).withColumn("_f", input_file_name())
      .where(predicateSql).select("_f").collect().map(r => new java.io.File(r.getString(0)).getName).toSet

  private def presenceEntries(basePath: String, indexPartition: String, term: String): Seq[String] = {
    val mc = metaClient(basePath)
    val metadata = new HoodieBackedTableMetadata(new HoodieLocalEngineContext(mc.getStorageConf), mc.getStorage,
      HoodieMetadataConfig.newBuilder().enable(true).build(), basePath)
    try {
      metadata.getRecordsByKeyPrefixes(
        HoodieListData.eager(Seq(FullTextRawKey(FullTextIndexUtils.presencePrefix(term))).asJava), indexPartition, true)
        .collectAsList().asScala.map(_.getRecordKey).toSeq
    } finally {
      metadata.close()
    }
  }

  /** Files the scan node reads for a SQL predicate, through the optimizer, with data skipping on. */
  private def scanFiles(basePath: String, predicateSql: String): Long = {
    val df = spark.read.format("hudi").option(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key, "true")
      .option(HoodieMetadataConfig.ENABLE.key, "true").load(basePath).where(predicateSql)
    df.collect()
    val scans = df.queryExecution.executedPlan.collectLeaves().filter(_.metrics.contains("numFiles"))
    assertResult(1)(scans.size)
    scans.head.metrics("numFiles").value
  }

  /** Ids of the rows matching a SQL predicate, read through the file index with or without data skipping. */
  private def idsRead(basePath: String, predicateSql: String, skipping: Boolean = true): Seq[Int] =
    spark.read.format("hudi").option(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key, skipping.toString)
      .option(HoodieMetadataConfig.ENABLE.key, "true").load(basePath).where(predicateSql)
      .select("id").collect().map(_.getInt(0)).toSeq.sorted

  private def ids(sql: String): Seq[Int] = spark.sql(sql).collect().map(_.getInt(0)).toSeq.sorted

  test("Test full-text index prunes by row-level co-occurrence") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      createTable(tableName, basePath)
      spark.sql(s"insert into $tableName values (1, 'disk full', 1, 'a'), (2, 'quota ok', 1, 'a')")
      spark.sql(s"insert into $tableName values (3, 'disk quota', 1, 'b')")
      spark.sql(s"create index idx_msg on $tableName using full_text(msg) options (dense_ratio = '1.0')")

      val all = filesRead(basePath, Literal.TrueLiteral, skipping = false)
      assertResult(2)(all.size)
      val fileA = filesWithMatches(basePath, "p = 'a'").head
      val fileB = filesWithMatches(basePath, "p = 'b'").head

      // File a holds both tokens, but never in one row: the per-file bitmap AND must prune it.
      assertResult(Set(fileB))(filesRead(basePath, HudiHasAllTokens(msg, Literal("disk quota"))))
      assertResult(Set(fileA))(filesRead(basePath, HudiHasToken(msg, Literal("full"))))
      assertResult(Set(fileA, fileB))(filesRead(basePath, HudiHasAnyTokens(msg, Literal("full quota"))))
      assertResult(Set.empty)(filesRead(basePath, HudiHasToken(msg, Literal("absent"))))
      // Case and punctuation go through the same tokenizer as the index.
      assertResult(Set(fileB))(filesRead(basePath, HudiHasAllTokens(msg, Literal("DISK, quota!"))))
      assertResult(all)(filesRead(basePath, HudiHasAllTokens(msg, Literal("disk quota")), skipping = false))

      assertResult(Seq(3))(ids(s"select id from $tableName where hudi_has_all_tokens(msg, 'disk quota')"))
      assertResult(Seq(1))(ids(s"select id from $tableName where hudi_has_token(msg, 'full')"))
      assertResult(Seq(1, 2))(ids(s"select id from $tableName where hudi_has_any_tokens(msg, 'full ok')"))
    }
  }

  test("Test full-text index retires the rewritten unit on COW upsert, and ignores stale entries") {
    Seq(false, true).foreach { skipRetirement =>
      withTempDir { tmp =>
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        createTable(tableName, basePath)
        spark.sql(s"insert into $tableName values (1, 'node rebooted', 1, 'a'), (2, 'user login', 1, 'a')")
        spark.sql(s"insert into $tableName values (3, 'disk quota', 1, 'b')")
        spark.sql(s"create index idx_msg on $tableName using full_text(msg)")
        val partition = "full_text_index_idx_msg"
        assertResult(1)(presenceEntries(basePath, partition, "rebooted").size)

        FullTextIndexer.skipRetirementForTesting = skipRetirement
        try {
          spark.sql(s"update $tableName set msg = 'node back online', ts = 2 where id = 1")
        } finally {
          FullTextIndexer.skipRetirementForTesting = false
        }
        // T3: the old unit is retired. T4: with retirement suppressed the stale entry stays,
        // and the liveness filter still keeps the query correct.
        assertResult(if (skipRetirement) 1 else 0)(presenceEntries(basePath, partition, "rebooted").size)
        assertResult(Set.empty)(filesRead(basePath, HudiHasToken(msg, Literal("rebooted"))))
        assertResult(Seq.empty)(ids(s"select id from $tableName where hudi_has_token(msg, 'rebooted')"))
        assertResult(filesWithMatches(basePath, "p = 'a'"))(filesRead(basePath, HudiHasAllTokens(msg, Literal("back online"))))
        assertResult(Seq(1))(ids(s"select id from $tableName where hudi_has_all_tokens(msg, 'back online')"))
        assertResult(Seq(2))(ids(s"select id from $tableName where hudi_has_token(msg, 'login')"))
      }
    }
  }

  test("Test full-text index when the reader's file slices and the index view are at different instants") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      createTable(tableName, basePath)
      spark.sql(s"insert into $tableName values (1, 'alpha one', 1, 'a'), (2, 'beta two', 1, 'b')")
      spark.sql(s"create index idx_msg on $tableName using full_text(msg)")

      // Reader older than the index: the DataFrame keeps the pre-update file slices while the index has already
      // retired that unit.
      val staleDf = spark.read.format("hudi").load(basePath)
      assertResult(1)(staleDf.where("hudi_has_token(msg, 'alpha')").count())
      spark.sql(s"update $tableName set ts = 2 where id = 1")
      assertResult(1)(staleDf.where("hudi_has_token(msg, 'alpha')").count())

      // Index view older than the reader: a long-lived file index is refreshed after a write.
      val opts = Map("path" -> basePath, DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true", HoodieMetadataConfig.ENABLE.key -> "true")
      val fileIndex = HoodieFileIndex(spark, metaClient(basePath), None, opts, includeLogFiles = true)
      try {
        val filter = HudiHasToken(msg, Literal("gamma"))
        assertResult(Set.empty)(fileIndex.listFiles(Seq(), Seq(filter)).flatMap(_.files).map(_.getPath.getName).toSet)
        spark.sql(s"update $tableName set msg = 'alpha gamma', ts = 3 where id = 1")
        fileIndex.refresh()
        val kept = fileIndex.listFiles(Seq(), Seq(filter)).flatMap(_.files).map(_.getPath.getName).toSet
        assert(filesWithMatches(basePath, "hudi_has_token(msg, 'gamma')").subsetOf(kept), s"kept=$kept")
      } finally {
        fileIndex.close()
      }
    }
  }

  test("Test full-text index on a column added by schema evolution") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      spark.sql(
        s"""
           |create table $tableName (id int, ts long, p string) using hudi
           | options (primaryKey = 'id', preCombineField = 'ts', type = 'cow', hoodie.metadata.enable = 'true',
           |  hoodie.parquet.small.file.limit = '0', hoodie.metadata.index.column.stats.enable = 'false',
           |  hoodie.metadata.index.partition.stats.enable = 'false')
           | partitioned by (p)
           | location '$basePath'
         """.stripMargin)
      spark.sql(s"insert into $tableName values (1, 1, 'a')")
      spark.sql(s"alter table $tableName add columns (msg string)")
      // Added columns are appended before the partition column: (id, ts, msg, p).
      spark.sql(s"insert into $tableName values (2, 1, 'disk quota', 'b')")
      spark.sql(s"create index idx_msg on $tableName using full_text(msg)")
      val withMatch = filesWithMatches(basePath, "hudi_has_token(msg, 'disk')")
      assertResult(1)(withMatch.size)
      // The file written before the column existed is not covered by the index, so it is never pruned.
      val olderFile = filesWithMatches(basePath, "p = 'a'")
      assertResult(1)(olderFile.size)
      assertResult(withMatch ++ olderFile)(filesRead(basePath, HudiHasToken(msg, Literal("disk"))))
      assertResult(Seq(2))(ids(s"select id from $tableName where hudi_has_token(msg, 'disk')"))
    }
  }

  test("Test full-text index is not used for time travel to an older instant") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      createTable(tableName, basePath)
      spark.sql(s"insert into $tableName values (1, 'node rebooted', 1, 'a'), (2, 'user login', 1, 'a')")
      spark.sql(s"create index idx_msg on $tableName using full_text(msg)")
      val beforeUpdate = metaClient(basePath).getActiveTimeline.getCommitsTimeline.filterCompletedInstants().lastInstant().get().requestedTime()
      spark.sql(s"update $tableName set msg = 'node back online', ts = 2 where id = 1")
      // The index only describes the latest units; an as-of read of the old file must not be pruned by it.
      val asOf = spark.read.format("hudi").option(DataSourceReadOptions.TIME_TRAVEL_AS_OF_INSTANT.key, beforeUpdate).load(basePath)
      assertResult(Seq(1))(asOf.where("hudi_has_token(msg, 'rebooted')").select("id").collect().map(_.getInt(0)).toSeq)
      assertResult(Seq(1))(ids(s"select id from $tableName timestamp as of '$beforeUpdate' where hudi_has_token(msg, 'rebooted')"))
      assertResult(Seq.empty)(ids(s"select id from $tableName where hudi_has_token(msg, 'rebooted')"))
    }
  }

  test("Test full-text index across clustering, insert overwrite and rollback") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      createTable(tableName, basePath, extraOptions = ", hoodie.metadata.compact.max.delta.commits = '100'")
      spark.sql(s"insert into $tableName values (1, 'disk full', 1, 'a')")
      spark.sql(s"insert into $tableName values (2, 'quota ok', 1, 'a')")
      spark.sql(s"insert into $tableName values (3, 'disk quota', 1, 'b')")
      spark.sql(s"create index idx_msg on $tableName using full_text(msg)")
      val partition = "full_text_index_idx_msg"

      // Clustering merges the two files of partition a into one new file group.
      spark.sql(s"call run_clustering(table => '$tableName', op => 'scheduleandexecute')")
      spark.catalog.refreshTable(tableName)
      val fileA = filesWithMatches(basePath, "p = 'a'")
      assertResult(1)(fileA.size)
      assertResult(fileA)(filesRead(basePath, HudiHasToken(msg, Literal("full"))))
      assertResult(Seq(1))(ids(s"select id from $tableName where hudi_has_token(msg, 'full')"))
      assertResult(1)(presenceEntries(basePath, partition, "full").size)

      // INSERT OVERWRITE replaces partition a; the old units must not be trusted.
      spark.sql(s"insert overwrite table $tableName partition (p = 'a') select 4, 'fresh text', 1")
      assertResult(Set.empty)(filesRead(basePath, HudiHasToken(msg, Literal("full"))))
      assertResult(Seq.empty)(ids(s"select id from $tableName where hudi_has_token(msg, 'full')"))
      assertResult(Seq(4))(ids(s"select id from $tableName where hudi_has_token(msg, 'fresh')"))

      // T10: rolling back a commit removes its postings.
      spark.sql(s"insert into $tableName values (5, 'xylophone', 1, 'b')")
      assertResult(1)(presenceEntries(basePath, partition, "xylophone").size)
      val lastInstant = metaClient(basePath).getActiveTimeline.getCommitsTimeline.lastInstant().get().requestedTime()
      spark.sql(s"call rollback_to_instant(table => '$tableName', instant_time => '$lastInstant')")
      spark.catalog.refreshTable(tableName)
      assertResult(0)(presenceEntries(basePath, partition, "xylophone").size)
      assertResult(Seq.empty)(ids(s"select id from $tableName where hudi_has_token(msg, 'xylophone')"))
    }
  }

  test("Test full-text index eligibility and keyless tables") {
    withTempDir { tmp =>
      // T8: CUSTOM merge mode is rejected.
      val customTable = generateTableName
      val customPath = s"${tmp.getCanonicalPath}/$customTable"
      spark.createDataFrame(Seq((1, "disk full", 1L, "a"))).toDF("id", "msg", "ts", "p")
        .write.format("hudi")
        .option(HoodieWriteConfig.TBL_NAME.key, customTable)
        .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "id")
        .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "p")
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
        .option(DataSourceWriteOptions.RECORD_MERGE_MODE.key, RecordMergeMode.CUSTOM.name())
        .option(DataSourceWriteOptions.RECORD_MERGE_STRATEGY_ID.key, "example_merger_strategy")
        .option(HoodieMetadataConfig.ENABLE.key, "true")
        .mode(SaveMode.Overwrite)
        .save(customPath)
      assertResult(RecordMergeMode.CUSTOM)(metaClient(customPath).getTableConfig.getRecordMergeMode)
      spark.sql(s"create table $customTable using hudi location '$customPath'")
      checkExceptionContain(s"create index idx_msg on $customTable using full_text(msg)")("CUSTOM merge mode")


      // T9: a table without record keys prunes without any record index.
      val keyless = generateTableName
      val keylessPath = s"${tmp.getCanonicalPath}/$keyless"
      createTable(keyless, keylessPath, primaryKey = false)
      spark.sql(s"insert into $keyless values (1, 'disk full', 1, 'a')")
      spark.sql(s"insert into $keyless values (3, 'disk quota', 1, 'b')")
      checkExceptionContain(s"create index idx_ts on $keyless using full_text(ts)")("must be a STRING column")
      checkExceptionContain(s"create index idx_bad on $keyless using full_text(msg) options (dense_ratio = '2')")("invalid value '2'")
      checkExceptionContain(s"create index idx_bad on $keyless using full_text(msg) options (file_group_count = 'x')")("invalid value 'x'")
      spark.sql(s"create index idx_msg on $keyless using full_text(msg)")
      assertResult(filesWithMatches(keylessPath, "p = 'b'"))(filesRead(keylessPath, HudiHasAllTokens(msg, Literal("disk quota"))))
      assertResult(Seq(3))(ids(s"select id from $keyless where hudi_has_all_tokens(msg, 'disk quota')"))
    }
  }

  test("Test full-text index never prunes a file with a match (property test)") {
    // Positions are stored for every term with dense_ratio 1.0 and for few terms of these small files with 0.25.
    Seq("0.25", "1.0").foreach { denseRatio =>
      withTempDir { tmp =>
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}/$tableName"
        createTable(tableName, basePath)
        val random = new Random(20260924L)
        val vocab = Seq("disk", "full", "quota", "node", "user", "login", "error", "ok", "the", "a", "cache", "miss")
        // A small sub-vocabulary makes rows that hold a phrase's tokens out of order, repeated or apart common.
        val phraseVocab = Seq("disk", "quota", "full")
        def phraseWords(): String = Seq.fill(1 + random.nextInt(3))(phraseVocab(random.nextInt(phraseVocab.size))).mkString(" ")
        def sentence(): String =
          if (random.nextInt(3) == 0) phraseWords() else Seq.fill(1 + random.nextInt(5))(vocab(random.nextInt(vocab.size))).mkString(" ")
        def text(): String = if (random.nextInt(8) == 0) "null" else s"'${sentence()}'"
        def rows(ids: Seq[Int], ts: Int): String =
          ids.map(id => s"($id, ${text()}, $ts, '${if (random.nextBoolean()) "a" else "b"}')").mkString(", ")
        // A file of its own where every row holds every phrase word, in shuffled order and sometimes repeated: the
        // index then reports each word in every row while only some rows hold a given phrase.
        def phraseFileRows(ids: Seq[Int], ts: Int): String = ids.map { id =>
          val words = random.shuffle(phraseVocab ++ phraseVocab.take(random.nextInt(2))).mkString(" ")
          s"($id, '$words', $ts, 'w$ts')"
        }.mkString(", ")
        def leaf(): (Expression, String) = {
          val word = vocab(random.nextInt(vocab.size))
          val query = Seq.fill(1 + random.nextInt(3))(vocab(random.nextInt(vocab.size))).mkString(" ")
          random.nextInt(6) match {
            case 0 => (HudiHasToken(msg, Literal(word)), s"hudi_has_token(msg, '$word')")
            case 1 => (HudiHasAllTokens(msg, Literal(query)), s"hudi_has_all_tokens(msg, '$query')")
            case 2 => (HudiHasAnyTokens(msg, Literal(query)), s"hudi_has_any_tokens(msg, '$query')")
            case 3 =>
              val prefix = word.take(1 + random.nextInt(word.length))
              (HudiHasTokenPrefix(msg, Literal(prefix)), s"hudi_has_token_prefix(msg, '$prefix')")
            case 4 =>
              val phrase = if (random.nextBoolean()) phraseWords() else query
              (HudiHasPhrase(msg, Literal(phrase)), s"hudi_has_phrase(msg, '$phrase')")
            case _ =>
              val bound = random.nextInt(25)
              (GreaterThan(ts, Literal(bound.toLong)), s"ts > $bound")
          }
        }
        def predicate(depth: Int): (Expression, String) =
          if (depth == 0 || random.nextInt(3) == 0) {
            leaf()
          } else {
            random.nextInt(3) match {
              case 0 =>
                val ((l, ls), (r, rs)) = (predicate(depth - 1), predicate(depth - 1))
                (And(l, r), s"($ls) and ($rs)")
              case 1 =>
                val ((l, ls), (r, rs)) = (predicate(depth - 1), predicate(depth - 1))
                (Or(l, r), s"($ls) or ($rs)")
              case _ =>
                val (c, cs) = predicate(depth - 1)
                (Not(c), s"not ($cs)")
            }
          }

        spark.sql(s"insert into $tableName values ${rows(1 to 8, 1)}")
        spark.sql(s"create index idx_msg on $tableName using full_text(msg) options (dense_ratio = '$denseRatio')")
        var nextId = 9
        var checks = 0
        (1 to 20).foreach { step =>
          random.nextInt(4) match {
            case 0 =>
              val values = if (random.nextBoolean()) rows(nextId until nextId + 3, step) else phraseFileRows(nextId until nextId + 3, step)
              spark.sql(s"insert into $tableName values $values")
              nextId += 3
            case 1 =>
              val id = 1 + random.nextInt(nextId - 1)
              spark.sql(s"update $tableName set msg = ${text()}, ts = ${step + 100} where id = $id")
            case 2 =>
              spark.sql(s"delete from $tableName where id = ${1 + random.nextInt(nextId - 1)}")
            case _ =>
              spark.sql(s"call run_clustering(table => '$tableName', op => 'scheduleandexecute')")
          }
          (1 to 10).foreach { _ =>
            val (expr, sqlPred) = predicate(3)
            val needed = filesWithMatches(basePath, sqlPred)
            val kept = filesRead(basePath, expr)
            assert(needed.subsetOf(kept), s"dense_ratio $denseRatio, step $step, $sqlPred: pruned a file with a match; needed=$needed kept=$kept")
            checks += 1
          }
        }
        assertResult(200)(checks)
      }
    }
  }

  test("Test full-text index prunes a query string given as a constant expression") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      createTable(tableName, basePath)
      spark.sql(s"insert into $tableName values (1, 'disk full', 1, 'a'), (2, 'quota ok', 1, 'a')")
      spark.sql(s"insert into $tableName values (3, 'disk quota', 1, 'b')")
      spark.sql(s"create index idx_msg on $tableName using full_text(msg) options (dense_ratio = '1.0')")
      assertResult(1)(scanFiles(basePath, "hudi_has_token(msg, 'full')"))
      assertResult(1)(scanFiles(basePath, "hudi_has_token(msg, concat('fu', 'll'))"))
      assertResult(Set(filesWithMatches(basePath, "p = 'a'").head))(
        filesRead(basePath, HudiHasToken(msg, Concat(Seq(Literal("fu"), Literal("ll"))))))
    }
  }

  test("Test full-text index prunes AND, OR and NOT of predicates by row") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      createTable(tableName, basePath)
      spark.sql(s"insert into $tableName values (1, 'disk full', 1, 'a'), (2, 'quota ok', 1, 'a')")
      spark.sql(s"insert into $tableName values (3, 'disk quota', 1, 'b')")
      spark.sql(s"insert into $tableName values (4, 'disk', 1, 'c'), (5, 'disk error', 1, 'c')")
      spark.sql(s"create index idx_msg on $tableName using full_text(msg) options (dense_ratio = '1.0')")

      val all = filesRead(basePath, Literal.TrueLiteral, skipping = false)
      val Seq(fileA, fileB, fileC) = Seq("a", "b", "c").map(p => filesWithMatches(basePath, s"p = '$p'").head)
      val disk = HudiHasToken(msg, Literal("disk"))
      val quota = HudiHasToken(msg, Literal("quota"))
      val tsPositive = GreaterThan(ts, Literal(0L))

      // File a holds 'disk' and 'quota', never in one row.
      assertResult(Set(fileB))(filesRead(basePath, And(disk, quota)))
      assertResult(Set(fileB))(filesRead(basePath, And(HudiHasTokenPrefix(msg, Literal("dis")), quota)))
      assertResult(all)(filesRead(basePath, And(disk, tsPositive)))
      assertResult(Set(fileA))(filesRead(basePath, Or(HudiHasToken(msg, Literal("full")), HudiHasToken(msg, Literal("absent")))))
      assertResult(all)(filesRead(basePath, Or(HudiHasToken(msg, Literal("full")), tsPositive)))
      // Every row of b and c holds 'disk'; only b has a row with both tokens, and it is its only row.
      assertResult(Set(fileA))(filesRead(basePath, Not(disk)))
      assertResult(Set(fileA, fileC))(filesRead(basePath, Not(HudiHasAllTokens(msg, Literal("disk quota")))))
      assertResult(Set(fileA, fileC))(filesRead(basePath, And(Not(quota), disk)))
      assertResult(all)(filesRead(basePath, Not(tsPositive)))

      Seq("hudi_has_token(msg, 'disk') and hudi_has_token(msg, 'quota')",
        "hudi_has_token(msg, 'full') or hudi_has_token(msg, 'absent')",
        "not hudi_has_token(msg, 'disk')",
        "not hudi_has_all_tokens(msg, 'disk quota')",
        "not hudi_has_token(msg, 'quota') and hudi_has_token(msg, 'disk')").foreach { predicate =>
        assertResult(idsRead(basePath, predicate, skipping = false), predicate)(idsRead(basePath, predicate))
      }
      assertResult(Seq(2))(idsRead(basePath, "not hudi_has_token(msg, 'disk')"))
      assertResult(1)(scanFiles(basePath, "not hudi_has_token(msg, 'disk')"))
      assertResult(1)(scanFiles(basePath, "hudi_has_token(msg, 'disk') and hudi_has_token(msg, 'quota')"))
    }
  }

  test("Test full-text index prunes token prefix and phrase predicates") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}/$tableName"
      createTable(tableName, basePath)
      val longToken = "x" * 70
      spark.sql(s"insert into $tableName values (1, 'disk full', 1, 'a'), (2, 'quota ok', 1, 'a')")
      spark.sql(s"insert into $tableName values (3, 'disk quota exceeded', 1, 'b')")
      spark.sql(s"insert into $tableName values (4, 'quota disk', 1, 'c'), (5, 'diskette', 1, 'c')")
      spark.sql(s"insert into $tableName values (6, 'disk $longToken quota', 1, 'd')")
      spark.sql(s"create index idx_msg on $tableName using full_text(msg) options (dense_ratio = '1.0')")

      val all = filesRead(basePath, Literal.TrueLiteral, skipping = false)
      val Seq(fileA, fileB, fileC, fileD) = Seq("a", "b", "c", "d").map(p => filesWithMatches(basePath, s"p = '$p'").head)

      assertResult(Set(fileC))(filesRead(basePath, HudiHasTokenPrefix(msg, Literal("diske"))))
      assertResult(Set(fileB))(filesRead(basePath, HudiHasTokenPrefix(msg, Literal("ex"))))
      assertResult(Set(fileA))(filesRead(basePath, HudiHasTokenPrefix(msg, Literal("FUL"))))
      assertResult(Set.empty)(filesRead(basePath, HudiHasTokenPrefix(msg, Literal("zz"))))
      // A prefix of two tokens matches nothing and is not used for pruning.
      assertResult(all)(filesRead(basePath, HudiHasTokenPrefix(msg, Literal("di sk"))))
      assertResult(Seq(1, 3, 4, 5, 6))(idsRead(basePath, "hudi_has_token_prefix(msg, 'disk')"))
      assertResult(Seq.empty)(idsRead(basePath, "hudi_has_token_prefix(msg, 'di sk')"))

      // Pruning needs every phrase token in one row; order and adjacency are checked on the rows read.
      assertResult(Set(fileB, fileC, fileD))(filesRead(basePath, HudiHasPhrase(msg, Literal("disk quota"))))
      assertResult(Seq(3))(idsRead(basePath, "hudi_has_phrase(msg, 'disk quota')"))
      assertResult(Seq(3))(idsRead(basePath, "hudi_has_phrase(msg, 'Disk, QUOTA')"))
      assertResult(Seq(4))(idsRead(basePath, "hudi_has_phrase(msg, 'quota disk')"))
      // A token too long for the index still separates the phrase in the row, and still has to match.
      assertResult(Seq(6))(idsRead(basePath, s"hudi_has_phrase(msg, 'disk $longToken')"))
      // NOT of a phrase: rows holding every phrase token, but not as the phrase, still satisfy it.
      assertResult(Seq(1, 2, 4, 5, 6))(idsRead(basePath, "not hudi_has_phrase(msg, 'disk quota')", skipping = false))
      Seq("hudi_has_token_prefix(msg, 'disk')", "hudi_has_phrase(msg, 'disk quota')", s"hudi_has_phrase(msg, 'disk $longToken')",
        "not hudi_has_phrase(msg, 'disk quota')", "not hudi_has_phrase(msg, 'quota disk')", "not hudi_has_phrase(msg, 'disk disk')",
        s"not hudi_has_phrase(msg, 'disk $longToken')", "not hudi_has_phrase(msg, 'disk')")
        .foreach(predicate => assertResult(idsRead(basePath, predicate, skipping = false), predicate)(idsRead(basePath, predicate)))
    }
  }
}
