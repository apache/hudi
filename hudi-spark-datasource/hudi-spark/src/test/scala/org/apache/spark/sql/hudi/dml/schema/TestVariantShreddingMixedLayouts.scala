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

package org.apache.spark.sql.hudi.dml.schema

import org.apache.hudi.{DataSourceReadOptions, HoodieSparkUtils}
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.testutils.DataSourceTestUtils

import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

/**
 * Mixed-layout variant shredding matrix: files with DIFFERENT typed_value layouts in one table,
 * shredded/unshredded splits between base and log files, and rows inside one file that fell back
 * to the residual value column, driven through compaction, clustering, merges and every Spark
 * read mode. Complements [[TestVariantDataType]], whose shredded tests force ONE layout per
 * table.
 *
 * Layouts are toggled per commit or table service through session confs (session hoodie.* confs
 * override tblproperties for SQL DML and for the run_compaction/run_clustering procedures alike).
 * Legs that need #18961's per-file shredding-schema inference substitute a forced stand-in
 * schema via [[inferredOr]] when no inferrer is on the classpath, so the mixed-layout shape is
 * preserved on every profile.
 *
 * Deliberately not covered here:
 * - Custom payloads: FileGroupRecordBuffer.getProjectedTransformer short-circuits the variant
 *   log-block projection when payload classes are present (#18674), so that is a real,
 *   explicitly UNTESTED variant branch; PartialUpdateMode and the CUSTOM merge mode are
 *   likewise unreached (only EVENT_TIME/COMMIT_TIME ordering is swept).
 * - Multi-writer OCC: conflict resolution is key/instant based and never inspects layouts; the
 *   mixed-file outcomes it can produce are the same ones pinned here.
 */
class TestVariantShreddingMixedLayouts extends HoodieSparkSqlTestBase with VariantShreddingTestSupport {

  import VariantShreddingTestSupport._
  import VariantShreddingTestSupport.VariantShape._

  private val SPARK_4_1_GATE = "Shredded variant read-back requires Spark 4.1 or higher"

  /** One insert commit per layout; returns the completed instant of each commit, in order. */
  private def seedMixedLayoutTable(tableName: String,
                                   tablePath: String,
                                   layouts: Seq[(WriteLayout, Seq[(Range, VariantShape)])]): Seq[String] = {
    layouts.map { case (layout, segments) =>
      withWriteLayout(layout) {
        spark.sql(s"insert into $tableName ${variantSourceSql(segments)}")
      }
      latestCompletedInstant(tablePath)
    }
  }

  /** scheduleAndExecute compaction; the options carry the NUM_COMMITS trigger so one delta commit suffices. */
  private def runCompaction(tableName: String): Unit = {
    spark.sql(s"call run_compaction(op => 'scheduleandexecute', table => '$tableName', " +
      "options => 'hoodie.compact.inline.max.delta.commits=1')")
  }

  private def runClustering(tableName: String, rowWriter: Boolean): Unit = {
    spark.sql(s"call run_clustering(table => '$tableName', " +
      s"options => 'hoodie.datasource.write.row.writer.enable=$rowWriter')")
  }

  // -----------------------------------------------------------------------------------------------
  // A. Mixed records inside one file
  // -----------------------------------------------------------------------------------------------

  test("Forced shredding: non-matching rows fall back to the residual in the same file") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath
      val leg = s"same-file mix, $tableName"
      createVariantTable(tableName, tablePath, "cow")

      // One insert, one file: rows 0-9 match the forced schema exactly; 10-14 conflict on the
      // type of a (string into a bigint slot -> per-field residual); 15-19 carry disjoint keys
      // (root residual); 20-22 are root scalars and 23 a JSON null (no object typed_value);
      // 24 is a SQL NULL variant.
      val segments = Seq(
        (0 until 10, ObjA),
        (10 until 15, ObjAConflict),
        (15 until 20, ObjB),
        (20 until 23, RootScalar),
        (23 until 24, JsonNull),
        (24 until 25, SqlNull))
      withWriteLayout(Forced("a bigint, b string")) {
        spark.sql(s"insert into $tableName ${variantSourceSql(segments)}")
      }

      val files = listDataParquetFiles(tablePath)
      assert(files.size == 1, s"[$leg] expected exactly one data file, got $files")
      assertVariantLayout(tablePath, shredded = true, leg)

      // Physical placement per the shredding spec: objects always materialize typed_value;
      // unmatched FIELDS go to the per-field residual, unmatched KEYS to the root residual;
      // non-objects (scalars, arrays, JSON null) live entirely in the root residual.
      val stats = inspectVariantRows(files.head)
      assert(stats.rows == 25, s"[$leg] rows: $stats")
      assert(stats.nullVariants == 1, s"[$leg] null variants: $stats")
      assert(stats.rootTyped == 20, s"[$leg] object rows with typed_value: $stats")
      assert(stats.rootResidual == 9, s"[$leg] root residual rows (ObjB 5 + scalars 3 + json null 1): $stats")
      assert(stats.fieldTyped("a") == 10, s"[$leg] typed a: $stats")
      assert(stats.fieldResidual("a") == 5, s"[$leg] residual a (type conflict): $stats")
      assert(stats.fieldTyped("b") == 15, s"[$leg] typed b: $stats")

      assertVariantSegments(tableName, leg, Seq(("v", segments)))

      // Update rows served from the typed slot and from the residual: the AVRO record type
      // reconstructs both through HoodieVariantReconstruction, SPARK natively.
      withWriteLayout(Forced("a bigint, b string")) {
        spark.sql(s"""update $tableName set v = parse_json('{"a":100,"b":"bu"}'), ts = 1001 where id = 20""")
        spark.sql(s"""update $tableName set v = parse_json('{"a":101,"b":"bv"}'), ts = 1001 where id = 5""")
      }
      checkAnswer(s"select id, cast(v as string), ts from $tableName where id in (5, 12, 20) order by id")(
        Seq(5, """{"a":101,"b":"bv"}""", 1001),
        Seq(12, """{"a":"s12","b":"b12"}""", 1000),
        Seq(20, """{"a":100,"b":"bu"}""", 1001)
      )
      assertVariantLayout(tablePath, shredded = true, leg)
    })
  }

  // -----------------------------------------------------------------------------------------------
  // B. Mixed files inside one table
  // -----------------------------------------------------------------------------------------------

  test("Each commit keeps its own layout; snapshot, time travel, incremental and RO read them all") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    // Read-mode test: layouts are writer-side and every layout is written identically by both
    // record types, so the sweep would only re-run the same reads. SPARK pinned.
    withRecordType(Seq(HoodieRecordType.SPARK))(withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath
      val leg = s"mixed-files, $tableName"
      createVariantTable(tableName, tablePath, "cow", props = Seq(NEW_FILE_GROUP_PER_COMMIT))

      // Four commits, four layouts, one file each (small.file.limit=0 keeps every commit in its
      // own file group). The last commit infers when an inferrer is present; the forced stand-in
      // yields the same {c, d} typed_value, so the expectations below hold either way.
      val instants = seedMixedLayoutTable(tableName, tablePath, Seq(
        (Unshredded, Seq((0 until 2, ObjA))),
        (Forced("a bigint, b string"), Seq((2 until 4, ObjA))),
        (Forced("b string"), Seq((4 until 6, ObjA))),
        (inferredOr(Forced("c bigint, d boolean")), Seq((6 until 8, ObjB)))))

      assertLayoutsByInstant(baseLayouts(tablePath), leg)(
        instants(0) -> None,
        instants(1) -> Some(Seq("a", "b")),
        instants(2) -> Some(Seq("b")),
        instants(3) -> Some(Seq("c", "d")))

      // Snapshot reads every layout.
      assertVariantSegments(tableName, leg, Seq(("v", Seq(
        (0 until 6, ObjA), (6 until 8, ObjB)))))

      // Time travel at the second commit sees only the first two layouts.
      checkAnswer(s"select id, cast(v as string) from $tableName timestamp as of '${instants(1)}' order by id")(
        Seq(0, """{"a":0,"b":"b0"}"""),
        Seq(1, """{"a":1,"b":"b1"}"""),
        Seq(2, """{"a":2,"b":"b2"}"""),
        Seq(3, """{"a":3,"b":"b3"}""")
      )

      // Incremental over the full range returns the latest state of all eight keys, values
      // intact (a count alone would pass even if v reconstructed as all-null).
      val incRows = spark.read.format("hudi")
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.START_COMMIT.key, "000")
        .load(tablePath)
        .selectExpr("id", "cast(v as string)")
        .orderBy("id")
        .collect()
      assert(incRows.length == 8, s"[$leg] incremental over the full range should see all rows")
      incRows.foreach { row =>
        val id = row.getInt(0)
        val expected = if (id < 6) s"""{"a":$id,"b":"b$id"}""" else s"""{"c":$id,"d":true}"""
        assert(row.getString(1) == expected,
          s"[$leg] incremental id=$id: expected $expected, got ${row.getString(1)}")
      }

      // Read-optimized on COW equals the snapshot, values intact.
      checkAnswer(s"select id, cast(v as string) from hudi_query('$tableName', 'read_optimized') " +
        "where id in (0, 6) order by id")(
        Seq(0, """{"a":0,"b":"b0"}"""),
        Seq(6, """{"c":6,"d":true}""")
      )
    })
  }

  test("Small-file bin-pack rewrites the file under the layout of the incoming commit") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath
      val leg = s"bin-pack layout flip, $tableName"
      // Default small.file.limit on purpose: each insert bin-packs into the first file group
      // and rewrites it (HoodieConcatHandle -> HoodieMergeHelper on the AVRO record type).
      // The value round-trip of that merge is owned by TestVariantDataType's small-file test;
      // this one exists for the per-instant LAYOUT pin below.
      createVariantTable(tableName, tablePath, "cow")

      withWriteLayout(Forced("a bigint, b string")) {
        spark.sql(s"""insert into $tableName values (1, parse_json('{"a":1,"b":"b1"}'), 1000)""")
      }
      val instant1 = latestCompletedInstant(tablePath)
      withWriteLayout(Unshredded) {
        spark.sql(s"""insert into $tableName values (2, parse_json('{"a":2,"b":"b2"}'), 1000)""")
      }
      val instant2 = latestCompletedInstant(tablePath)
      withWriteLayout(Forced("a bigint")) {
        spark.sql(s"""insert into $tableName values (3, parse_json('{"a":3,"b":"b3"}'), 1000)""")
      }
      val instant3 = latestCompletedInstant(tablePath)

      assertSingleFileGroup(tablePath, leg)
      // The rewrite re-derives the layout from the CURRENT write config; the input file's
      // layout is never consulted. Older file versions keep their own layouts.
      assertLayoutsByInstant(baseLayouts(tablePath), leg)(
        instant1 -> Some(Seq("a", "b")),
        instant2 -> None,
        instant3 -> Some(Seq("a")))

      checkAnswer(s"select id, cast(v as string), ts from $tableName order by id")(
        Seq(1, """{"a":1,"b":"b1"}""", 1000),
        Seq(2, """{"a":2,"b":"b2"}""", 1000),
        Seq(3, """{"a":3,"b":"b3"}""", 1000)
      )
    })
  }

  // -----------------------------------------------------------------------------------------------
  // C. MOR compaction over base/log layout splits
  // -----------------------------------------------------------------------------------------------

  test("MOR compaction merges logs of three layouts and re-derives the base layout per service run") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath
      val leg = s"compaction layout split, $tableName"
      // INMEMORY sends MOR inserts to log files; compaction runs via the procedure so each run
      // can happen under its own layout confs.
      createVariantTable(tableName, tablePath, "mor",
        props = Seq("hoodie.index.type = 'INMEMORY'", "hoodie.compact.inline = 'false'"))
      val layout3 = inferredOr(Forced("c bigint, d boolean"))

      withWriteLayout(Forced("a bigint, b string")) {
        spark.sql(s"""insert into $tableName values (1, parse_json('{"a":1,"b":"b1"}'), 1000)""")
      }
      val instant1 = latestCompletedInstant(tablePath)
      withWriteLayout(Unshredded) {
        spark.sql(s"""insert into $tableName values (2, parse_json('{"a":2,"b":"b2"}'), 1000), """ +
          """(3, parse_json('{"a":3,"b":"b3"}'), 1000), (4, parse_json('{"a":4,"b":"b4"}'), 1000)""")
      }
      val instant2 = latestCompletedInstant(tablePath)
      withWriteLayout(layout3) {
        spark.sql(s"""insert into $tableName values (5, parse_json('{"c":5,"d":true}'), 1000), """ +
          """(6, parse_json('{"c":6,"d":true}'), 1000)""")
      }
      val instant3 = latestCompletedInstant(tablePath)

      assertResult(true)(DataSourceTestUtils.isLogFileOnly(tablePath))
      // On the default table version the data logs are native parquet, each with the layout of
      // its own commit. (The SPARK withRecordType leg sets the parquet log block format, the
      // AVRO leg avro blocks, but write version >= 10 writes native log FILES either way.)
      assertLayoutsByInstant(nativeLogLayouts(tablePath), leg)(
        instant1 -> Some(Seq("a", "b")),
        instant2 -> None,
        instant3 -> Some(Seq("c", "d")))

      // Merge-on-read snapshot over the three-layout split, before any base file exists.
      checkAnswer(s"select id, cast(v as string) from $tableName order by id")(
        Seq(1, """{"a":1,"b":"b1"}"""),
        Seq(2, """{"a":2,"b":"b2"}"""),
        Seq(3, """{"a":3,"b":"b3"}"""),
        Seq(4, """{"a":4,"b":"b4"}"""),
        Seq(5, """{"c":5,"d":true}"""),
        Seq(6, """{"c":6,"d":true}""")
      )

      // Compaction 1 under layout3: reads all three log layouts, writes the base under layout3.
      withWriteLayout(layout3) {
        runCompaction(tableName)
      }
      assertResult(false)(DataSourceTestUtils.isLogFileOnly(tablePath))
      assertCompactionCount(tablePath, 1, leg)
      val base1 = baseLayouts(tablePath)
      assert(base1.nonEmpty && base1.forall(_.isShredded),
        s"[$leg] compacted base must be shredded under $layout3: $base1")
      if (inferrerPresent) {
        // 6 rows: a and b on 4 (66 percent), c and d on 2 (33 percent) - all clear the 10
        // percent inference bar.
        base1.foreach(l => assert(l.typedFields.toSet == Set("a", "b", "c", "d"),
          s"[$leg] inferred typed_value should carry all four keys: ${l.typedFields}"))
      } else {
        base1.foreach(l => assert(l.typedFields.toSet == Set("c", "d"),
          s"[$leg] forced typed_value should carry c, d: ${l.typedFields}"))
      }
      checkAnswer(s"select id, cast(v as string) from $tableName where id in (1, 5) order by id")(
        Seq(1, """{"a":1,"b":"b1"}"""),
        Seq(5, """{"c":5,"d":true}""")
      )
      checkAnswer(s"select id, cast(v as string) from hudi_query('$tableName', 'read_optimized') " +
        "where id in (1, 5) order by id")(
        Seq(1, """{"a":1,"b":"b1"}"""),
        Seq(5, """{"c":5,"d":true}""")
      )

      // Round 2: updates under two further layouts, compaction under Unshredded. The service
      // reads a shredded base plus mixed logs and must strip typed_value on the way out.
      withWriteLayout(Forced("a bigint")) {
        spark.sql(s"""update $tableName set v = parse_json('{"a":22,"b":"b22"}'), ts = 1001 where id = 2""")
      }
      withWriteLayout(Unshredded) {
        spark.sql(s"""update $tableName set v = parse_json('{"a":33,"b":"b33"}'), ts = 1001 where id = 3""")
      }
      // A delete block (no data column) between the differently-shredded logs: the merged read
      // and the following compaction must step over it without a layout to anchor on.
      withWriteLayout(Forced("a bigint")) {
        spark.sql(s"delete from $tableName where id = 6")
      }
      // Merge-on-read over shredded base + {a}-shredded log + unshredded log + delete block.
      checkAnswer(s"select id, cast(v as string) from $tableName where id in (2, 3, 5) order by id")(
        Seq(2, """{"a":22,"b":"b22"}"""),
        Seq(3, """{"a":33,"b":"b33"}"""),
        Seq(5, """{"c":5,"d":true}""")
      )
      withWriteLayout(Unshredded) {
        runCompaction(tableName)
      }
      assertCompactionCount(tablePath, 2, leg)
      val compact2Instant = latestCompletedInstant(tablePath)
      val base2 = baseLayouts(tablePath).filter(_.instantTime == compact2Instant)
      assert(base2.nonEmpty && base2.forall(!_.isShredded),
        s"[$leg] compaction under Unshredded must write an unshredded base: $base2")
      checkAnswer(s"select id, cast(v as string) from $tableName where id in (2, 3) order by id")(
        Seq(2, """{"a":22,"b":"b22"}"""),
        Seq(3, """{"a":33,"b":"b33"}""")
      )

      // Round 3: compaction under layout3 again, this time reading an UNSHREDDED base plus a
      // shredded log.
      withWriteLayout(layout3) {
        spark.sql(s"""update $tableName set v = parse_json('{"a":44,"b":"b44"}'), ts = 1001 where id = 4""")
        runCompaction(tableName)
      }
      assertCompactionCount(tablePath, 3, leg)
      val compact3Instant = latestCompletedInstant(tablePath)
      val base3 = baseLayouts(tablePath).filter(_.instantTime == compact3Instant)
      assert(base3.nonEmpty && base3.forall(_.isShredded),
        s"[$leg] compaction under $layout3 must write a shredded base again: $base3")

      checkAnswer(s"select id, cast(v as string) from $tableName order by id")(
        Seq(1, """{"a":1,"b":"b1"}"""),
        Seq(2, """{"a":22,"b":"b22"}"""),
        Seq(3, """{"a":33,"b":"b33"}"""),
        Seq(4, """{"a":44,"b":"b44"}"""),
        Seq(5, """{"c":5,"d":true}""")
      )
      // Incremental over the full range sees the latest value of every LIVE key (id 6 deleted),
      // values intact - a bare count would pass with v all-null.
      val incRows = spark.read.format("hudi")
        .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.START_COMMIT.key, "000")
        .load(tablePath)
        .selectExpr("id", "cast(v as string)")
        .orderBy("id")
        .collect()
      assert(incRows.map(r => (r.getInt(0), r.getString(1))).toSeq == Seq(
        (1, """{"a":1,"b":"b1"}"""),
        (2, """{"a":22,"b":"b22"}"""),
        (3, """{"a":33,"b":"b33"}"""),
        (4, """{"a":44,"b":"b44"}"""),
        (5, """{"c":5,"d":true}""")
      ), s"[$leg] incremental over the full range, got: ${incRows.mkString(", ")}")
    })
  }

  test("Table version 9 legacy log blocks stay unshredded and compact onto a shredded base") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath
      val leg = s"table version 9, $tableName"
      createVariantTable(tableName, tablePath, "mor",
        props = Seq(
          "hoodie.write.table.version = '9'",
          "hoodie.index.type = 'INMEMORY'",
          "hoodie.compact.inline = 'false'"))
      val layout = inferredOr(Forced("key string"))

      withWriteLayout(layout) {
        spark.sql(s"""insert into $tableName values (1, parse_json('{"key":"value1"}'), 1000)""")
        spark.sql(s"""insert into $tableName values (2, parse_json('{"key":"value2"}'), 1000)""")
      }
      assertResult(true)(DataSourceTestUtils.isLogFileOnly(tablePath))
      // Write version 9 writes the legacy inline log format (avro blocks on the AVRO record
      // type leg, inline parquet data blocks on the SPARK leg), never native parquet log files;
      // neither inline form shreds, so the shredded layout materializes only at compaction.
      assert(nativeLogLayouts(tablePath).isEmpty,
        s"[$leg] table version 9 must not write native parquet log files")

      withWriteLayout(layout) {
        runCompaction(tableName)
      }
      assertResult(false)(DataSourceTestUtils.isLogFileOnly(tablePath))
      val base1 = baseLayouts(tablePath)
      assert(base1.nonEmpty && base1.forall(_.isShredded),
        s"[$leg] compacted base must be shredded: $base1")
      base1.foreach(l => assert(l.typedFields == Seq("key"),
        s"[$leg] typed_value should carry key: ${l.typedFields}"))

      // Legacy log over the shredded base, then a second compaction reads base + legacy log.
      withWriteLayout(layout) {
        spark.sql(s"""update $tableName set v = parse_json('{"key":"v1-updated"}'), ts = 1001 where id = 1""")
      }
      checkAnswer(s"select id, cast(v as string) from $tableName order by id")(
        Seq(1, """{"key":"v1-updated"}"""),
        Seq(2, """{"key":"value2"}""")
      )
      withWriteLayout(layout) {
        runCompaction(tableName)
      }
      checkAnswer(s"select id, cast(v as string) from $tableName order by id")(
        Seq(1, """{"key":"v1-updated"}"""),
        Seq(2, """{"key":"value2"}""")
      )
    })
  }

  // -----------------------------------------------------------------------------------------------
  // D. Clustering over heterogeneous inputs
  // -----------------------------------------------------------------------------------------------

  test("Clustering rewrites heterogeneous files into the configured layout") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    // The row-writer path is record-type independent; the RDD path writes through the
    // record-type file writer factories, so it sweeps both.
    Seq(true, false).foreach { rowWriter =>
      val recordTypes = if (rowWriter) {
        Seq(HoodieRecordType.SPARK)
      } else {
        Seq(HoodieRecordType.AVRO, HoodieRecordType.SPARK)
      }
      Seq(Unshredded, inferredOr(Forced("a bigint"))).foreach { outLayout =>
        withRecordType(recordTypes)(withTempDir { tmp =>
          val tableName = generateTableName
          val tablePath = tmp.getCanonicalPath
          val leg = s"clustering rowWriter=$rowWriter out=$outLayout, $tableName"
          createVariantTable(tableName, tablePath, "cow", props = Seq(NEW_FILE_GROUP_PER_COMMIT))

          val instants = seedMixedLayoutTable(tableName, tablePath, Seq(
            (Forced("a bigint, b string"), Seq((0 until 2, ObjA))),
            (Unshredded, Seq((2 until 4, ObjA))),
            (inferredOr(Forced("c bigint, d boolean")), Seq((4 until 6, ObjB)))))

          withWriteLayout(outLayout) {
            runClustering(tableName, rowWriter)
          }
          val clusteringInstant = completedClusteringInstant(tablePath, leg)
          val outFiles = baseLayouts(tablePath).filter(_.instantTime == clusteringInstant)
          assert(outFiles.nonEmpty, s"[$leg] clustering should have written base files")
          outLayout match {
            case Unshredded =>
              outFiles.foreach(l => assert(!l.isShredded,
                s"[$leg] clustering under Unshredded must write unshredded output: ${l.path}"))
            case Forced(_) =>
              outFiles.foreach(l => assert(l.typedFields == Seq("a"),
                s"[$leg] forced output typed_value should be {a}: ${l.typedFields}"))
            case Inferred =>
              // 6 rows: a, b on 4 and c, d on 2 - all clear the 10 percent bar.
              outFiles.foreach(l => assert(l.typedFields.toSet == Set("a", "b", "c", "d"),
                s"[$leg] inferred output typed_value should carry all keys: ${l.typedFields}"))
          }

          // Values survive the rewrite; the pre-clustering slice stays readable via time travel.
          assertVariantSegments(tableName, leg, Seq(("v", Seq(
            (0 until 4, ObjA), (4 until 6, ObjB)))))
          checkAnswer(s"select id, cast(v as string) from $tableName " +
            s"timestamp as of '${instants(2)}' where id in (0, 4) order by id")(
            Seq(0, """{"a":0,"b":"b0"}"""),
            Seq(4, """{"c":4,"d":true}""")
          )
        })
      }
    }
  }

  test("Clustering sort on a variant column is rejected with a clear error") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    withRecordType(Seq(HoodieRecordType.SPARK))(withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath
      createVariantTable(tableName, tablePath, "cow")
      withWriteLayout(Forced("a bigint")) {
        spark.sql(s"""insert into $tableName values (1, parse_json('{"a":1}'), 1000)""")
      }

      // The procedure's order parameter is validated up front...
      checkNestedExceptionContains(
        s"call run_clustering(table => '$tableName', order => 'v')")(
        "Sorting by column 'v'")
      // ...and the config-driven sort columns are validated by the execution strategy and the
      // partitioner constructors (SortUtils.validateSortableColumns), so the inline/async paths
      // get the same error instead of an AnalysisException (row partitioner) or
      // ClassCastException (RDD partitioner) deep in the job.
      checkNestedExceptionContains(
        s"call run_clustering(table => '$tableName', " +
          "options => 'hoodie.clustering.plan.strategy.sort.columns=v')")(
        "Sorting by column 'v'")
    })
  }

  test("MOR clustering folds log files of another layout into the rewritten base") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    Seq(true, false).foreach { rowWriter =>
      val recordTypes = if (rowWriter) {
        Seq(HoodieRecordType.SPARK)
      } else {
        Seq(HoodieRecordType.AVRO, HoodieRecordType.SPARK)
      }
      withRecordType(recordTypes)(withTempDir { tmp =>
        val tableName = generateTableName
        val tablePath = tmp.getCanonicalPath
        val leg = s"mor clustering rowWriter=$rowWriter, $tableName"
        // No INMEMORY index: the first insert creates a base file, the update goes to a log.
        createVariantTable(tableName, tablePath, "mor",
          props = Seq("hoodie.compact.inline = 'false'"))
        val outLayout = inferredOr(Forced("c bigint"))

        withWriteLayout(Forced("a bigint, b string")) {
          spark.sql(s"""insert into $tableName values (1, parse_json('{"a":1,"b":"b1"}'), 1000), """ +
            """(2, parse_json('{"a":2,"b":"b2"}'), 1000)""")
        }
        withWriteLayout(Unshredded) {
          spark.sql(s"""update $tableName set v = parse_json('{"a":10,"b":"b10"}'), ts = 1001 where id = 1""")
        }
        // The slice going into clustering: a shredded base plus an unshredded native log.
        val preBase = baseLayouts(tablePath)
        assert(preBase.nonEmpty && preBase.forall(_.isShredded),
          s"[$leg] pre-clustering base must be shredded: $preBase")
        val preLogs = nativeLogLayouts(tablePath)
        assert(preLogs.nonEmpty && preLogs.forall(!_.isShredded),
          s"[$leg] pre-clustering log must be unshredded: $preLogs")

        withWriteLayout(outLayout) {
          runClustering(tableName, rowWriter)
        }
        val clusteringInstant = completedClusteringInstant(tablePath, leg)
        val outFiles = baseLayouts(tablePath).filter(_.instantTime == clusteringInstant)
        assert(outFiles.nonEmpty, s"[$leg] clustering should have written base files")
        outFiles.foreach(l => assert(l.isShredded,
          s"[$leg] clustering under $outLayout must write shredded output: ${l.path}"))

        // The clustered base carries the merged (updated) row.
        checkAnswer(s"select id, cast(v as string) from $tableName order by id")(
          Seq(1, """{"a":10,"b":"b10"}"""),
          Seq(2, """{"a":2,"b":"b2"}""")
        )
      })
    }
  }

  // -----------------------------------------------------------------------------------------------
  // E. Read modes over mixed layouts
  // -----------------------------------------------------------------------------------------------

  test("variant_get filters and projections resolve per file across mixed layouts") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    // Read-mode test; SPARK pinned (see the mixed-files test above).
    Seq("true", "false").foreach { pushIntoScan =>
      withRecordType(Seq(HoodieRecordType.SPARK))(withTempDir { tmp =>
        withSQLConf("spark.sql.variant.pushVariantIntoScan" -> pushIntoScan) {
          val tableName = generateTableName
          val tablePath = tmp.getCanonicalPath
          val leg = s"cow pushVariantIntoScan=$pushIntoScan, $tableName"
          createVariantTable(tableName, tablePath, "cow", props = Seq(NEW_FILE_GROUP_PER_COMMIT))

          // $.a is typed in file 1, residual (unshredded) in file 2, a type-conflicted residual
          // in file 3 and absent in file 4.
          seedMixedLayoutTable(tableName, tablePath, Seq(
            (Forced("a bigint"), Seq((0 until 10, ObjA))),
            (Unshredded, Seq((10 until 20, ObjA))),
            (Forced("a bigint"), Seq((20 until 30, ObjAConflict))),
            (inferredOr(Forced("c bigint, d boolean")), Seq((30 until 40, ObjB)))))

          // Typed and residual rows answer alike; the string a declines the cast, the missing
          // a returns null.
          val aValues = spark.sql(
            s"select id, try_variant_get(v, '$$.a', 'bigint') from $tableName order by id").collect()
          assert(aValues.length == 40, s"[$leg] row count")
          aValues.foreach { row =>
            val id = row.getInt(0)
            val expected: Any = if (id < 20) id.toLong else null
            val actual = if (row.isNullAt(1)) null else row.getLong(1)
            assert(actual == expected, s"[$leg] id=$id: expected $expected, got $actual")
          }

          checkAnswer(
            s"select count(*) from $tableName where try_variant_get(v, '$$.a', 'bigint') > 5")(Seq(14))
          checkAnswer(
            s"select id from $tableName where variant_get(v, '$$.b', 'string') = 'b25'")(Seq(25))
          checkAnswer(
            s"select count(*) from $tableName where try_variant_get(v, '$$.d', 'boolean')")(Seq(10))
          checkAnswer(s"select count(*) from $tableName where v is null")(Seq(0))
          assertVariantSegments(tableName, leg, Seq(("v", Seq(
            (0 until 20, ObjA), (20 until 30, ObjAConflict), (30 until 40, ObjB)))))
        }
      })
    }

    // MOR: the same path is typed in the base, then updated through an unshredded log and a
    // shredded log; the merged read serves each row from a different physical slot.
    Seq("true", "false").foreach { pushIntoScan =>
      withRecordType(Seq(HoodieRecordType.SPARK))(withTempDir { tmp =>
        withSQLConf("spark.sql.variant.pushVariantIntoScan" -> pushIntoScan) {
          val tableName = generateTableName
          val tablePath = tmp.getCanonicalPath
          val leg = s"mor pushVariantIntoScan=$pushIntoScan, $tableName"
          createVariantTable(tableName, tablePath, "mor",
            props = Seq("hoodie.compact.inline = 'false'"))

          withWriteLayout(Forced("a bigint")) {
            spark.sql(s"insert into $tableName ${variantSourceSql(Seq((0 until 10, ObjA)))}")
          }
          withWriteLayout(Unshredded) {
            spark.sql(s"update $tableName set " +
              s"""v = parse_json(concat('{"a":"s', id, '","b":"b', id, '"}')), ts = 1001 """ +
              "where id >= 5")
          }
          withWriteLayout(Forced("a bigint")) {
            spark.sql(s"update $tableName set " +
              s"""v = parse_json(concat('{"a":', 100 + id, ',"b":"b', id, '"}')), ts = 1002 """ +
              "where id < 3")
          }

          val aValues = spark.sql(
            s"select id, try_variant_get(v, '$$.a', 'bigint') from $tableName order by id").collect()
          assert(aValues.length == 10, s"[$leg] row count")
          aValues.foreach { row =>
            val id = row.getInt(0)
            val expected: Any = if (id < 3) 100L + id else if (id < 5) id.toLong else null
            val actual = if (row.isNullAt(1)) null else row.getLong(1)
            assert(actual == expected, s"[$leg] id=$id: expected $expected, got $actual")
          }
          checkAnswer(
            s"select count(*) from $tableName where try_variant_get(v, '$$.a', 'bigint') > 100")(Seq(2))
          checkAnswer(
            s"select id from $tableName where variant_get(v, '$$.b', 'string') = 'b7'")(Seq(7))
        }
      })
    }
  }

  test("Schema-on-read reads of shredded variant files fail fast") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    withRecordType(Seq(HoodieRecordType.SPARK))(withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath
      val leg = s"schema-on-read, $tableName"
      createVariantTable(tableName, tablePath, "cow")
      withWriteLayout(Forced("a bigint")) {
        spark.sql(s"""insert into $tableName values (1, parse_json('{"a":1}'), 1000)""")
      }

      withSQLConf("hoodie.schema.on.read.enable" -> "true") {
        // Committing a schema-on-read DDL stores the internal schema; reads under
        // hoodie.schema.on.read.enable then request the internal-schema form of the variant
        // ({metadata, value}), which clips typed_value away. With PushVariantIntoScan disabled,
        // that would return silent nulls for the typed rows - the guard must fire instead
        // (#18285 tracks real reconstruction under schema-on-read).
        spark.sql(s"alter table $tableName add columns (note string)")
        withSQLConf("spark.sql.variant.pushVariantIntoScan" -> "false") {
          checkNestedExceptionContains(
            () => spark.sql(s"select id, cast(v as string), note from $tableName").collect())(
            "shredded variant")
        }
        // Under the default PushVariantIntoScan rewrite the read fails as well - through the
        // guard when internal-schema pruning survives the synthetic projection, otherwise with
        // an engine-internal error before it (a pruning failure or a codegen NPE on the clipped
        // value). Pin that it never silently returns rows AND that the failure is
        // variant-related, so an unrelated analysis error cannot green this leg; the real fix
        // for both legs is #18285.
        val failure = intercept[Throwable] {
          spark.sql(s"select id, cast(v as string), note from $tableName").collect()
        }
        val chain = Iterator.iterate(failure)(_.getCause).takeWhile(_ != null)
          .map(t => s"${t.getClass.getName}: ${Option(t.getMessage).getOrElse("")}").mkString("\n")
        assert(chain.toLowerCase.contains("variant") || chain.contains("NullPointerException"),
          s"[$leg] expected a variant-related failure, got:\n$chain")
      }

      // Known #18285 residue, documented rather than pinned: the schema-on-read DDL also
      // rewrites the CATALOG schema through the internal-schema converter, which has no VARIANT
      // arm, so the catalog column degrades to a plain struct<metadata,value> (the resolved
      // avro table schema keeps its variant logical type). Plain reads of the table after the
      // DDL request that struct and fail in Spark before any Hudi hook.
    })

    // The guard recurses: a NESTED shredded variant (struct<inner: variant>, written by the
    // bulk-insert row writer, the one production writer that shreds below the top level) fails
    // fast too, instead of slipping past a top-level-only walk.
    withRecordType(Seq(HoodieRecordType.SPARK))(withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath
      val leg = s"nested schema-on-read, $tableName"
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  s struct<inner: variant>,
           |  ts long
           |) using hudi
           | location '$tablePath'
           | tblproperties (
           |  primaryKey = 'id',
           |  preCombineField = 'ts',
           |  type = 'cow',
           |  hoodie.datasource.write.row.writer.enable = 'true'
           | )
         """.stripMargin)
      withSQLConf("hoodie.spark.sql.insert.into.operation" -> "bulk_insert") {
        withWriteLayout(Forced("k string")) {
          spark.sql(s"""insert into $tableName values (1, named_struct('inner', parse_json('{"k":"x1"}')), 1000)""")
        }
      }
      val files = listDataParquetFiles(tablePath)
      assert(files.size == 1
        && getFieldAsGroup(getFieldAsGroup(readParquetSchema(files.head), "s"), "inner").containsField("typed_value"),
        s"[$leg] setup: the nested variant must be shredded")

      withSQLConf("hoodie.schema.on.read.enable" -> "true") {
        spark.sql(s"alter table $tableName add columns (note string)")
        withSQLConf("spark.sql.variant.pushVariantIntoScan" -> "false") {
          checkNestedExceptionContains(
            () => spark.sql(s"select id, cast(s.inner as string), note from $tableName").collect())(
            "shredded variant")
        }
      }
    })
  }

  // -----------------------------------------------------------------------------------------------
  // F. Nested variant
  // -----------------------------------------------------------------------------------------------

  test("Row-writer forced shredding of a nested variant reads back and merges") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    // The bulk-insert row writer is the only production writer that shreds a NESTED variant
    // (the forced hook of the AVRO write support is top-level only, and #18961's inference
    // never shreds nested columns).
    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath
      val leg = s"nested forced shredding, $tableName"
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  s struct<inner: variant>,
           |  ts long
           |) using hudi
           | location '$tablePath'
           | tblproperties (
           |  primaryKey = 'id',
           |  preCombineField = 'ts',
           |  type = 'cow',
           |  hoodie.datasource.write.row.writer.enable = 'true'
           | )
         """.stripMargin)

      withSQLConf("hoodie.spark.sql.insert.into.operation" -> "bulk_insert") {
        withWriteLayout(Forced("k string")) {
          spark.sql(s"""insert into $tableName values (1, named_struct('inner', parse_json('{"k":"x1"}')), 1000)""")
        }
      }

      val files = listDataParquetFiles(tablePath)
      assert(files.size == 1, s"[$leg] expected one base file, got $files")
      val sGroup = getFieldAsGroup(readParquetSchema(files.head), "s")
      val innerGroup = getFieldAsGroup(sGroup, "inner")
      assert(innerGroup.containsField("typed_value"),
        s"[$leg] nested variant should be shredded by the row writer:\n$innerGroup")
      assert(getFieldAsGroup(innerGroup, "typed_value").containsField("k"),
        s"[$leg] nested typed_value should carry k:\n$innerGroup")

      // #18605 history: the batch-disabling guards in HoodieFileGroupReaderBasedFileFormat are
      // top-level-only, so a NESTED variant may still take the vectorized reader. Sweep both
      // modes so this leg cannot silently flip branches.
      Seq("true", "false").foreach { vectorized =>
        withSQLConf("spark.sql.parquet.enableVectorizedReader" -> vectorized) {
          checkAnswer(s"select id, cast(s.inner as string) from $tableName")(
            Seq(1, """{"k":"x1"}""")
          )
        }
      }

      // A plain insert bin-packs into the same file group: the small-file merge must read the
      // nested-shredded base back (nested reconstruction on the AVRO record type leg).
      withWriteLayout(Forced("k string")) {
        spark.sql(s"""insert into $tableName values (2, named_struct('inner', parse_json('{"k":"x2"}')), 1000)""")
      }
      assertSingleFileGroup(tablePath, leg)
      Seq("true", "false").foreach { vectorized =>
        withSQLConf("spark.sql.parquet.enableVectorizedReader" -> vectorized) {
          checkAnswer(s"select id, cast(s.inner as string) from $tableName order by id")(
            Seq(1, """{"k":"x1"}"""),
            Seq(2, """{"k":"x2"}""")
          )
        }
      }
    })
  }

  // -----------------------------------------------------------------------------------------------
  // G. Schema evolution
  // -----------------------------------------------------------------------------------------------

  test("Add-column evolution followed by compaction and clustering over mixed layouts") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath
      val leg = s"evolution + services, $tableName"
      createVariantTable(tableName, tablePath, "mor",
        props = Seq("hoodie.index.type = 'INMEMORY'", "hoodie.compact.inline = 'false'"))
      val layout2 = inferredOr(Forced("c bigint, d boolean"))

      withWriteLayout(Forced("a bigint, b string")) {
        spark.sql(s"""insert into $tableName values (1, parse_json('{"a":1,"b":"b1"}'), 1000), """ +
          """(2, parse_json('{"a":2,"b":"b2"}'), 1000)""")
      }
      spark.sql(s"alter table $tableName add columns (note string)")
      withWriteLayout(layout2) {
        spark.sql(s"""insert into $tableName values (3, parse_json('{"c":3,"d":true}'), 1000, 'n3')""")
      }

      withWriteLayout(layout2) {
        runCompaction(tableName)
      }
      assertCompactionCount(tablePath, 1, leg)
      checkAnswer(s"select id, cast(v as string), note from $tableName order by id")(
        Seq(1, """{"a":1,"b":"b1"}""", null),
        Seq(2, """{"a":2,"b":"b2"}""", null),
        Seq(3, """{"c":3,"d":true}""", "n3")
      )
      val compactedBase = baseLayouts(tablePath)
      assert(compactedBase.nonEmpty && compactedBase.forall(_.isShredded),
        s"[$leg] compacted base must be shredded under $layout2: $compactedBase")

      withWriteLayout(Unshredded) {
        runClustering(tableName, rowWriter = true)
      }
      val clusteringInstant = completedClusteringInstant(tablePath, leg)
      val clustered = baseLayouts(tablePath).filter(_.instantTime == clusteringInstant)
      assert(clustered.nonEmpty && clustered.forall(!_.isShredded),
        s"[$leg] clustering under Unshredded must write unshredded output: $clustered")
      checkAnswer(s"select id, cast(v as string), note from $tableName order by id")(
        Seq(1, """{"a":1,"b":"b1"}""", null),
        Seq(2, """{"a":2,"b":"b2"}""", null),
        Seq(3, """{"c":3,"d":true}""", "n3")
      )
    })
  }

  // -----------------------------------------------------------------------------------------------
  // H. Robustness
  // -----------------------------------------------------------------------------------------------

  test("Rollback and savepoint restore across layout changes") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    withRecordType(Seq(HoodieRecordType.SPARK))(withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath
      val leg = s"rollback/savepoint, $tableName"
      createVariantTable(tableName, tablePath, "cow", props = Seq(NEW_FILE_GROUP_PER_COMMIT))

      val instants = seedMixedLayoutTable(tableName, tablePath, Seq(
        (Forced("a bigint, b string"), Seq((0 until 2, ObjA))),
        (inferredOr(Forced("c bigint")), Seq((2 until 4, ObjB)))))
      spark.sql(s"call create_savepoint(table => '$tableName', commit_time => '${instants(1)}')")

      withWriteLayout(Unshredded) {
        spark.sql(s"insert into $tableName ${variantSourceSql(Seq((4 until 6, ObjA)))}")
      }
      val instant3 = latestCompletedInstant(tablePath)
      checkAnswer(s"select count(*) from $tableName")(Seq(6))

      // Rollback of the unshredded commit removes its files; the shredded ones stay readable.
      spark.sql(s"call rollback_to_instant(table => '$tableName', instant_time => '$instant3')")
      checkAnswer(s"select count(*) from $tableName")(Seq(4))
      val remainingInstants = variantFileLayouts(tablePath).map(_.instantTime).distinct.sorted
      assert(remainingInstants == instants.sorted,
        s"[$leg] only the first two commits' files should remain, got $remainingInstants")

      // New writes work after the rollback, under yet another layout.
      withWriteLayout(Forced("a bigint")) {
        spark.sql(s"insert into $tableName ${variantSourceSql(Seq((4 until 6, ObjA)))}")
      }
      checkAnswer(s"select count(*) from $tableName")(Seq(6))

      // Restore to the savepoint drops everything after the second commit.
      spark.sql(s"call rollback_to_savepoint(table => '$tableName', instant_time => '${instants(1)}')")
      checkAnswer(s"select id, cast(v as string) from $tableName order by id")(
        Seq(0, """{"a":0,"b":"b0"}"""),
        Seq(1, """{"a":1,"b":"b1"}"""),
        Seq(2, """{"c":2,"d":true}"""),
        Seq(3, """{"c":3,"d":true}""")
      )

      // And the table accepts writes again.
      withWriteLayout(Unshredded) {
        spark.sql(s"insert into $tableName ${variantSourceSql(Seq((6 until 8, ObjA)))}")
      }
      checkAnswer(s"select count(*) from $tableName")(Seq(6))
    })
  }
}
