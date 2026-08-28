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

import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.core.io.storage.VariantShreddingInferenceFileWriter
import org.apache.hudi.testutils.DataSourceTestUtils

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase.getLastCommitMetadata

/**
 * Mixed-layout variant shredding matrix: files with DIFFERENT typed_value layouts in one table,
 * shredded/unshredded splits between base and log files, and rows inside one file that fell back
 * to the residual value column, driven through compaction, clustering, merges and every Spark
 * read mode. Complements [[TestVariantDataType]], whose shredded tests force ONE layout per
 * table.
 *
 * Layouts are toggled per commit or table service through session confs (session hoodie.* confs
 * override tblproperties for SQL DML and for the run_compaction/run_clustering procedures alike).
 * Every test is gated on Spark 4.1+, which is exactly the set of profiles that register #18961's
 * per-file shredding-schema inferrer (pinned by TestVariantDataType's "A shredding-schema
 * inferrer is registered for every Spark version that ships one"), so an [[Inferred]] leg here
 * always infers rather than silently degrading to an unshredded write.
 *
 * Deliberately not covered here:
 * - Custom payloads: FileGroupRecordBuffer.getProjectedTransformer short-circuits the variant
 *   log-block projection when payload classes are present (#18674), so that is a real,
 *   explicitly UNTESTED variant branch; PartialUpdateMode and the CUSTOM merge mode are
 *   likewise unreached (EVENT_TIME ordering is swept throughout, COMMIT_TIME in the one leg
 *   that sets hoodie.record.merge.mode).
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

    withVariantTable("same-file mix", "cow") { (tableName, tablePath, leg) =>
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
    }
  }

  // -----------------------------------------------------------------------------------------------
  // B. Mixed files inside one table
  // -----------------------------------------------------------------------------------------------

  test("Each commit keeps its own layout; snapshot, time travel, incremental and RO read them all") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    // Read-mode test: layouts are writer-side and every layout is written identically by both
    // record types, so the sweep would only re-run the same reads. SPARK pinned.
    withVariantTable("mixed-files", "cow", props = Seq(NEW_FILE_GROUP_PER_COMMIT),
      recordTypes = Seq(HoodieRecordType.SPARK)) { (tableName, tablePath, leg) =>
      // Four commits, four layouts, one file each (small.file.limit=0 keeps every commit in its
      // own file group). The last commit infers {c, d} from its own ObjB rows.
      val instants = seedMixedLayoutTable(tableName, tablePath, Seq(
        (Unshredded, Seq((0 until 2, ObjA))),
        (Forced("a bigint, b string"), Seq((2 until 4, ObjA))),
        (Forced("b string"), Seq((4 until 6, ObjA))),
        (Inferred, Seq((6 until 8, ObjB)))))

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
      val incRows = incrementalIdAndVariant(tablePath)
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
    }
  }

  test("Small-file bin-pack rewrites the file under the layout of the incoming commit") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    // Default small.file.limit on purpose: each insert bin-packs into the first file group
    // and rewrites it (HoodieConcatHandle -> HoodieMergeHelper on the AVRO record type).
    // The value round-trip of that merge is owned by TestVariantDataType's small-file test;
    // this one exists for the per-instant LAYOUT pin below.
    withVariantTable("bin-pack layout flip", "cow") { (tableName, tablePath, leg) =>
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
    }
  }

  // -----------------------------------------------------------------------------------------------
  // C. MOR compaction over base/log layout splits
  // -----------------------------------------------------------------------------------------------

  test("MOR compaction merges logs of three layouts and re-derives the base layout per service run") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    // INMEMORY sends MOR inserts to log files; compaction runs via the procedure so each run
    // can happen under its own layout confs.
    withVariantTable("compaction layout split", "mor", props = Seq(
      "hoodie.index.type = 'INMEMORY'", "hoodie.compact.inline = 'false'")) { (tableName, tablePath, leg) =>
      withWriteLayout(Forced("a bigint, b string")) {
        spark.sql(s"""insert into $tableName values (1, parse_json('{"a":1,"b":"b1"}'), 1000)""")
      }
      val instant1 = latestCompletedInstant(tablePath)
      withWriteLayout(Unshredded) {
        spark.sql(s"""insert into $tableName values (2, parse_json('{"a":2,"b":"b2"}'), 1000), """ +
          """(3, parse_json('{"a":3,"b":"b3"}'), 1000), (4, parse_json('{"a":4,"b":"b4"}'), 1000)""")
      }
      val instant2 = latestCompletedInstant(tablePath)
      withWriteLayout(Inferred) {
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

      // Compaction 1 under Inferred: reads all three log layouts, infers the base layout from
      // the merged rows.
      withWriteLayout(Inferred) {
        runCompaction(tableName)
      }
      assertResult(false)(DataSourceTestUtils.isLogFileOnly(tablePath))
      assertCompactionCount(tablePath, 1, leg)
      val base1 = baseLayouts(tablePath)
      assertAllShredded(base1, shredded = true, s"$leg compacted base under Inferred")
      // 6 rows: a and b on 4 (66 percent), c and d on 2 (33 percent) - all clear the 10
      // percent inference bar.
      base1.foreach(l => assert(l.typedFields.toSet == Set("a", "b", "c", "d"),
        s"[$leg] inferred typed_value should carry all four keys: ${l.typedFields}"))
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
      assertAllShredded(base2, shredded = false, s"$leg base of the compaction under Unshredded")
      checkAnswer(s"select id, cast(v as string) from $tableName where id in (2, 3) order by id")(
        Seq(2, """{"a":22,"b":"b22"}"""),
        Seq(3, """{"a":33,"b":"b33"}""")
      )

      // Round 3: compaction under Inferred again, this time reading an UNSHREDDED base plus a
      // shredded log.
      withWriteLayout(Inferred) {
        spark.sql(s"""update $tableName set v = parse_json('{"a":44,"b":"b44"}'), ts = 1001 where id = 4""")
        runCompaction(tableName)
      }
      assertCompactionCount(tablePath, 3, leg)
      val compact3Instant = latestCompletedInstant(tablePath)
      val base3 = baseLayouts(tablePath).filter(_.instantTime == compact3Instant)
      assertAllShredded(base3, shredded = true, s"$leg base of the second compaction under Inferred")

      checkAnswer(s"select id, cast(v as string) from $tableName order by id")(
        Seq(1, """{"a":1,"b":"b1"}"""),
        Seq(2, """{"a":22,"b":"b22"}"""),
        Seq(3, """{"a":33,"b":"b33"}"""),
        Seq(4, """{"a":44,"b":"b44"}"""),
        Seq(5, """{"c":5,"d":true}""")
      )
      // Incremental over the full range sees the latest value of every LIVE key (id 6 deleted),
      // values intact - a bare count would pass with v all-null.
      val incRows = incrementalIdAndVariant(tablePath)
      assert(incRows.map(r => (r.getInt(0), r.getString(1))).toSeq == Seq(
        (1, """{"a":1,"b":"b1"}"""),
        (2, """{"a":22,"b":"b22"}"""),
        (3, """{"a":33,"b":"b33"}"""),
        (4, """{"a":44,"b":"b44"}"""),
        (5, """{"c":5,"d":true}""")
      ), s"[$leg] incremental over the full range, got: ${incRows.mkString(", ")}")
    }
  }

  test("COMMIT_TIME ordering lets a lower-ts update win across layouts, in the log merge and after compaction") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    // Every table in the suite carries preCombineField = 'ts'; the others merge under EVENT_TIME
    // ordering, this one sets hoodie.record.merge.mode to COMMIT_TIME_ORDERING, where the later
    // commit wins whatever its ts: the update below carries a LOWER ts (500) than the row it
    // replaces (1000) and must still win - under EVENT_TIME the read would keep {"a":1}. The
    // ordering field has to stay: with none, getOrderingValue returns the same default for both
    // records and both modes keep the newer one, so the leg would not discriminate. Pinned once
    // in the log merge over two layouts and again on the compacted base.
    withVariantTable("commit-time ordering", "mor", props = Seq(
      "hoodie.index.type = 'INMEMORY'", "hoodie.compact.inline = 'false'",
      "hoodie.record.merge.mode = 'COMMIT_TIME_ORDERING'")) {
      (tableName, tablePath, leg) =>
      withWriteLayout(Forced("a bigint")) {
        spark.sql(s"""insert into $tableName values (1, parse_json('{"a":1}'), 1000)""")
      }
      withWriteLayout(Unshredded) {
        spark.sql(s"""update $tableName set v = parse_json('{"a":2}'), ts = 500 where id = 1""")
      }
      assertResult(true)(DataSourceTestUtils.isLogFileOnly(tablePath))
      checkAnswer(s"select id, cast(v as string) from $tableName")(Seq(1, """{"a":2}"""))

      withWriteLayout(Forced("a bigint")) {
        runCompaction(tableName)
      }
      assertCompactionCount(tablePath, 1, leg)
      assertAllShredded(baseLayouts(tablePath), shredded = true, s"$leg compacted base")
      checkAnswer(s"select id, cast(v as string) from $tableName")(Seq(1, """{"a":2}"""))
      checkAnswer(s"select id, cast(v as string) from hudi_query('$tableName', 'read_optimized')")(
        Seq(1, """{"a":2}"""))
    }
  }

  test("Table version 9 legacy log blocks stay unshredded and compact onto a shredded base") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    withVariantTable("table version 9", "mor", props = Seq(
      "hoodie.write.table.version = '9'",
      "hoodie.index.type = 'INMEMORY'",
      "hoodie.compact.inline = 'false'")) { (tableName, tablePath, leg) =>
      withWriteLayout(Inferred) {
        spark.sql(s"""insert into $tableName values (1, parse_json('{"key":"value1"}'), 1000)""")
        spark.sql(s"""insert into $tableName values (2, parse_json('{"key":"value2"}'), 1000)""")
      }
      assertResult(true)(DataSourceTestUtils.isLogFileOnly(tablePath))
      // Write version 9 writes the legacy inline log format (avro blocks on the AVRO record
      // type leg, inline parquet data blocks on the SPARK leg), never native parquet log files;
      // neither inline form shreds, so the shredded layout materializes only at compaction.
      assert(nativeLogLayouts(tablePath).isEmpty,
        s"[$leg] table version 9 must not write native parquet log files")

      withWriteLayout(Inferred) {
        runCompaction(tableName)
      }
      assertResult(false)(DataSourceTestUtils.isLogFileOnly(tablePath))
      val base1 = baseLayouts(tablePath)
      assertAllShredded(base1, shredded = true, s"$leg compacted base")
      base1.foreach(l => assert(l.typedFields == Seq("key"),
        s"[$leg] typed_value should carry key: ${l.typedFields}"))

      // Legacy log over the shredded base, then a second compaction reads base + legacy log.
      withWriteLayout(Inferred) {
        spark.sql(s"""update $tableName set v = parse_json('{"key":"v1-updated"}'), ts = 1001 where id = 1""")
      }
      checkAnswer(s"select id, cast(v as string) from $tableName order by id")(
        Seq(1, """{"key":"v1-updated"}"""),
        Seq(2, """{"key":"value2"}""")
      )
      withWriteLayout(Inferred) {
        runCompaction(tableName)
      }
      checkAnswer(s"select id, cast(v as string) from $tableName order by id")(
        Seq(1, """{"key":"v1-updated"}"""),
        Seq(2, """{"key":"value2"}""")
      )
    }
  }

  // -----------------------------------------------------------------------------------------------
  // D. Clustering over heterogeneous inputs
  // -----------------------------------------------------------------------------------------------

  test("Clustering rewrites heterogeneous files into the configured layout") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    Seq(true, false).foreach { rowWriter =>
      val recordTypes = clusteringRecordTypes(rowWriter)
      Seq(Unshredded, Inferred).foreach { outLayout =>
        // The unshredded output cell only re-pins the unshredded rewrite, which the evolution
        // test's clustering leg already sweeps over both record types; SPARK alone here, so this
        // test runs 5 clustering jobs instead of 6.
        val cellRecordTypes = if (outLayout == Unshredded) Seq(HoodieRecordType.SPARK) else recordTypes
        withVariantTable(s"clustering rowWriter=$rowWriter out=$outLayout", "cow",
          props = Seq(NEW_FILE_GROUP_PER_COMMIT), recordTypes = cellRecordTypes) { (tableName, tablePath, leg) =>
          val instants = seedMixedLayoutTable(tableName, tablePath, Seq(
            (Forced("a bigint, b string"), Seq((0 until 2, ObjA))),
            (Unshredded, Seq((2 until 4, ObjA))),
            (Inferred, Seq((4 until 6, ObjB)))))

          withWriteLayout(outLayout) {
            runClustering(tableName, rowWriter)
          }
          val clusteringInstant = completedClusteringInstant(tablePath, leg)
          val outFiles = baseLayouts(tablePath).filter(_.instantTime == clusteringInstant)
          assert(outFiles.nonEmpty, s"[$leg] clustering should have written base files")
          if (outLayout == Unshredded) {
            outFiles.foreach(l => assert(!l.isShredded,
              s"[$leg] clustering under Unshredded must write unshredded output: ${l.path}"))
          } else {
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
        }
      }
    }
  }

  test("MOR clustering folds log files of another layout into the rewritten base") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    Seq(true, false).foreach { rowWriter =>
      val recordTypes = clusteringRecordTypes(rowWriter)
      // No INMEMORY index: the first insert creates a base file, the update goes to a log.
      withVariantTable(s"mor clustering rowWriter=$rowWriter", "mor",
        props = Seq("hoodie.compact.inline = 'false'"), recordTypes = recordTypes) { (tableName, tablePath, leg) =>
        withWriteLayout(Forced("a bigint, b string")) {
          spark.sql(s"""insert into $tableName values (1, parse_json('{"a":1,"b":"b1"}'), 1000), """ +
            """(2, parse_json('{"a":2,"b":"b2"}'), 1000)""")
        }
        withWriteLayout(Unshredded) {
          spark.sql(s"""update $tableName set v = parse_json('{"a":10,"b":"b10"}'), ts = 1001 where id = 1""")
        }
        // The slice going into clustering: a shredded base plus an unshredded native log.
        assertAllShredded(baseLayouts(tablePath), shredded = true, s"$leg pre-clustering base")
        assertAllShredded(nativeLogLayouts(tablePath), shredded = false, s"$leg pre-clustering log")

        withWriteLayout(Inferred) {
          runClustering(tableName, rowWriter)
        }
        val clusteringInstant = completedClusteringInstant(tablePath, leg)
        val outFiles = baseLayouts(tablePath).filter(_.instantTime == clusteringInstant)
        assert(outFiles.nonEmpty, s"[$leg] clustering should have written base files")
        outFiles.foreach(l => assert(l.isShredded,
          s"[$leg] clustering under Inferred must write shredded output: ${l.path}"))

        // The clustered base carries the merged (updated) row.
        checkAnswer(s"select id, cast(v as string) from $tableName order by id")(
          Seq(1, """{"a":10,"b":"b10"}"""),
          Seq(2, """{"a":2,"b":"b2"}""")
        )
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // E. Read modes over mixed layouts
  // -----------------------------------------------------------------------------------------------

  test("variant_get filters and projections resolve per file across mixed layouts") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    // Read-mode test; SPARK pinned (see the mixed-files test above).
    Seq("true", "false").foreach { pushIntoScan =>
      withSQLConf("spark.sql.variant.pushVariantIntoScan" -> pushIntoScan) {
        withVariantTable(s"cow pushVariantIntoScan=$pushIntoScan", "cow",
          props = Seq(NEW_FILE_GROUP_PER_COMMIT), recordTypes = Seq(HoodieRecordType.SPARK)) {
          (tableName, tablePath, leg) =>
          // $.a is typed in file 1, residual (unshredded) in file 2, a type-conflicted residual
          // in file 3 and absent in file 4.
          seedMixedLayoutTable(tableName, tablePath, Seq(
            (Forced("a bigint"), Seq((0 until 10, ObjA))),
            (Unshredded, Seq((10 until 20, ObjA))),
            (Forced("a bigint"), Seq((20 until 30, ObjAConflict))),
            (Inferred, Seq((30 until 40, ObjB)))))

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
      }
    }

    // MOR: the same path is typed in the base, then updated through an unshredded log and a
    // shredded log; the merged read serves each row from a different physical slot.
    Seq("true", "false").foreach { pushIntoScan =>
      withSQLConf("spark.sql.variant.pushVariantIntoScan" -> pushIntoScan) {
        withVariantTable(s"mor pushVariantIntoScan=$pushIntoScan", "mor",
          props = Seq("hoodie.compact.inline = 'false'"), recordTypes = Seq(HoodieRecordType.SPARK)) {
          (tableName, tablePath, leg) =>
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
      }
    }
  }

  test("Schema-on-read reads of shredded variant files fail fast") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    withVariantTable("schema-on-read", "cow", recordTypes = Seq(HoodieRecordType.SPARK)) {
      (tableName, tablePath, leg) =>
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
        // Under the default PushVariantIntoScan rewrite the read fails through the guard as
        // well: pruning treats the rewritten ordinal-named struct as the variant column itself
        // (SparkInternalSchemaConverter.isVariantRewriteStruct), so the guard sees the request
        // and rejects it up front instead of an engine-internal pruning error or codegen NPE.
        // Pinned on the rewrite arm's own wording: both messages carry "cannot reconstruct", so
        // matching on that alone would stay green with the rewrite arm gone. Fix is #18285.
        checkNestedExceptionContains(
          () => spark.sql(s"select id, cast(v as string), note from $tableName").collect())(
          "pushVariantIntoScan")
        // The guard's empty-projection carve-out: count(*) reads no column data, and the query
        // schema it would be checked against is the UNPRUNED table schema, variant included, so
        // the guard must not run at all. Drop the requiredSchema.nonEmpty gate at any of its five
        // sites and this count fails on the shredded file.
        checkAnswer(s"select count(*) from $tableName")(Seq(1))
      }

      // Known #18285 residue, documented rather than pinned: the schema-on-read DDL also
      // rewrites the CATALOG schema through the internal-schema converter, which has no VARIANT
      // arm, so the catalog column degrades to a plain struct<metadata,value> (the resolved
      // avro table schema keeps its variant logical type). Plain reads of the table after the
      // DDL request that struct and fail in Spark before any Hudi hook.
    }

    // The guard recurses: a NESTED shredded variant (struct<inner: variant>, written by the
    // bulk-insert row writer, the one production writer that shreds below the top level) fails
    // fast too, instead of slipping past a top-level-only walk.
    withNestedVariantTable("nested schema-on-read", recordTypes = Seq(HoodieRecordType.SPARK)) {
      (tableName, tablePath, leg) =>
      assertVariantLayout(tablePath, shredded = true, s"$leg setup", column = "s.inner")

      withSQLConf("hoodie.schema.on.read.enable" -> "true") {
        spark.sql(s"alter table $tableName add columns (note string)")
        withSQLConf("spark.sql.variant.pushVariantIntoScan" -> "false") {
          checkNestedExceptionContains(
            () => spark.sql(s"select id, cast(s.inner as string), note from $tableName").collect())(
            "shredded variant")
        }
      }
    }
  }

  test("Inline compaction and clustering under schema-on-read fail fast on the variant column") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    // Hudi's own base-file reads request the same full-variant shape as Spark's PushVariantIntoScan
    // rewrite (SparkFileFormatInternalRowReaderContext), and a table service's reader context
    // carries the table's internal schema once one is committed (SparkReaderContextFactory puts
    // the table path and the valid commits on the conf), so an inline service under a schema-on-read
    // write reaches the guard's rewrite arm on a variant column that was never shredded. It cannot be
    // served until #18285 - the merged internal-schema request comes back as {metadata, value} where
    // the restore projection expects the ordinal struct - and before the guard the same read died
    // inside pruning ("cannot prune col: v.0"). What is pinned here is that the failure names this
    // route rather than a Spark conf the service never set. Upserts are unaffected (the merge
    // handle's base-file read never enters the reader's schema-on-read branch), and so are
    // run_compaction / run_clustering, whose clients carry no internal schema.
    def writeThroughDataFrame(tableName: String, tablePath: String, tableType: String, id: Int,
                              serviceOptions: (String, String)*): Unit = {
      var writer = spark.sql(s"""select $id as id, parse_json('{"a":$id}') as v, 2000L as ts, cast(null as string) as note""")
        .write.format("hudi")
        .option("hoodie.table.name", tableName)
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.precombine.field", "ts")
        .option("hoodie.datasource.write.operation", "upsert")
        .option("hoodie.datasource.write.table.type", tableType)
        .option("hoodie.schema.on.read.enable", "true")
      serviceOptions.foreach { case (key, value) => writer = writer.option(key, value) }
      writer.mode("append").save(tablePath)
    }
    def seedWithCommittedInternalSchema(tableName: String): Unit = {
      withSQLConf("hoodie.schema.on.read.enable" -> "true") {
        spark.sql(s"""insert into $tableName values (1, parse_json('{"a":1}'), 1000)""")
        // The schema-on-read DDL is what commits the internal schema; the insert alone does not.
        spark.sql(s"alter table $tableName add columns (note string)")
      }
    }

    withVariantTable("inline clustering under schema-on-read", "cow", recordTypes = Seq(HoodieRecordType.SPARK)) {
      (tableName, tablePath, leg) =>
      seedWithCommittedInternalSchema(tableName)
      // With the default small-file limit a new key bin-packs into the seed file, so the upsert's
      // own base-file read (the merge handle) takes the very file the service would. That write
      // passes on its own; only the one that also schedules clustering fails, and it fails after
      // its commit completed, leaving the scheduled clustering incomplete - which pins the failure
      // on the clustering read and not on the write.
      writeThroughDataFrame(tableName, tablePath, "COPY_ON_WRITE", 2)
      val beforeClustering = latestCompletedInstant(tablePath)
      checkNestedExceptionContains(() => writeThroughDataFrame(tableName, tablePath, "COPY_ON_WRITE", 3,
        "hoodie.clustering.inline" -> "true", "hoodie.clustering.inline.max.commits" -> "1"))(
        "Hudi's own base-file reads")
      assert(latestCompletedInstant(tablePath) != beforeClustering, s"[$leg] the upsert itself must commit")
      assertPendingClustering(tablePath, leg)
    }
    withVariantTable("inline compaction under schema-on-read", "mor", recordTypes = Seq(HoodieRecordType.SPARK)) {
      (tableName, tablePath, leg) =>
      seedWithCommittedInternalSchema(tableName)
      checkNestedExceptionContains(() => writeThroughDataFrame(tableName, tablePath, "MERGE_ON_READ", 1,
        "hoodie.compact.inline" -> "true", "hoodie.compact.inline.max.delta.commits" -> "1"))(
        "Hudi's own base-file reads")
    }
  }

  // -----------------------------------------------------------------------------------------------
  // F. Nested variant
  // -----------------------------------------------------------------------------------------------

  test("Row-writer forced shredding of a nested variant reads back and merges") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    // The bulk-insert row writer is the only production writer that shreds a NESTED variant
    // (the forced hook of the AVRO write support is top-level only, and #18961's inference
    // never shreds nested columns).
    withNestedVariantTable("nested forced shredding") { (tableName, tablePath, leg) =>
      val files = listDataParquetFiles(tablePath)
      assert(files.size == 1, s"[$leg] expected one base file, got $files")
      assertVariantLayout(tablePath, shredded = true, leg, column = "s.inner")
      val innerGroup = variantGroupOf(files.head, "s.inner")
      assert(getFieldAsGroup(innerGroup, "typed_value").containsField("k"),
        s"[$leg] nested typed_value should carry k:\n$innerGroup")

      // #18605 history: the batch-disabling guards in HoodieFileGroupReaderBasedFileFormat are
      // top-level-only, so a NESTED variant still reaches the vectorized reader. The session conf
      // does pick the reader - HoodieFileGroupReaderBasedFileFormat.supportBatch reads
      // sparkSession.sessionState.conf and ParquetUtils.isBatchReadSupportedForSchema gates on
      // spark.sql.parquet.enableVectorizedReader, and only afterwards does
      // buildReaderWithPartitionValues write that decision back into the conf - so sweep it and
      // pin both readers. Every nested-variant read bug so far (HUDI-7190, HUDI-8803, #18605) is
      // vectorized-only, which leaves the row-based leg as the control.
      Seq("true", "false").foreach { vectorizedReader =>
        withSQLConf("spark.sql.parquet.enableVectorizedReader" -> vectorizedReader) {
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
      checkAnswer(s"select id, cast(s.inner as string) from $tableName order by id")(
        Seq(1, """{"k":"x1"}"""),
        Seq(2, """{"k":"x2"}""")
      )
    }
  }

  // -----------------------------------------------------------------------------------------------
  // G. Schema evolution
  // -----------------------------------------------------------------------------------------------

  test("Add-column evolution followed by compaction and clustering over mixed layouts") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    withVariantTable("evolution + services", "mor", props = Seq(
      "hoodie.index.type = 'INMEMORY'", "hoodie.compact.inline = 'false'")) { (tableName, tablePath, leg) =>
      withWriteLayout(Forced("a bigint, b string")) {
        spark.sql(s"""insert into $tableName values (1, parse_json('{"a":1,"b":"b1"}'), 1000), """ +
          """(2, parse_json('{"a":2,"b":"b2"}'), 1000)""")
      }
      spark.sql(s"alter table $tableName add columns (note string)")
      withWriteLayout(Inferred) {
        spark.sql(s"""insert into $tableName values (3, parse_json('{"c":3,"d":true}'), 1000, 'n3')""")
      }

      withWriteLayout(Inferred) {
        runCompaction(tableName)
      }
      assertCompactionCount(tablePath, 1, leg)
      checkAnswer(s"select id, cast(v as string), note from $tableName order by id")(
        Seq(1, """{"a":1,"b":"b1"}""", null),
        Seq(2, """{"a":2,"b":"b2"}""", null),
        Seq(3, """{"c":3,"d":true}""", "n3")
      )
      assertAllShredded(baseLayouts(tablePath), shredded = true, s"$leg compacted base under Inferred")

      withWriteLayout(Unshredded) {
        runClustering(tableName, rowWriter = true)
      }
      val clusteringInstant = completedClusteringInstant(tablePath, leg)
      val clustered = baseLayouts(tablePath).filter(_.instantTime == clusteringInstant)
      assertAllShredded(clustered, shredded = false, s"$leg output of the clustering under Unshredded")
      checkAnswer(s"select id, cast(v as string), note from $tableName order by id")(
        Seq(1, """{"a":1,"b":"b1"}""", null),
        Seq(2, """{"a":2,"b":"b2"}""", null),
        Seq(3, """{"c":3,"d":true}""", "n3")
      )
    }
  }

  // -----------------------------------------------------------------------------------------------
  // H. Robustness
  // -----------------------------------------------------------------------------------------------

  test("Rollback and savepoint restore across layout changes") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    withVariantTable("rollback/savepoint", "cow", props = Seq(NEW_FILE_GROUP_PER_COMMIT),
      recordTypes = Seq(HoodieRecordType.SPARK)) { (tableName, tablePath, leg) =>
      val instants = seedMixedLayoutTable(tableName, tablePath, Seq(
        (Forced("a bigint, b string"), Seq((0 until 2, ObjA))),
        (Inferred, Seq((2 until 4, ObjB)))))
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
    }
  }

  // -----------------------------------------------------------------------------------------------
  // I. Inference
  // -----------------------------------------------------------------------------------------------

  test("Inference samples the head of a file; keys that only appear past the sample stay residual and read back") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    // The inferrer only ever sees the buffered head of a file: MAX_BUFFERED_RECORDS records, or
    // MAX_BUFFERED_BYTES (capped by the max file size) worth of them, whichever comes first.
    val cap = VariantShreddingInferenceFileWriter.MAX_BUFFERED_RECORDS
    val total = cap + 500
    val tailJson = """concat('{"a":', id, ',"b":"b', id, '","late":', id, '}')"""
    // Ids below the cap are plain ObjA - everything the sample can see; the tail adds a "late"
    // key the inferred typed_value therefore cannot carry.
    val rowJson = s"case when id < $cap then ${ObjA.jsonExpr} else $tailJson end"
    val sourceSql = s"select cast(id as int) as id, parse_json($rowJson) as v, 1000L as ts " +
      s"from range(0, $total, 1, 1)"

    def assertHeadAndTailReads(tableName: String, leg: String): Unit = {
      checkAnswer(s"select count(*) from $tableName")(Seq(total))
      // Whole-table round trip: every row renders what was written, typed keys and residual
      // "late" alike (a spot check would miss a systematically dropped residual). `is distinct
      // from` rather than `<>`: a row whose variant reconstructed as NULL would make `<>` NULL
      // too and slip past the count.
      checkAnswer(
        s"select count(*) from $tableName where cast(v as string) is distinct from $rowJson")(Seq(0))
      // $.late resolves out of the residual for every tail row...
      checkAnswer(s"select count(*) from $tableName " +
        s"where id >= $cap and coalesce(variant_get(v, '$$.late', 'int'), -1) <> id")(Seq(0))
      // ...and stays null over the sampled head, which never carried the key.
      checkAnswer(s"select count(*) from $tableName " +
        s"where id < $cap and variant_get(v, '$$.late', 'int') is not null")(Seq(0))
      checkAnswer(s"select id, cast(v as string) from $tableName where id in (0, ${total - 1}) order by id")(
        Seq(0, ObjA.expected(0)),
        Seq(total - 1, s"""{"a":${total - 1},"b":"b${total - 1}","late":${total - 1}}""")
      )
    }

    // The AVRO read reconstructs the residual rows through HoodieVariantReconstruction, SPARK
    // natively, so both record types have to serve the split.
    withVariantTable("head sample cow", "cow", props = Seq(NEW_FILE_GROUP_PER_COMMIT)) {
      (tableName, tablePath, leg) =>
      withWriteLayout(Inferred) {
        spark.sql(s"insert into $tableName $sourceSql")
      }
      val instant = latestCompletedInstant(tablePath)
      assertLayoutsByInstant(baseLayouts(tablePath), leg)(instant -> Some(Seq("a", "b")))
      assertHeadAndTailReads(tableName, leg)

      // The split is physical, not just a read-side artifact: every row types a and b, only the
      // tail rows also carry a root residual (the unmatched "late" key).
      val stats = baseLayouts(tablePath).map(l => inspectVariantRows(l.path))
      assert(stats.map(_.rows).sum == total, s"[$leg] rows: $stats")
      assert(stats.map(_.fieldTyped("a")).sum == total, s"[$leg] every row should type a: $stats")
      assert(stats.map(_.rootResidual).sum == total - cap,
        s"[$leg] only the tail rows should carry a root residual: $stats")
    }

    // MOR twin: the same rows in a base file, the late keys re-applied through a log file, then
    // a compaction that re-infers from the merged rows. Whatever the compacted base types, the
    // reads must hold.
    // SPARK pinned: AVRO compaction over a shredded base is swept by the MOR compaction and declines tests.
    withVariantTable("head sample mor", "mor",
      props = Seq(NEW_FILE_GROUP_PER_COMMIT, "hoodie.compact.inline = 'false'"),
      recordTypes = Seq(HoodieRecordType.SPARK)) { (tableName, tablePath, leg) =>
      withWriteLayout(Inferred) {
        spark.sql(s"insert into $tableName $sourceSql")
      }
      val baseInstant = latestCompletedInstant(tablePath)
      assertLayoutsByInstant(baseLayouts(tablePath), leg)(baseInstant -> Some(Seq("a", "b")))

      withWriteLayout(Inferred) {
        spark.sql(s"update $tableName set v = parse_json($tailJson), ts = 1001 where id >= $cap")
        runCompaction(tableName)
      }
      assertCompactionCount(tablePath, 1, leg)
      assertHeadAndTailReads(tableName, leg)
    }

    // The row writer buffers through a decorator of its own, sharing the same cap constants.
    // Record-type independent, SPARK pinned.
    withVariantTable("head sample row writer", "cow", props = Seq(NEW_FILE_GROUP_PER_COMMIT),
      recordTypes = Seq(HoodieRecordType.SPARK)) { (tableName, tablePath, leg) =>
      withBulkInsertRowWriter {
        withWriteLayout(Inferred) {
          spark.sql(s"insert into $tableName $sourceSql")
        }
      }
      assertBulkInsertOperation(tablePath, leg)
      val instant = latestCompletedInstant(tablePath)
      assertLayoutsByInstant(baseLayouts(tablePath), leg)(instant -> Some(Seq("a", "b")))
      assertHeadAndTailReads(tableName, leg)
    }
  }

  test("An insert that rolls over infers each file on its own and keeps every row") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    // The inference buffer's byte cap is min(MAX_BUFFERED_BYTES, max file size), so with a 256KB
    // max file size the cap coincides with the file-size rollover by construction - materializing
    // the buffer mid-insert is not what this leg pins. What it pins is the rollover itself: the
    // insert spreads over several base files, each infers its own schema, and the record counts
    // and distinct ids across those files still add up to the input.
    // Dictionary encoding is off on purpose: the pad is the same 1KB string in every row, and a
    // one-entry dictionary keeps the writer's data-size estimate near zero, so the file-size
    // rollover the row-writer leg depends on would never trip.
    val maxFileSize = 256 * 1024
    val rows = 2000
    val pad = "x" * 1024
    val padJson = """concat('{"a":', id, ',"pad":"', repeat('x', 1024), '"}')"""
    val sourceSql = s"select cast(id as int) as id, parse_json($padJson) as v, 1000L as ts " +
      s"from range(0, $rows, 1, 1)"
    val props = Seq(
      NEW_FILE_GROUP_PER_COMMIT,
      s"hoodie.parquet.max.file.size = '$maxFileSize'",
      "hoodie.parquet.dictionary.enabled = 'false'")

    def assertRolledOver(tableName: String, tablePath: String, leg: String): Unit = {
      val layouts = baseLayouts(tablePath)
      assert(layouts.size > 1,
        s"[$leg] the insert should have rolled over into several base files, got ${layouts.map(_.path)}")
      layouts.foreach { layout =>
        assert(layout.isShredded, s"[$leg] every base file should infer its own schema: ${layout.path}")
        assert(layout.typedFields.toSet == Set("a", "pad"),
          s"[$leg] typed_value should carry a and pad, got ${layout.typedFields}: ${layout.path}")
      }
      // Nothing is dropped or duplicated across the rollover.
      checkAnswer(s"select count(*), count(distinct id) from $tableName")(Seq(rows, rows))
      assertResult(rows.toLong)(getLastCommitMetadata(spark, tablePath).fetchTotalRecordsWritten)
      checkAnswer(s"select id, cast(v as string) from $tableName where id in (0, 999, 1999) order by id")(
        Seq(0, s"""{"a":0,"pad":"$pad"}"""),
        Seq(999, s"""{"a":999,"pad":"$pad"}"""),
        Seq(1999, s"""{"a":1999,"pad":"$pad"}""")
      )
    }

    withVariantTable("buffer cap", "cow", props = props) { (tableName, tablePath, leg) =>
      withWriteLayout(Inferred) {
        spark.sql(s"insert into $tableName $sourceSql")
      }
      assertRolledOver(tableName, tablePath, leg)
    }

    withVariantTable("buffer cap row writer", "cow", props = props,
      recordTypes = Seq(HoodieRecordType.SPARK)) { (tableName, tablePath, leg) =>
      withBulkInsertRowWriter {
        withWriteLayout(Inferred) {
          spark.sql(s"insert into $tableName $sourceSql")
        }
      }
      assertBulkInsertOperation(tablePath, leg)
      assertRolledOver(tableName, tablePath, leg)
    }
  }

  test("The inference tblproperty alone reaches run_clustering by table name") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    // No withWriteLayout and no session confs anywhere in this test. The flag is a WRITE config,
    // not a table config, so it is never persisted in hoodie.properties: SQL DML and procedures
    // called by TABLE NAME pick it up from the table's catalog properties, while path-based
    // procedures, the DataSource writer and the streamer have to be handed it explicitly. The
    // DML half is pinned by TestVariantDataType's inference tests; this one is about the
    // procedure.
    withVariantTable("tblproperty inference", "cow",
      props = Seq(NEW_FILE_GROUP_PER_COMMIT, s"$INFERENCE_KEY = 'true'"),
      recordTypes = Seq(HoodieRecordType.SPARK)) { (tableName, tablePath, leg) =>
      spark.sql(s"insert into $tableName ${variantSourceSql(Seq((0 until 4, ObjA)))}")

      // The procedure resolves the table by name, so the same catalog properties reach the
      // clustering write: no options are passed here at all.
      spark.sql(s"call run_clustering(table => '$tableName')")
      val clusteringInstant = completedClusteringInstant(tablePath, leg)
      assertLayoutsByInstant(baseLayouts(tablePath), leg)(clusteringInstant -> Some(Seq("a", "b")))
      assertVariantSegments(tableName, leg, Seq(("v", Seq((0 until 4, ObjA)))))
    }
  }

  test("Nested variants stay unshredded under inference") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    // Inference is top-level only (VariantSchemaUtils.getInferableVariantColumns), so the nested
    // variant keeps the plain {metadata, value} shape in the very file where the top-level one
    // shredded. Only the forced hook shreds below the top level, and only through the row writer.
    val nestedSourceSql = s"select cast(id as int) as id, parse_json(${ObjA.jsonExpr}) as v, " +
      s"named_struct('inner', parse_json(${ObjA.jsonExpr})) as s, 1000L as ts from range(0, 4, 1, 1)"

    def assertNestedUnshredded(tableName: String, tablePath: String, leg: String): Unit = {
      val instant = latestCompletedInstant(tablePath)
      assertLayoutsByInstant(baseLayouts(tablePath), leg)(instant -> Some(Seq("a", "b")))
      assertVariantLayout(tablePath, shredded = false, leg, column = "s.inner")
      checkAnswer(s"select id, cast(v as string), cast(s.inner as string) from $tableName order by id")(
        (0 until 4).map(id => Seq(id, ObjA.expected(id), ObjA.expected(id))): _*)
    }

    withVariantTable("nested inference", "cow", props = Seq(NEW_FILE_GROUP_PER_COMMIT),
      extraCols = "s struct<inner: variant>") { (tableName, tablePath, leg) =>
      withWriteLayout(Inferred) {
        spark.sql(s"insert into $tableName $nestedSourceSql")
      }
      assertNestedUnshredded(tableName, tablePath, leg)
    }

    // The row writer is the one production writer that CAN shred a nested variant; under
    // inference it must not, because inference never asks it to.
    withVariantTable("nested inference row writer", "cow", props = Seq(NEW_FILE_GROUP_PER_COMMIT),
      extraCols = "s struct<inner: variant>", recordTypes = Seq(HoodieRecordType.SPARK)) {
      (tableName, tablePath, leg) =>
      withBulkInsertRowWriter {
        withWriteLayout(Inferred) {
          spark.sql(s"insert into $tableName $nestedSourceSql")
        }
      }
      assertBulkInsertOperation(tablePath, leg)
      assertNestedUnshredded(tableName, tablePath, leg)
    }
  }

  test("Files whose inference declined sit next to inferred ones; reads and compaction span them") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    // Read-mode heavy; SPARK pinned for the COW half (see the mixed-files test above).
    withVariantTable("declined beside inferred cow", "cow", props = Seq(NEW_FILE_GROUP_PER_COMMIT),
      recordTypes = Seq(HoodieRecordType.SPARK)) { (tableName, tablePath, leg) =>
      val objSegments: Seq[(Range, VariantShape)] = Seq((0 until 4, ObjA))
      val nullSegments: Seq[(Range, VariantShape)] = Seq((4 until 8, JsonNull), (8 until 10, SqlNull))
      val instants = seedMixedLayoutTable(tableName, tablePath, Seq(
        (Inferred, objSegments),
        (Inferred, nullSegments)))
      // Pinned before the empty-object commit: assertVariantSegments spans the whole table and
      // there is no shape for {}.
      assertVariantSegments(tableName, leg, Seq(("v", objSegments ++ nullSegments)))
      // JSON-null rows are non-null variants; only the two SQL NULL rows may count, so a declined
      // file that reconstructed as all-null would show up here.
      checkAnswer(s"select count(*) from $tableName where v is null")(Seq(2))

      withWriteLayout(Inferred) {
        spark.sql(s"insert into $tableName select cast(id as int) as id, parse_json('{}') as v, " +
          "1000L as ts from range(10, 13, 1, 1)")
      }
      val emptyObjectInstant = latestCompletedInstant(tablePath)
      // Inference declines a file of nulls and a file of empty objects (nothing survives the
      // sample), and those files sit unshredded beside the inferred one.
      assertLayoutsByInstant(baseLayouts(tablePath), leg)(
        instants(0) -> Some(Seq("a", "b")),
        instants(1) -> None,
        emptyObjectInstant -> None)

      checkAnswer(s"select id, cast(v as string) from $tableName where id >= 10 order by id")(
        Seq(10, "{}"),
        Seq(11, "{}"),
        Seq(12, "{}")
      )
      // Incremental over the full range spans all three files; ids 0-3 and 10-12 carry values,
      // the null file is pinned by the is-null count above.
      val incRows = incrementalIdAndVariant(tablePath)
      assert(incRows.length == 13,
        s"[$leg] incremental over the full range should span all three files, got ${incRows.length}")
      incRows.foreach { row =>
        val id = row.getInt(0)
        val expected = if (id < 10) expectedVariantString(objSegments ++ nullSegments, id) else "{}"
        val actual = if (row.isNullAt(1)) null else row.getString(1)
        assert(actual == expected, s"[$leg] incremental id=$id: expected $expected, got $actual")
      }

      // A file of root scalars is the third inference outcome beside inferred-object and
      // declined: Spark types the root and Spark41VariantShreddingSchemaInferrer.sanitizeTypedValue
      // passes scalars through, so the file IS shredded, with a typed_value that is a leaf rather
      // than an object.
      withWriteLayout(Inferred) {
        spark.sql(s"insert into $tableName ${variantSourceSql(Seq((13 until 16, RootScalar)))}")
      }
      val scalarInstant = latestCompletedInstant(tablePath)
      val scalarFiles = baseLayouts(tablePath).filter(_.instantTime == scalarInstant)
      assert(scalarFiles.nonEmpty, s"[$leg] the scalar insert should have written a base file")
      scalarFiles.foreach { layout =>
        assert(layout.typedValue.exists(_.isPrimitive),
          s"[$leg] an all-scalar file should shred with a scalar typed_value, not decline: " +
            s"${layout.typedValue}: ${layout.path}")
        // Spark reads the JSON integers as Decimal(2,0) and finalizeSimpleSchema widens that
        // back to a long, so the leaf lands on parquet INT64.
        assert(layout.typedValue.exists(_.asPrimitiveType().getPrimitiveTypeName == PrimitiveTypeName.INT64),
          s"[$leg] the scalar typed_value should be an INT64 leaf, got ${layout.typedValue}: ${layout.path}")
      }
      checkAnswer(s"select id, cast(v as string) from $tableName where id >= 13 order by id")(
        (13 until 16).map(id => Seq(id, RootScalar.expected(id))): _*)
    }

    // MOR twin: an inferred base file, a log file the inference declined, and a compaction that
    // has to merge across the two.
    withVariantTable("declined beside inferred mor", "mor",
      props = Seq(NEW_FILE_GROUP_PER_COMMIT, "hoodie.compact.inline = 'false'")) { (tableName, tablePath, leg) =>
      withWriteLayout(Inferred) {
        spark.sql(s"insert into $tableName ${variantSourceSql(Seq((0 until 10, ObjA)))}")
      }
      val baseInstant = latestCompletedInstant(tablePath)
      assertLayoutsByInstant(baseLayouts(tablePath), leg)(baseInstant -> Some(Seq("a", "b")))

      withWriteLayout(Inferred) {
        spark.sql(s"update $tableName set v = parse_json('null'), ts = 1001 where id < 5")
      }
      val logInstant = latestCompletedInstant(tablePath)
      assertLayoutsByInstant(nativeLogLayouts(tablePath), leg)(logInstant -> None)

      withWriteLayout(Inferred) {
        runCompaction(tableName)
      }
      assertCompactionCount(tablePath, 1, leg)
      assertVariantSegments(tableName, leg, Seq(("v", Seq((0 until 5, JsonNull), (5 until 10, ObjA)))))
      // The updated rows hold a JSON null, not a SQL NULL variant: the merge kept the value the
      // declined log file wrote, it did not lose the column.
      checkAnswer(s"select count(*) from $tableName where v is null")(Seq(0))
    }
  }

  test("Inference types decimal leaves and they read back") {
    assume(HoodieSparkUtils.gteqSpark4_1, SPARK_4_1_GATE)

    // Issue #19442 reports FIXED-typed typed_value broken on the Avro paths; an inferred decimal
    // leaf is how that type reaches a typed_value without a forced schema, so pin that it still
    // round-trips today.
    withVariantTable("inferred decimal", "cow") { (tableName, tablePath, leg) =>
      withWriteLayout(Inferred) {
        spark.sql(s"insert into $tableName select cast(id as int) as id, parse_json(" +
          """concat('{"amt":', cast(id as string), '.25,"n":', cast(id as string), '}')) as v, """ +
          "1000L as ts from range(0, 4, 1, 1)")
      }
      assertLayoutsByInstant(baseLayouts(tablePath), leg)(
        latestCompletedInstant(tablePath) -> Some(Seq("amt", "n")))
      checkAnswer(s"select id, cast(v as string), variant_get(v, '$$.amt', 'decimal(5,2)') " +
        s"from $tableName order by id")(
        (0 until 4).map(id =>
          Seq(id, s"""{"amt":$id.25,"n":$id}""", new java.math.BigDecimal(s"$id.25"))): _*)
    }
  }
}
