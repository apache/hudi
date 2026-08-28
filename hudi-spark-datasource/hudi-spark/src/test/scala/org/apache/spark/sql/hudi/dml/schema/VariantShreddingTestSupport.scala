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

import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.model.WriteOperationType
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path => HadoopPath}
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetReader}
import org.apache.parquet.hadoop.api.ReadSupport
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.{GroupType, LogicalTypeAnnotation, MessageType, Type}
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.datasources.parquet.VariantParquetTestFixtures.{listElement, mapValue}
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase.getLastCommitMetadata

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Shared helpers for variant-shredding tests: parquet-footer layout inspection, a row-level
 * typed-vs-residual inspector, a shape-drift data generator, and a write-layout toggle. Mixed
 * into [[TestVariantDataType]] and [[TestVariantShreddingMixedLayouts]].
 */
trait VariantShreddingTestSupport { self: HoodieSparkSqlTestBase =>

  import VariantShreddingTestSupport._

  /**
   * The `(id int, v variant, ts long)` table both suites use, with the knobs they vary.
   */
  protected def createVariantTable(tableName: String,
                                   tablePath: String,
                                   tableType: String,
                                   props: Seq[String] = Seq.empty,
                                   extraCols: String = ""): Unit = {
    val extraColsDdl = if (extraCols.isEmpty) "" else s"$extraCols,"
    val extraProps = if (props.isEmpty) "" else props.mkString(",\n  ", ",\n  ", "")
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  v variant,
         |  $extraColsDdl
         |  ts long
         |) using hudi
         | location '$tablePath'
         | tblproperties (
         |  primaryKey = 'id',
         |  preCombineField = 'ts',
         |  type = '$tableType'$extraProps
         | )
       """.stripMargin)
  }

  /**
   * The scaffold nearly every leg opens with: sweep the record types, take a temp dir, generate a
   * table name, create the table in it through `create` and hand the body the
   * (tableName, tablePath, leg) triple, where `leg` is `"<label>, <tableName>"`.
   */
  private def withTableScaffold[T](label: String, recordTypes: Seq[HoodieRecordType])
                                  (create: (String, String) => Unit)
                                  (f: (String, String, String) => T): Unit = {
    withRecordType(recordTypes)(withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = tmp.getCanonicalPath
      create(tableName, tablePath)
      f(tableName, tablePath, s"$label, $tableName")
    })
  }

  /** [[withTableScaffold]] over the `(id int, v variant, ts long)` table of [[createVariantTable]]. */
  protected def withVariantTable[T](label: String,
                                    tableType: String,
                                    props: Seq[String] = Seq.empty,
                                    extraCols: String = "",
                                    recordTypes: Seq[HoodieRecordType] =
                                      Seq(HoodieRecordType.AVRO, HoodieRecordType.SPARK))
                                   (f: (String, String, String) => T): Unit = {
    withTableScaffold(label, recordTypes) { (tableName, tablePath) =>
      createVariantTable(tableName, tablePath, tableType, props = props, extraCols = extraCols)
    }(f)
  }

  /**
   * Creates `(id int, s struct<inner: variant>, ts long)` and bulk-inserts row 1 through the row
   * writer under a forced `k string` nested schema. Both forced hooks recurse into record members
   * (the row writer's always did, the Avro write support's since the #19689 fix), so a nested
   * variant shreds on either write path; inference alone stays top-level, and a variant that is
   * directly an array element or a map value shreds only where the write schema already declares
   * it. This row-writer seed is what the nested-shredding legs open on.
   */
  private def createNestedVariantRowWriterTable(tableName: String, tablePath: String): Unit = {
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
  }

  /**
   * [[withVariantTable]] for the nested case: the table and its seed row come from
   * [[createNestedVariantRowWriterTable]], so the body opens on a nested-shredded base file.
   */
  protected def withNestedVariantTable[T](label: String,
                                          recordTypes: Seq[HoodieRecordType] =
                                            Seq(HoodieRecordType.AVRO, HoodieRecordType.SPARK))
                                         (f: (String, String, String) => T): Unit = {
    withTableScaffold(label, recordTypes)(createNestedVariantRowWriterTable)(f)
  }

  // ---------------------------------------------------------------------------------------------
  // Parquet footer helpers
  // ---------------------------------------------------------------------------------------------

  /** Lists data parquet files in the table directory, excluding Hudi metadata files. */
  protected def listDataParquetFiles(tablePath: String): Seq[String] = {
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(new HadoopPath(tablePath).toUri, conf)
    val iter = fs.listFiles(new HadoopPath(tablePath), true)
    val files = mutable.ArrayBuffer[String]()
    while (iter.hasNext) {
      val file = iter.next()
      val path = file.getPath.toString
      if (path.endsWith(".parquet") && !path.contains(".hoodie")) {
        files += path
      }
    }
    files.toSeq
  }

  /** Reads the Parquet schema (MessageType) from a parquet file. */
  protected def readParquetSchema(filePath: String): MessageType = {
    val conf = spark.sparkContext.hadoopConfiguration
    val inputFile = HadoopInputFile.fromPath(new HadoopPath(filePath), conf)
    val reader = ParquetFileReader.open(inputFile)
    try {
      reader.getFooter.getFileMetaData.getSchema
    } finally {
      reader.close()
    }
  }

  /**
   * Gets a named field from a GroupType (MessageType) and returns it as a GroupType.
   * Uses getFieldIndex(String) + getType(int) to avoid Scala overload resolution issues.
   */
  protected def getFieldAsGroup(parent: GroupType, fieldName: String): GroupType = {
    val idx: Int = parent.getFieldIndex(fieldName)
    parent.getType(idx).asGroupType()
  }

  /**
   * The variant group of `column` inside one parquet file. `column` may be a dotted path
   * ("s.inner"), walked one member per segment, so a nested variant is inspected the same way as
   * a top-level one. A segment that lands on a LIST or MAP group is stepped through its wrapper
   * to what it holds, so "items.inner" reaches the `inner` member of the element struct of an
   * `array<struct<inner: variant>>` and "arr" reaches the element of an `array<variant>`.
   */
  protected def variantGroupOf(filePath: String, column: String): GroupType =
    column.split('.').foldLeft(readParquetSchema(filePath): GroupType) {
      (group, segment) => collectionElementOf(getFieldAsGroup(group, segment))
    }

  /**
   * Steps a LIST or MAP group down to the group it holds - a list element or a map value - and
   * returns anything else, a variant group included, unchanged. The two write paths disagree on
   * the list layout (the Spark writer emits the 3-level `list`/`element` shape, parquet-avro the
   * 2-level one whose repeated group, named "array" or "<field>_tuple", IS the element), so the
   * step delegates to the read-side guards' own rule rather than restating it.
   */
  private def collectionElementOf(group: GroupType): GroupType =
    group.getLogicalTypeAnnotation match {
      case _: LogicalTypeAnnotation.ListLogicalTypeAnnotation =>
        collectionElementOf(listElement(group).asGroupType())
      case _: LogicalTypeAnnotation.MapLogicalTypeAnnotation =>
        collectionElementOf(mapValue(group).asGroupType())
      case _ => group
    }

  /**
   * Pins the on-disk layout of `column` (a name or a dotted path) across every data parquet file.
   * Without it a leg meant to exercise the shredded path can silently degenerate into the
   * unshredded one, or the reverse, and the branch it was written for goes uncovered.
   */
  protected def assertVariantLayout(tablePath: String, shredded: Boolean, leg: String,
                                    column: String = "v"): Unit = {
    val files = listDataParquetFiles(tablePath)
    assert(files.nonEmpty, s"[$leg] should have at least one data parquet file")
    files.foreach { filePath =>
      val variantGroup = variantGroupOf(filePath, column)
      if (shredded) {
        assert(variantGroup.containsField("typed_value"),
          s"[$leg] base file should carry typed_value. Schema:\n$variantGroup")
      } else {
        assert(!variantGroup.containsField("typed_value"),
          s"[$leg] base file must not carry typed_value. Schema:\n$variantGroup")
      }
    }
  }

  /**
   * Pins an INFERRED shredding layout of `column` across every data parquet file of the table:
   * see [[assertInferredTypedValueIn]].
   */
  protected def assertInferredTypedValue(tablePath: String, column: String, leg: String,
                                         present: Seq[String], absent: Seq[String] = Seq.empty): Unit = {
    assertInferredTypedValueIn(listDataParquetFiles(tablePath), column, leg, present, absent)
  }

  /**
   * Pins an INFERRED shredding layout of `column` in the given parquet files: the variant group is
   * shredded, carries the VARIANT logical type (the inferrer only exists on Spark 4.1+, whose
   * parquet ships the annotation), its typed_value has every `present` member and none of
   * the `absent` ones (e.g. an avro-illegal key the inferrer dropped), and typed_value holds
   * non-null values per the block column statistics.
   */
  protected def assertInferredTypedValueIn(files: Seq[String], column: String, leg: String,
                                           present: Seq[String], absent: Seq[String] = Seq.empty): Unit = {
    assert(files.nonEmpty, s"[$leg] should have at least one data parquet file")
    files.foreach { filePath =>
      val variantGroup = getFieldAsGroup(readParquetSchema(filePath), column)
      assert(variantGroup.containsField("typed_value"),
        s"[$leg] $column should be shredded with an inferred typed_value. File: $filePath Schema:\n$variantGroup")
      assert(Option(variantGroup.getLogicalTypeAnnotation).exists(_.toString.contains("VARIANT")),
        s"[$leg] inferred variant group must carry the VARIANT logical type. Schema:\n$variantGroup")
      val typedValue = getFieldAsGroup(variantGroup, "typed_value")
      present.foreach(member => assert(typedValue.containsField(member),
        s"[$leg] inferred typed_value should contain $member. Schema:\n$typedValue"))
      absent.foreach(member => assert(!typedValue.containsField(member),
        s"[$leg] inferred typed_value must not contain $member. Schema:\n$typedValue"))
      // The footer schema alone cannot tell real shredding from a file where every row fell
      // back to the residual value with typed_value all-null.
      assert(typedValueNonNullCount(filePath, column) > 0,
        s"[$leg] inferred typed_value of $column should hold non-null values. File: $filePath")
    }
  }

  /**
   * Sums the non-null value counts of the typed leaf columns under `column`.typed_value across
   * all blocks of the file, from the block column statistics. The residual `value` leaves of
   * shredded object fields are left out: they hold the rows that fell back, so counting them
   * would let a file with every field fallen back pass as shredded.
   */
  private def typedValueNonNullCount(filePath: String, column: String): Long = {
    val conf = spark.sparkContext.hadoopConfiguration
    val inputFile = HadoopInputFile.fromPath(new HadoopPath(filePath), conf)
    val reader = ParquetFileReader.open(inputFile)
    try {
      val prefix = s"$column.typed_value"
      reader.getFooter.getBlocks.asScala.flatMap(_.getColumns.asScala)
        .filter { c =>
          val dot = c.getPath.toDotString
          (dot == prefix || dot.startsWith(prefix + ".")) && !dot.endsWith(".value")
        }
        .map { c =>
          val stats = c.getStatistics
          // A chunk without a written null count must not pass as holding non-null values
          if (stats == null || stats.getNumNulls < 0) 0L else c.getValueCount - stats.getNumNulls
        }
        .sum
    } finally {
      reader.close()
    }
  }

  /** Pins that a write bin-packed into the existing file group rather than creating a new one. */
  protected def assertSingleFileGroup(tablePath: String, leg: String): Unit = {
    val fileGroupIds = listDataParquetFiles(tablePath)
      .map(f => FSUtils.getFileId(new HadoopPath(f).getName)).distinct
    assert(fileGroupIds.size == 1,
      s"[$leg] insert should bin-pack into the first file group via the small-file merge, got: $fileGroupIds")
  }

  // ---------------------------------------------------------------------------------------------
  // Per-file layout map
  // ---------------------------------------------------------------------------------------------

  /**
   * The on-disk variant layout of one data parquet file: base file or native parquet log file,
   * the instant that wrote it, and the typed_value subtree of `column` if the file shredded it.
   * `hasColumn` is false when the file predates the column (schema evolution).
   */
  case class VariantFileLayout(path: String,
                               isBaseFile: Boolean,
                               instantTime: String,
                               fileId: String,
                               hasColumn: Boolean,
                               typedValue: Option[Type]) {
    def isShredded: Boolean = typedValue.isDefined

    /** Member names of an object-shaped typed_value; Nil for scalar/list typed_value. */
    def typedFields: Seq[String] = typedValue match {
      case Some(t) if !t.isPrimitive => t.asGroupType().getFields.asScala.map(_.getName).toSeq
      case _ => Seq.empty
    }
  }

  /**
   * Maps every data parquet file of the table (base files and native `*.log.parquet` data logs;
   * delete/CDC logs carry no data column and parse as no-column files) to its variant layout.
   */
  protected def variantFileLayouts(tablePath: String, column: String = "v"): Seq[VariantFileLayout] = {
    listDataParquetFiles(tablePath).map { filePath =>
      val name = new HadoopPath(filePath).getName
      val isBase = FSUtils.isBaseFile(name)
      val instant = FSUtils.getCommitTime(name)
      val fileId = if (isBase) {
        FSUtils.getFileId(name)
      } else {
        FSUtils.getFileIdFromLogPath(new StoragePath(filePath))
      }
      val schema = readParquetSchema(filePath)
      if (!schema.containsField(column)) {
        VariantFileLayout(filePath, isBase, instant, fileId, hasColumn = false, None)
      } else {
        val fieldType = schema.getType(schema.getFieldIndex(column))
        val typedValue = if (!fieldType.isPrimitive) {
          val group = fieldType.asGroupType()
          if (group.containsField("typed_value")) {
            Some(group.getType(group.getFieldIndex("typed_value")))
          } else {
            None
          }
        } else {
          None
        }
        VariantFileLayout(filePath, isBase, instant, fileId, hasColumn = true, typedValue)
      }
    }
  }

  /** The base-file subset of [[variantFileLayouts]]. */
  protected def baseLayouts(tablePath: String, column: String = "v"): Seq[VariantFileLayout] =
    variantFileLayouts(tablePath, column).filter(_.isBaseFile)

  /** The native parquet data-log subset of [[variantFileLayouts]]. */
  protected def nativeLogLayouts(tablePath: String, column: String = "v"): Seq[VariantFileLayout] =
    variantFileLayouts(tablePath, column).filter(l => !l.isBaseFile && l.path.endsWith(".log.parquet"))

  /**
   * Pins that `layouts` is non-empty and every file in it is shredded (or none is). `leg` names
   * the file set being checked, e.g. "<leg> compacted base under Inferred".
   */
  protected def assertAllShredded(layouts: Seq[VariantFileLayout], shredded: Boolean, leg: String): Unit = {
    assert(layouts.nonEmpty, s"[$leg] expected at least one file to check, got none")
    val expected = if (shredded) "shredded" else "unshredded"
    val wrong = layouts.filter(_.isShredded != shredded)
    assert(wrong.isEmpty, s"[$leg] every file must be $expected, these are not: ${wrong.map(_.path)}")
  }

  /**
   * Pins the layout of every file written by each listed instant: None = unshredded,
   * Some(fields) = shredded with exactly that object-shaped typed_value member set.
   */
  protected def assertLayoutsByInstant(layouts: Seq[VariantFileLayout], leg: String)
                                      (expected: (String, Option[Seq[String]])*): Unit = {
    expected.foreach { case (instant, expectedFields) =>
      val files = layouts.filter(_.instantTime == instant)
      assert(files.nonEmpty,
        s"[$leg] expected at least one data parquet file for instant $instant, " +
          s"got instants: ${layouts.map(_.instantTime).distinct.sorted}")
      files.foreach { layout =>
        expectedFields match {
          case None =>
            assert(!layout.isShredded,
              s"[$leg] file of instant $instant must be unshredded: ${layout.path}")
          case Some(fields) =>
            assert(layout.isShredded,
              s"[$leg] file of instant $instant must be shredded: ${layout.path}")
            assert(layout.typedFields.toSet == fields.toSet,
              s"[$leg] typed_value of instant $instant should have fields $fields, " +
                s"got ${layout.typedFields}: ${layout.path}")
        }
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Row-level typed-vs-residual inspector
  // ---------------------------------------------------------------------------------------------

  /**
   * Physical per-row stats of `column` inside ONE parquet file. Counts rows by which physical
   * slot holds the value: `rootTyped` rows have a non-null typed_value, `rootResidual` rows a
   * non-null value binary (both can hold for partially shredded objects). For an object-shaped
   * typed_value, `fieldTyped`/`fieldResidual` count per member how many rows carry the member
   * in its typed_value leaf vs its per-field value residual.
   */
  case class VariantRowStats(rows: Int,
                             nullVariants: Int,
                             rootTyped: Int,
                             rootResidual: Int,
                             fieldTyped: Map[String, Int],
                             fieldResidual: Map[String, Int])

  /**
   * Reads `file` with the plain parquet-hadoop example reader (no Hudi, no Spark) and returns
   * [[VariantRowStats]] for `column`. The Group reader is annotation-agnostic, so it works for
   * VARIANT-annotated files (parquet 1.16+) and unannotated ones alike, and it exposes the
   * physical typed_value/value nulls that Hudi and Spark readers deliberately hide.
   */
  protected def inspectVariantRows(file: String, column: String = "v"): VariantRowStats = {
    val fileSchema = readParquetSchema(file)
    assert(fileSchema.containsField(column), s"file has no column $column: $file\n$fileSchema")
    val columnType = fileSchema.getType(fileSchema.getFieldIndex(column))
    assert(!columnType.isPrimitive, s"column $column is not a variant group in $file")
    val variantGroup = columnType.asGroupType()
    val typedValueType: Option[Type] = if (variantGroup.containsField("typed_value")) {
      Some(variantGroup.getType(variantGroup.getFieldIndex("typed_value")))
    } else {
      None
    }
    // Object-shaped typed_value: members are {value, typed_value} wrapper groups. A list or
    // scalar typed_value has no such members and only feeds the root counters. Each member
    // records which of the two wrapper slots its schema declares, so a value-only wrapper
    // (a declined field) never triggers a lookup of a field its schema does not have.
    val objectMembers: Seq[(String, Boolean, Boolean)] = typedValueType match {
      case Some(t) if !t.isPrimitive =>
        t.asGroupType().getFields.asScala.flatMap { member =>
          if (member.isPrimitive) {
            None
          } else {
            val wrapper = member.asGroupType()
            val hasTyped = wrapper.containsField("typed_value")
            val hasValue = wrapper.containsField("value")
            if (hasTyped || hasValue) Some((member.getName, hasTyped, hasValue)) else None
          }
        }.toSeq
      case _ => Seq.empty
    }

    val conf = new Configuration(spark.sparkContext.hadoopConfiguration)
    val projected = new MessageType(fileSchema.getName, columnType)
    conf.set(ReadSupport.PARQUET_READ_SCHEMA, projected.toString)
    val reader = ParquetReader.builder(new GroupReadSupport(), new HadoopPath(file))
      .withConf(conf)
      .build()
    var rows = 0
    var nullVariants = 0
    var rootTyped = 0
    var rootResidual = 0
    val fieldTyped = mutable.Map[String, Int]().withDefaultValue(0)
    val fieldResidual = mutable.Map[String, Int]().withDefaultValue(0)
    try {
      var group: Group = reader.read()
      while (group != null) {
        rows += 1
        if (group.getFieldRepetitionCount(column) == 0) {
          nullVariants += 1
        } else {
          val v = group.getGroup(column, 0)
          val hasTyped = typedValueType.isDefined && v.getFieldRepetitionCount("typed_value") > 0
          if (hasTyped) {
            rootTyped += 1
          }
          if (variantGroup.containsField("value") && v.getFieldRepetitionCount("value") > 0) {
            rootResidual += 1
          }
          if (hasTyped && objectMembers.nonEmpty) {
            val tv = v.getGroup("typed_value", 0)
            objectMembers.foreach { case (member, wrapperHasTyped, wrapperHasValue) =>
              if (tv.getFieldRepetitionCount(member) > 0) {
                val wrapper = tv.getGroup(member, 0)
                if (wrapperHasTyped && wrapper.getFieldRepetitionCount("typed_value") > 0) {
                  fieldTyped(member) += 1
                }
                if (wrapperHasValue && wrapper.getFieldRepetitionCount("value") > 0) {
                  fieldResidual(member) += 1
                }
              }
            }
          }
        }
        group = reader.read()
      }
    } finally {
      reader.close()
    }
    VariantRowStats(rows, nullVariants, rootTyped, rootResidual, fieldTyped.toMap, fieldResidual.toMap)
  }

  // ---------------------------------------------------------------------------------------------
  // Shape-drift data generator
  // ---------------------------------------------------------------------------------------------

  /**
   * A SQL source of `(id int, v variant, ts long)` rows over the union id span of all segments,
   * one input partition (`range(lo, hi, 1, 1)`) so a single-bucket write preserves row order.
   * Ids not covered by any segment get a SQL NULL variant.
   */
  protected def variantSourceSql(segments: Seq[(Range, VariantShape)],
                                 ts: String = "1000L"): String = {
    require(segments.nonEmpty, "at least one segment is required")
    val lo = segments.map(_._1.start).min
    val hi = segments.map(_._1.end).max
    val branches = segments
      .map { case (r, shape) => s"when id >= ${r.start} and id < ${r.end} then ${shape.jsonExpr}" }
      .mkString(" ")
    val variantCol = s"parse_json(case $branches else cast(null as string) end) as v"
    s"select cast(id as int) as id, $variantCol, $ts as ts from range($lo, $hi, 1, 1)"
  }

  /** The expected `cast(col as string)` value per id for a segment list; null when uncovered. */
  protected def expectedVariantString(segments: Seq[(Range, VariantShape)], id: Int): String =
    segments.collectFirst { case (r, shape) if r.contains(id) => shape.expected(id) }.orNull

  /**
   * Asserts that `select id, cast(<col> as string)... from tableName order by id` matches what
   * the generator segments produced, for the full id span. Use right after the initial insert;
   * updates invalidate the expectation.
   */
  protected def assertVariantSegments(tableName: String, leg: String,
                                      cols: Seq[(String, Seq[(Range, VariantShape)])]): Unit = {
    val allSegments = cols.flatMap(_._2)
    val lo = allSegments.map(_._1.start).min
    val hi = allSegments.map(_._1.end).max
    val selectCols = cols.map { case (name, _) => s"cast($name as string)" }.mkString(", ")
    val actual = spark.sql(s"select id, $selectCols from $tableName order by id").collect()
    assert(actual.length == hi - lo,
      s"[$leg] expected ${hi - lo} rows, got ${actual.length}")
    actual.foreach { row =>
      val id = row.getInt(0)
      cols.zipWithIndex.foreach { case ((name, segments), i) =>
        val expected = expectedVariantString(segments, id)
        val actualValue = if (row.isNullAt(i + 1)) null else row.getString(i + 1)
        assert(actualValue == expected,
          s"[$leg] id=$id column $name: expected $expected, got $actualValue")
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Read idioms
  // ---------------------------------------------------------------------------------------------

  /**
   * Incremental query over the whole timeline, as `(id, cast(v as string))` rows ordered by id.
   * Selecting the value rather than a count keeps a variant that reconstructed as all-null from
   * passing.
   */
  protected def incrementalIdAndVariant(tablePath: String): Array[Row] = {
    spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE.key, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.START_COMMIT.key, "000")
      .load(tablePath)
      .selectExpr("id", "cast(v as string)")
      .orderBy("id")
      .collect()
  }

  // ---------------------------------------------------------------------------------------------
  // Write-layout toggle
  // ---------------------------------------------------------------------------------------------

  /** The three write-side variant layout configs for a [[WriteLayout]]. */
  private def layoutConfs(layout: WriteLayout): Seq[(String, String)] = layout match {
    case Unshredded => Seq(
      WRITE_SHREDDING_KEY -> "false",
      FORCE_SCHEMA_KEY -> "",
      INFERENCE_KEY -> "false")
    case Forced(ddl) => Seq(
      WRITE_SHREDDING_KEY -> "true",
      FORCE_SCHEMA_KEY -> ddl,
      INFERENCE_KEY -> "false")
    case Inferred => Seq(
      WRITE_SHREDDING_KEY -> "true",
      FORCE_SCHEMA_KEY -> "",
      INFERENCE_KEY -> "true")
  }

  /**
   * Runs `f` with the session-level write-layout confs of `layout` set. Session confs reach SQL
   * DML and the run_compaction/run_clustering procedures alike, so this flips the layout for
   * writes AND table services inside `f`.
   */
  protected def withWriteLayout[T](layout: WriteLayout)(f: => T): T =
    withSQLConf(layoutConfs(layout): _*)(f)

  /** Runs `f` with SQL inserts routed through the bulk-insert row writer. */
  protected def withBulkInsertRowWriter[T](f: => T): T =
    withSQLConf("hoodie.spark.sql.insert.into.operation" -> "bulk_insert",
      "hoodie.datasource.write.row.writer.enable" -> "true")(f)

  /**
   * Pins that the last commit really took the row-writer path: the footer of a shredded file
   * looks the same when the insert falls back to the record-based writers, which infer through
   * the other buffering decorator.
   */
  protected def assertBulkInsertOperation(tablePath: String, leg: String): Unit = {
    val operation = getLastCommitMetadata(spark, tablePath).getOperationType
    assert(operation == WriteOperationType.BULK_INSERT,
      s"[$leg] insert should have taken the bulk-insert row writer, got $operation")
  }

  // ---------------------------------------------------------------------------------------------
  // Table-service idioms
  // ---------------------------------------------------------------------------------------------

  /**
   * The record types a clustering leg sweeps: the row-writer path is record-type independent; the
   * RDD path writes through the record-type file writer factories, so it sweeps both.
   */
  protected def clusteringRecordTypes(rowWriter: Boolean): Seq[HoodieRecordType] =
    if (rowWriter) Seq(HoodieRecordType.SPARK) else Seq(HoodieRecordType.AVRO, HoodieRecordType.SPARK)

  /** Asserts a COMPLETED clustering (replacecommit) instant exists and returns its time. */
  protected def completedClusteringInstant(tablePath: String, leg: String): String = {
    val metaClient = createMetaClient(spark, tablePath)
    val lastClustering = metaClient.getActiveTimeline.getLastClusteringInstant
    assert(lastClustering.isPresent && lastClustering.get.isCompleted,
      s"[$leg] a COMPLETED clustering (replacecommit) instant must exist")
    lastClustering.get.requestedTime
  }

  /** Asserts the latest clustering (replacecommit) instant exists and is NOT completed. */
  protected def assertPendingClustering(tablePath: String, leg: String): Unit = {
    val metaClient = createMetaClient(spark, tablePath)
    val lastClustering = metaClient.getActiveTimeline.getLastClusteringInstant
    assert(lastClustering.isPresent && !lastClustering.get.isCompleted,
      s"[$leg] a clustering (replacecommit) instant must exist and be left incomplete")
  }

  /** Asserts the number of completed compaction commits (MOR `commit` actions). */
  protected def assertCompactionCount(tablePath: String, expected: Int, leg: String): Unit = {
    val metaClient = createMetaClient(spark, tablePath)
    val commits = metaClient.getActiveTimeline.filterCompletedInstants
      .getInstantsAsStream.iterator().asScala.count(_.getAction == "commit")
    assert(commits == expected,
      s"[$leg] expected $expected completed compaction commits, got $commits")
  }

  /** The requested time of the latest completed commit-like instant, for time travel. */
  protected def latestCompletedInstant(tablePath: String): String = {
    val metaClient = createMetaClient(spark, tablePath)
    metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants
      .lastInstant.get.requestedTime
  }
}

object VariantShreddingTestSupport {

  val WRITE_SHREDDING_KEY = "hoodie.parquet.variant.write.shredding.enabled"
  val FORCE_SCHEMA_KEY = "hoodie.parquet.variant.force.shredding.schema.for.test"
  // Per-file shredding-schema inference (#18961); a writer with no inferrer ignores it.
  val INFERENCE_KEY = "hoodie.parquet.variant.shredding.schema.inference.enabled"

  /** How the writer lays out variant columns for one commit or table service. */
  sealed trait WriteLayout
  case object Unshredded extends WriteLayout
  case class Forced(ddl: String) extends WriteLayout
  case object Inferred extends WriteLayout

  /**
   * A JSON shape as a SQL expression over the `id` column of a `range()` scan, paired with the
   * canonical `cast(v as string)` rendering it reads back as.
   */
  sealed abstract class VariantShape(val jsonExpr: String) {
    def expected(id: Int): String
  }

  object VariantShape {
    /** Consistent object: a long and a string. */
    case object ObjA extends VariantShape("""concat('{"a":', id, ',"b":"b', id, '"}')""") {
      override def expected(id: Int): String = s"""{"a":$id,"b":"b$id"}"""
    }

    /** Object with keys disjoint from ObjA. */
    case object ObjB extends VariantShape("""concat('{"c":', id, ',"d":true}')""") {
      override def expected(id: Int): String = s"""{"c":$id,"d":true}"""
    }

    /** ObjA with `a` as a string: conflicts with ObjA's long `a`. */
    case object ObjAConflict extends VariantShape("""concat('{"a":"s', id, '","b":"b', id, '"}')""") {
      override def expected(id: Int): String = s"""{"a":"s$id","b":"b$id"}"""
    }

    /** Root-level scalar. */
    case object RootScalar extends VariantShape("cast(id as string)") {
      override def expected(id: Int): String = s"$id"
    }

    /**
     * JSON null (a non-null variant holding null). Casting it to string yields SQL NULL, same
     * as a null variant; the two are told apart physically ([[inspectVariantRows]] counts a
     * JSON null in rootResidual, a SQL NULL variant in nullVariants).
     */
    case object JsonNull extends VariantShape("'null'") {
      override def expected(id: Int): String = null
    }

    /** SQL NULL (a null variant). */
    case object SqlNull extends VariantShape("cast(null as string)") {
      override def expected(id: Int): String = null
    }
  }

  // tblproperties fragment for file-count control.
  val NEW_FILE_GROUP_PER_COMMIT = "hoodie.parquet.small.file.limit = '0'"
}
