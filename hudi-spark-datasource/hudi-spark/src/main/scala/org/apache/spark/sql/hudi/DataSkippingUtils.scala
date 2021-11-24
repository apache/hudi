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

package org.apache.spark.sql.hudi

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, EqualNullSafe, EqualTo, Expression, ExtractValue, GetStructField, GreaterThan, GreaterThanOrEqual, In, IsNotNull, IsNull, LessThan, LessThanOrEqual, Literal, Not, Or, StartsWith}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters._

object DataSkippingUtils {

  /**
   * Translates provided {@link filterExpr} into corresponding filter-expression for Z-index index table
   * to filter out candidate files that would hold records matching the original filter
   *
   * @param filterExpr  original filter from query
   * @param indexSchema index table schema
   * @return filter for Z-index table
   */
  def createZIndexLookupFilter(filterExpr: Expression, indexSchema: StructType): Expression = {

    def rewriteCondition(colName: Seq[String], conditionExpress: Expression): Expression = {
      val stats = Set.apply(
        UnresolvedAttribute(colName).name + "_minValue",
        UnresolvedAttribute(colName).name + "_maxValue",
        UnresolvedAttribute(colName).name + "_num_nulls"
      )

      if (stats.forall(stat => indexSchema.exists(_.name == stat))) {
        conditionExpress
      } else {
        Literal.TrueLiteral
      }
    }

    def refColExpr(colName: Seq[String], statisticValue: String): Expression =
      col(UnresolvedAttribute(colName).name + statisticValue).expr

    def minValue(colName: Seq[String]) = refColExpr(colName, "_minValue")
    def maxValue(colName: Seq[String]) = refColExpr(colName, "_maxValue")
    def numNulls(colName: Seq[String]) = refColExpr(colName, "_num_nulls")

    def colContainsValuesEqualToLiteral(colName: Seq[String], value: Literal) =
      And(LessThanOrEqual(minValue(colName), value), GreaterThanOrEqual(maxValue(colName), value))

    def colContainsValuesEqualToLiterals(colName: Seq[String], list: Seq[Literal]) =
      list.map { lit => colContainsValuesEqualToLiteral(colName, lit) }.reduce(Or)

    filterExpr match {
      // Filter "colA = b"
      // Translates to "colA_minValue <= b AND colA_maxValue >= b" condition for index lookup
      case EqualTo(attribute: AttributeReference, value: Literal) =>
        val colName = getTargetColNameParts(attribute)
        rewriteCondition(colName, colContainsValuesEqualToLiteral(colName, value))
      // Filter "b = colA"
      // Translates to "colA_minValue <= b AND colA_maxValue >= b" condition for index lookup
      case EqualTo(value: Literal, attribute: AttributeReference) =>
        val colName = getTargetColNameParts(attribute)
        rewriteCondition(colName, colContainsValuesEqualToLiteral(colName, value))
      // Filter "colA = null"
      // Translates to "colA_num_nulls = null" for index lookup
      case equalNullSafe @ EqualNullSafe(_: AttributeReference, _ @ Literal(null, _)) =>
        val colName = getTargetColNameParts(equalNullSafe.left)
        rewriteCondition(colName, EqualTo(numNulls(colName), equalNullSafe.right))
      // Filter "colA < b"
      // Translates to "colA_minValue < b" for index lookup
      case LessThan(attribute: AttributeReference, value: Literal) =>
        val colName = getTargetColNameParts(attribute)
        rewriteCondition(colName, LessThan(minValue(colName), value))
      // Filter "b < colA"
      // Translates to "b < colA_maxValue" for index lookup
      case LessThan(value: Literal, attribute: AttributeReference) =>
        val colName = getTargetColNameParts(attribute)
        rewriteCondition(colName, GreaterThan(maxValue(colName), value))
      // Filter "colA > b"
      // Translates to "colA_maxValue > b" for index lookup
      case GreaterThan(attribute: AttributeReference, value: Literal) =>
        val colName = getTargetColNameParts(attribute)
        rewriteCondition(colName, GreaterThan(maxValue(colName), value))
      // Filter "b > colA"
      // Translates to "b > colA_minValue" for index lookup
      case GreaterThan(value: Literal, attribute: AttributeReference) =>
        val colName = getTargetColNameParts(attribute)
        rewriteCondition(colName, LessThan(minValue(colName), value))
      // Filter "colA <= b"
      // Translates to "colA_minValue <= b" for index lookup
      case LessThanOrEqual(attribute: AttributeReference, value: Literal) =>
        val colName = getTargetColNameParts(attribute)
        rewriteCondition(colName, LessThanOrEqual(minValue(colName), value))
      // Filter "b <= colA"
      // Translates to "b <= colA_maxValue" for index lookup
      case LessThanOrEqual(value: Literal, attribute: AttributeReference) =>
        val colName = getTargetColNameParts(attribute)
        rewriteCondition(colName, GreaterThanOrEqual(maxValue(colName), value))
      // Filter "colA >= b"
      // Translates to "colA_maxValue >= b" for index lookup
      case GreaterThanOrEqual(attribute: AttributeReference, right: Literal) =>
        val colName = getTargetColNameParts(attribute)
        rewriteCondition(colName, GreaterThanOrEqual(maxValue(colName), right))
      // Filter "b >= colA"
      // Translates to "b >= colA_minValue" for index lookup
      case GreaterThanOrEqual(value: Literal, attribute: AttributeReference) =>
        val colName = getTargetColNameParts(attribute)
        rewriteCondition(colName, LessThanOrEqual(minValue(colName), value))
      // Filter "colA is null"
      // Translates to "colA_num_nulls > 0" for index lookup
      case IsNull(attribute: AttributeReference) =>
        val colName = getTargetColNameParts(attribute)
        rewriteCondition(colName, GreaterThan(numNulls(colName), Literal(0)))
      // Filter "colA is not null"
      // Translates to "colA_num_nulls = 0" for index lookup
      case IsNotNull(attribute: AttributeReference) =>
        val colName = getTargetColNameParts(attribute)
        rewriteCondition(colName, EqualTo(numNulls(colName), Literal(0)))
      // Filter "colA in (a, b, ...)"
      // Translates to "(colA_minValue <= a AND colA_maxValue >= a) OR (colA_minValue <= b AND colA_maxValue >= b)" for index lookup
      case In(attribute: AttributeReference, list: Seq[Literal]) =>
        val colName = getTargetColNameParts(attribute)
        rewriteCondition(colName, colContainsValuesEqualToLiterals(colName, list))
      // Filter "colA like xxx"
      // Translates to "colA_minValue <= xxx AND colA_maxValue >= xxx" for index lookup
      // NOTE: That this operator only matches string prefixes, and this is
      //       essentially equivalent to "colA = b" expression
      case StartsWith(attribute, v @ Literal(_: UTF8String, _)) =>
        val colName = getTargetColNameParts(attribute)
        rewriteCondition(colName, colContainsValuesEqualToLiteral(colName, v))
      // Filter "colA not in (a, b, ...)"
      // Translates to "(colA_minValue > a OR colA_maxValue < a) AND (colA_minValue > b OR colA_maxValue < b)" for index lookup
      // NOTE: This is an inversion of `in (a, b, ...)` expr
      case Not(In(attribute: AttributeReference, list: Seq[Literal])) =>
        val colName = getTargetColNameParts(attribute)
        rewriteCondition(colName, Not(colContainsValuesEqualToLiterals(colName, list)))
      // Filter "colA != b"
      // Translates to "colA_minValue > b OR colA_maxValue < b" (which is an inversion of expr for "colA = b") for index lookup
      // NOTE: This is an inversion of `colA = b` expr
      case Not(EqualTo(attribute: AttributeReference, value: Literal)) =>
        val colName = getTargetColNameParts(attribute)
        rewriteCondition(colName, Not(colContainsValuesEqualToLiteral(colName, value)))
      // Filter "b != colA"
      // Translates to "colA_minValue > b OR colA_maxValue < b" (which is an inversion of expr for "colA = b") for index lookup
      // NOTE: This is an inversion of `colA != b` expr
      case Not(EqualTo(value: Literal, attribute: AttributeReference)) =>
        val colName = getTargetColNameParts(attribute)
        rewriteCondition(colName, Not(colContainsValuesEqualToLiteral(colName, value)))
      // Filter "colA not like xxx"
      // Translates to "!(colA_minValue <= xxx AND colA_maxValue >= xxx)" for index lookup
      // NOTE: This is a inversion of "colA like xxx" assuming that colA is a string-based type
      case Not(StartsWith(attribute, value @ Literal(_: UTF8String, _))) =>
        val colName = getTargetColNameParts(attribute)
        rewriteCondition(colName, Not(colContainsValuesEqualToLiteral(colName, value)))

      case or: Or =>
        val resLeft = createZIndexLookupFilter(or.left, indexSchema)
        val resRight = createZIndexLookupFilter(or.right, indexSchema)
        Or(resLeft, resRight)

      case and: And =>
        val resLeft = createZIndexLookupFilter(and.left, indexSchema)
        val resRight = createZIndexLookupFilter(and.right, indexSchema)
        And(resLeft, resRight)

      case expr: Expression =>
        Literal.TrueLiteral
    }
  }

  /**
    * Extracts name from a resolved expression referring to a nested or non-nested column.
    */
  def getTargetColNameParts(resolvedTargetCol: Expression): Seq[String] = {
    resolvedTargetCol match {
      case attr: Attribute => Seq(attr.name)

      case Alias(c, _) => getTargetColNameParts(c)

      case GetStructField(c, _, Some(name)) => getTargetColNameParts(c) :+ name

      case ex: ExtractValue =>
        throw new AnalysisException(s"convert reference to name failed, Updating nested fields is only supported for StructType: ${ex}.")

      case other =>
        throw new AnalysisException(s"convert reference to name failed,  Found unsupported expression ${other}")
    }
  }

  def getIndexFiles(conf: Configuration, indexPath: String): Seq[FileStatus] = {
    val basePath = new Path(indexPath)
    basePath.getFileSystem(conf)
      .listStatus(basePath).filter(f => f.getPath.getName.endsWith(".parquet"))
  }

  /**
    * read parquet files concurrently by local.
    * this method is mush faster than spark
    */
  def readParquetFile(spark: SparkSession, indexFiles: Seq[FileStatus], filters: Seq[Filter] = Nil, schemaOpts: Option[StructType] = None): Set[String] = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val partitionedFiles = indexFiles.map(f => PartitionedFile(InternalRow.empty, f.getPath.toString, 0, f.getLen))

    val requiredSchema = new StructType().add("file", StringType, true)
    val schema = schemaOpts.getOrElse(requiredSchema)
    val parquetReader = new ParquetFileFormat().buildReaderWithPartitionValues(spark
      , schema , StructType(Nil), requiredSchema, filters, Map.empty, hadoopConf)
    val results = new Array[Iterator[String]](partitionedFiles.size)
    partitionedFiles.zipWithIndex.par.foreach { case (pf, index) =>
      val fileIterator = parquetReader(pf).asInstanceOf[Iterator[Any]]
      val rows = fileIterator.flatMap(_ match {
        case r: InternalRow => Seq(r)
        case b: ColumnarBatch => b.rowIterator().asScala
      }).map(r => r.getString(0))
      results(index) = rows
    }
    results.flatMap(f => f).toSet
  }
}
