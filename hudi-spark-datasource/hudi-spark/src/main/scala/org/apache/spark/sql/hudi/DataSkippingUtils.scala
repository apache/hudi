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
    * create z_index filter and push those filters to index table to filter all candidate scan files.
    * @param condition  origin filter from query.
    * @param indexSchema schema from index table.
    * @return filters for index table.
    */
  def createZindexFilter(condition: Expression, indexSchema: StructType): Expression = {
    def buildExpressionInternal(colName: Seq[String], statisticValue: String): Expression = {
      val appendColName = UnresolvedAttribute(colName).name + statisticValue
      col(appendColName).expr
    }

    def reWriteCondition(colName: Seq[String], conditionExpress: Expression): Expression = {
      val appendColName = UnresolvedAttribute(colName).name + "_minValue"
      if (indexSchema.exists(p => p.name == appendColName)) {
        conditionExpress
      } else {
        Literal.TrueLiteral
      }
    }

    val minValue = (colName: Seq[String]) => buildExpressionInternal(colName, "_minValue")
    val maxValue = (colName: Seq[String]) => buildExpressionInternal(colName, "_maxValue")
    val num_nulls = (colName: Seq[String]) => buildExpressionInternal(colName, "_num_nulls")

    condition match {
      // query filter "colA = b"  convert it to "colA_minValue <= b and colA_maxValue >= b" for index table
      case EqualTo(attribute: AttributeReference, value: Literal) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, And(LessThanOrEqual(minValue(colName), value), GreaterThanOrEqual(maxValue(colName), value)))
      // query filter "b = colA"  convert it to "colA_minValue <= b and colA_maxValue >= b" for index table
      case EqualTo(value: Literal, attribute: AttributeReference) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, And(LessThanOrEqual(minValue(colName), value), GreaterThanOrEqual(maxValue(colName), value)))
      // query filter "colA = null"  convert it to "colA_num_nulls = null" for index table
      case equalNullSafe @ EqualNullSafe(_: AttributeReference, _ @ Literal(null, _)) =>
        val colName = getTargetColNameParts(equalNullSafe.left)
        reWriteCondition(colName, EqualTo(num_nulls(colName), equalNullSafe.right))
      // query filter "colA < b"  convert it to "colA_minValue < b" for index table
      case LessThan(attribute: AttributeReference, value: Literal) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName,LessThan(minValue(colName), value))
      // query filter "b < colA"  convert it to "colA_maxValue > b" for index table
      case LessThan(value: Literal, attribute: AttributeReference) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, GreaterThan(maxValue(colName), value))
      // query filter "colA > b"  convert it to "colA_maxValue > b" for index table
      case GreaterThan(attribute: AttributeReference, value: Literal) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, GreaterThan(maxValue(colName), value))
      // query filter "b > colA"  convert it to "colA_minValue < b" for index table
      case GreaterThan(value: Literal, attribute: AttributeReference) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, LessThan(minValue(colName), value))
      // query filter "colA <= b"  convert it to "colA_minValue <= b" for index table
      case LessThanOrEqual(attribute: AttributeReference, value: Literal) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, LessThanOrEqual(minValue(colName), value))
      // query filter "b <= colA"  convert it to "colA_maxValue >= b" for index table
      case LessThanOrEqual(value: Literal, attribute: AttributeReference) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, GreaterThanOrEqual(maxValue(colName), value))
      // query filter "colA >= b"   convert it to "colA_maxValue >= b" for index table
      case GreaterThanOrEqual(attribute: AttributeReference, right: Literal) =>
        val colName = getTargetColNameParts(attribute)
        GreaterThanOrEqual(maxValue(colName), right)
      // query filter "b >= colA"   convert it to "colA_minValue <= b" for index table
      case GreaterThanOrEqual(value: Literal, attribute: AttributeReference) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, LessThanOrEqual(minValue(colName), value))
      // query filter "colA is null"   convert it to "colA_num_nulls > 0" for index table
      case IsNull(attribute: AttributeReference) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, GreaterThan(num_nulls(colName), Literal(0)))
      // query filter "colA is not null"   convert it to "colA_num_nulls = 0" for index table
      case IsNotNull(attribute: AttributeReference) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, EqualTo(num_nulls(colName), Literal(0)))
      // query filter "colA in (a,b)"   convert it to " (colA_minValue <= a and colA_maxValue >= a) or (colA_minValue <= b and colA_maxValue >= b) " for index table
      case In(attribute: AttributeReference, list: Seq[Literal]) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, list.map { lit =>
          And(LessThanOrEqual(minValue(colName), lit), GreaterThanOrEqual(maxValue(colName), lit))
        }.reduce(Or))
      // query filter "colA like xxx"   convert it to "  (colA_minValue <= xxx and colA_maxValue >= xxx) or (colA_min start with xxx or colA_max start with xxx)  " for index table
      case StartsWith(attribute, v @ Literal(_: UTF8String, _)) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, Or(And(LessThanOrEqual(minValue(colName), v), GreaterThanOrEqual(maxValue(colName), v)) ,
          Or(StartsWith(minValue(colName), v), StartsWith(maxValue(colName), v))))
      // query filter "colA not in (a, b)"   convert it to " (not( colA_minValue = a and colA_maxValue = a)) and (not( colA_minValue = b and colA_maxValue = b)) " for index table
      case Not(In(attribute: AttributeReference, list: Seq[Literal])) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, list.map { lit =>
          Not(And(EqualTo(minValue(colName), lit), EqualTo(maxValue(colName), lit)))
        }.reduce(And))
      // query filter "colA != b"   convert it to "not ( colA_minValue = b and colA_maxValue = b )" for index table
      case Not(EqualTo(attribute: AttributeReference, value: Literal)) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, Not(And(EqualTo(minValue(colName), value), EqualTo(maxValue(colName), value))))
      // query filter "b != colA"   convert it to "not ( colA_minValue = b and colA_maxValue = b )" for index table
      case Not(EqualTo(value: Literal, attribute: AttributeReference)) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, Not(And(EqualTo(minValue(colName), value), EqualTo(maxValue(colName), value))))
      // query filter "colA not like xxxx"   convert it to "not ( colA_minValue startWith xxx and colA_maxValue startWith xxx)" for index table
      case Not(StartsWith(attribute, value @ Literal(_: UTF8String, _))) =>
        val colName = getTargetColNameParts(attribute)
        reWriteCondition(colName, Not(And(StartsWith(minValue(colName), value), StartsWith(maxValue(colName), value))))
      case or: Or =>
        val resLeft = createZindexFilter(or.left, indexSchema)
        val resRight = createZindexFilter(or.right, indexSchema)
        Or(resLeft, resRight)

      case and: And =>
        val resLeft = createZindexFilter(and.left, indexSchema)
        val resRight = createZindexFilter(and.right, indexSchema)
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
      .listStatus(basePath).filterNot(f => f.getPath.getName.endsWith(".parquet"))
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
