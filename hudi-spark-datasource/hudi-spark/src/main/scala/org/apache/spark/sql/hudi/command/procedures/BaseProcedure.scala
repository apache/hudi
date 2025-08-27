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

package org.apache.spark.sql.hudi.command.procedures

import org.apache.hudi.HoodieCLIUtils
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.index.HoodieIndex.IndexType

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

abstract class BaseProcedure extends Procedure {
  val spark: SparkSession = SparkSession.active
  val jsc = new JavaSparkContext(spark.sparkContext)

  protected def sparkSession: SparkSession = spark

  protected def getWriteConfig(basePath: String): HoodieWriteConfig = {
    HoodieWriteConfig.newBuilder
      .withPath(basePath)
      .withIndexConfig(HoodieIndexConfig.newBuilder.withIndexType(IndexType.BLOOM).build)
      .build
  }

  protected def createMetaClient(jsc: JavaSparkContext, basePath: String): HoodieTableMetaClient = {
    HoodieTableMetaClient.builder
      .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()))
      .setBasePath(basePath).build
  }

  protected def getParamKey(parameter: ProcedureParameter, isNamedArgs: Boolean): String = {
    if (isNamedArgs) {
      parameter.name
    } else {
      parameter.index.toString
    }
  }

  protected def checkArgs(parameters: Array[ProcedureParameter], args: ProcedureArgs): Unit = {
    for (parameter <- parameters) {
      if (parameter.required) {
        val paramKey = getParamKey(parameter, args.isNamedArgs)
        assert(args.map.containsKey(paramKey) &&
          args.internalRow.get(args.map.get(paramKey), parameter.dataType) != null,
          s"Argument: ${parameter.name} is required")
      }
    }
  }

  protected def getArgValueOrDefault(args: ProcedureArgs, parameter: ProcedureParameter): Option[Any] = {
    val paramKey = getParamKey(parameter, args.isNamedArgs)
    if (args.map.containsKey(paramKey)) {
      Option.apply(getInternalRowValue(args.internalRow, args.map.get(paramKey), parameter.dataType))
    } else {
      Option.apply(parameter.default)
    }
  }

  protected def isArgDefined(args: ProcedureArgs, parameter: ProcedureParameter): Boolean = {
    val paramKey = getParamKey(parameter, args.isNamedArgs)
    args.map.containsKey(paramKey)
  }

  protected def getInternalRowValue(row: InternalRow, index: Int, dataType: DataType): Any = {
    dataType match {
      case StringType => row.getString(index)
      case BinaryType => row.getBinary(index)
      case BooleanType => row.getBoolean(index)
      case CalendarIntervalType => row.getInterval(index)
      case DoubleType => row.getDouble(index)
      case d: DecimalType => row.getDecimal(index, d.precision, d.scale)
      case FloatType => row.getFloat(index)
      case ByteType => row.getByte(index)
      case IntegerType => row.getInt(index)
      case LongType => row.getLong(index)
      case ShortType => row.getShort(index)
      case NullType => null
      case _ =>
        throw new UnsupportedOperationException(s"type: ${dataType.typeName} not supported")
    }
  }

  protected def getBasePath(tableName: Option[Any], tablePath: Option[Any] = Option.empty): String = {
    tableName.map(
      t => HoodieCLIUtils.getHoodieCatalogTable(sparkSession, t.asInstanceOf[String]).tableLocation)
      .getOrElse(
        tablePath.map(p => p.asInstanceOf[String]).getOrElse(
          throw new HoodieException("Table name or table path must be given one"))
      )
  }

  protected def getDbAndTableName(tableName: String): (String, String) = {
    val names = tableName.split("\\.")
    if (names.length == 1) {
      ("default", names(0))
    } else if (names.length == 2) {
      (names(0), names(1))
    } else {
      throw new HoodieException(s"Table name: $tableName is not valid")
    }
  }

  protected def validateFilter(filter: String, schema: StructType): Unit = {
    if (filter != null && filter.trim.nonEmpty) {
      HoodieProcedureFilterUtils.validateFilterExpression(filter, schema, sparkSession) match {
        case Left(errorMessage) =>
          throw new IllegalArgumentException(s"Invalid filter expression: $errorMessage")
        case Right(_) => // Validation passed, continue
      }
    }
  }

  protected def applyFilter(results: Seq[Row], filter: String, schema: StructType): Seq[Row] = {
    if (filter != null && filter.trim.nonEmpty) {
      HoodieProcedureFilterUtils.evaluateFilter(results, filter, schema, sparkSession)
    } else {
      results
    }
  }
}
