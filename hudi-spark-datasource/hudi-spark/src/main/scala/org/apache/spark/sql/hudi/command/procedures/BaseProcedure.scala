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
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.model.HoodieRecordPayload
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.exception.HoodieClusteringException
import org.apache.hudi.index.HoodieIndex.IndexType
import org.apache.spark.SparkException
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.types._

import java.nio.charset.Charset
import java.sql.{Date, Timestamp}

abstract class BaseProcedure extends Procedure {
  val INVALID_ARG_INDEX: Int = -1

  val spark: SparkSession = SparkSession.active
  val jsc = new JavaSparkContext(spark.sparkContext)

  protected def sparkSession: SparkSession = spark

  protected def createHoodieClient(jsc: JavaSparkContext, basePath: String): SparkRDDWriteClient[_ <: HoodieRecordPayload[_ <: AnyRef]] = {
    val config = getWriteConfig(basePath)
    new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), config)
  }

  protected def getWriteConfig(basePath: String): HoodieWriteConfig = {
    HoodieWriteConfig.newBuilder
      .withPath(basePath)
      .withIndexConfig(HoodieIndexConfig.newBuilder.withIndexType(IndexType.BLOOM).build)
      .build
  }

  protected def checkArgs(target: Array[ProcedureParameter], args: ProcedureArgs): Unit = {
    val internalRow = args.internalRow
    for (i <- target.indices) {
      if (target(i).required) {
        var argsIndex: Integer = null
        if (args.isNamedArgs) {
          argsIndex = getArgsIndex(target(i).name, args)
        } else {
          argsIndex = getArgsIndex(i.toString, args)
        }
        assert(-1 != argsIndex && internalRow.get(argsIndex, target(i).dataType) != null,
          s"Argument: ${target(i).name} is required")
      }
    }
  }

  protected def getArgsIndex(key: String, args: ProcedureArgs): Integer = {
    args.map.getOrDefault(key, INVALID_ARG_INDEX)
  }

  protected def getArgValueOrDefault(args: ProcedureArgs, parameter: ProcedureParameter): Option[Any] = {
    var argsIndex: Int = INVALID_ARG_INDEX
    if (args.isNamedArgs) {
      argsIndex = getArgsIndex(parameter.name, args)
    } else {
      argsIndex = getArgsIndex(parameter.index.toString, args)
    }

    if (argsIndex.equals(INVALID_ARG_INDEX)) {
      parameter.default match {
        case option: Option[Any] => option
        case _ => Option.apply(parameter.default)
      }
    } else {
      Option.apply(getInternalRowValue(args.internalRow, argsIndex, parameter.dataType))
    }
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
          throw new HoodieClusteringException("Table name or table path must be given one"))
      )
  }

  protected def convertCatalystType(value: String, dataType: DataType): Any = {
    try {
      val valueWithType = dataType match {
        case StringType => value
        case BinaryType => value.getBytes(Charset.forName("utf-8"))
        case BooleanType => value.toBoolean
        case DoubleType => value.toDouble
        case d: DecimalType => Decimal.apply(BigDecimal(value), d.precision, d.scale)
        case FloatType => value.toFloat
        case ByteType => value.toByte
        case IntegerType => value.toInt
        case LongType => value.toLong
        case ShortType => value.toShort
        case DateType => DateTimeUtils.fromJavaDate(Date.valueOf(value))
        case TimestampType => DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(value))
        case _ => throw new HoodieClusteringException("Data type not support:" + dataType)
      }

      valueWithType
    } catch {
      case e: HoodieClusteringException =>
        throw e
      case _ =>
        throw new HoodieClusteringException("Data type not match, value:" + value + ", dataType:" + dataType)
    }
  }
}
