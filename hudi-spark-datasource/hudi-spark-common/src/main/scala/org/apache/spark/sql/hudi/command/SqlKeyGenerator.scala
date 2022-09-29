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

package org.apache.spark.sql.hudi.command

import org.apache.avro.generic.GenericRecord
import org.apache.hudi.DataSourceOptionsHelper
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.util.PartitionPathEncodeUtils
import org.apache.hudi.common.util.ValidationUtils.checkArgument
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen._
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String
import org.joda.time.format.DateTimeFormat

import java.sql.Timestamp
import java.util
import java.util.Collections
import java.util.concurrent.TimeUnit.{MICROSECONDS, MILLISECONDS}

/**
 * Custom Spark-specific [[KeyGenerator]] overriding behavior handling [[TimestampType]] partition values
 */
class SqlKeyGenerator(props: TypedProperties) extends BuiltinKeyGenerator(props) {

  private lazy val partitionSchema = {
    val partitionSchema = props.getString(SqlKeyGenerator.PARTITION_SCHEMA, "")
    if (partitionSchema != null && partitionSchema.nonEmpty) {
      Some(StructType.fromDDL(partitionSchema))
    } else {
      None
    }
  }

  private lazy val complexKeyGen = new ComplexKeyGenerator(props)
  private lazy val originalKeyGen =
    Option(props.getString(SqlKeyGenerator.ORIGINAL_KEYGEN_CLASS_NAME, null))
      .map { originalKeyGenClassName =>
        checkArgument(originalKeyGenClassName.nonEmpty)

        val convertedKeyGenClassName = HoodieSparkKeyGeneratorFactory.convertToSparkKeyGenerator(originalKeyGenClassName)

        val keyGenProps = new TypedProperties()
        keyGenProps.putAll(props)
        keyGenProps.remove(SqlKeyGenerator.ORIGINAL_KEYGEN_CLASS_NAME)
        keyGenProps.put(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key, convertedKeyGenClassName)

        KeyGenUtils.createKeyGeneratorByClassName(keyGenProps).asInstanceOf[SparkKeyGeneratorInterface]
      }

  override def getRecordKey(record: GenericRecord): String =
    originalKeyGen.map {
      _.getKey(record).getRecordKey
    } getOrElse {
      complexKeyGen.getRecordKey(record)
    }

  override def getRecordKey(row: Row): String =
    originalKeyGen.map {
      _.getRecordKey(row)
    } getOrElse {
      complexKeyGen.getRecordKey(row)
    }


  override def getRecordKey(internalRow: InternalRow, schema: StructType): UTF8String =
    originalKeyGen.map {
      _.getRecordKey(internalRow, schema)
    } getOrElse {
      complexKeyGen.getRecordKey(internalRow, schema)
    }

  override def getPartitionPath(record: GenericRecord): String = {
    val partitionPath = originalKeyGen.map {
      _.getKey(record).getPartitionPath
    } getOrElse {
      complexKeyGen.getPartitionPath(record)
    }

    convertPartitionPathToSqlType(partitionPath, rowType = false)
  }

  override def getPartitionPath(row: Row): String = {
    val partitionPath = originalKeyGen.map {
      _.getPartitionPath(row)
    } getOrElse {
      complexKeyGen.getPartitionPath(row)
    }

    convertPartitionPathToSqlType(partitionPath, rowType = true)
  }

  override def getPartitionPath(internalRow: InternalRow, schema: StructType): UTF8String = {
    val partitionPath = originalKeyGen.map {
      _.getPartitionPath(internalRow, schema)
    } getOrElse {
      complexKeyGen.getPartitionPath(internalRow, schema)
    }

    UTF8String.fromString(convertPartitionPathToSqlType(partitionPath.toString, rowType = true))
  }

  override def getPartitionPathFields: util.List[String] = {
    originalKeyGen.map {
      case bkg: BaseKeyGenerator => bkg.getPartitionPathFields
      case _ =>
        Option(super.partitionPathFields).getOrElse(Collections.emptyList)
    } getOrElse {
      complexKeyGen.getPartitionPathFields
    }
  }

  // TODO clean up
  private def convertPartitionPathToSqlType(partitionPath: String, rowType: Boolean): String = {
    if (partitionSchema.isDefined) {
      // we can split the partitionPath here because we enable the URL_ENCODE_PARTITIONING_OPT
      // by default for sql.
      val partitionFragments = partitionPath.split(KeyGenUtils.DEFAULT_PARTITION_PATH_SEPARATOR)
      // If it is a table which is not write by spark sql before and the url encode has disabled,
      // the partition path level may not equal to the partition schema size. Just return the partitionPath
      // in this case.
      if (partitionFragments.size != partitionSchema.get.size) {
        partitionPath
      } else {
        partitionFragments.zip(partitionSchema.get.fields).map {
          case (partitionValue, partitionField) =>
            val hiveStylePrefix = s"${partitionField.name}="
            val isHiveStyle = partitionValue.startsWith(hiveStylePrefix)
            val _partitionValue = if (isHiveStyle) partitionValue.substring(hiveStylePrefix.length) else partitionValue

            partitionField.dataType match {
              case TimestampType =>
                val timeMs = if (rowType) { // In RowType, the partitionPathValue is the time format string, convert to millis
                  SqlKeyGenerator.sqlTimestampFormat.parseMillis(_partitionValue)
                } else {
                  if (isConsistentLogicalTimestampEnabled) {
                    Timestamp.valueOf(_partitionValue).getTime
                  } else {
                    MILLISECONDS.convert(_partitionValue.toLong, MICROSECONDS)
                  }
                }
                val timestampFormat = PartitionPathEncodeUtils.escapePathName(
                  SqlKeyGenerator.timestampTimeFormat.print(timeMs))
                if (isHiveStyle) s"$hiveStylePrefix$timestampFormat" else timestampFormat
              case _ => partitionValue
            }
        }.mkString(KeyGenUtils.DEFAULT_PARTITION_PATH_SEPARATOR)
      }
    } else partitionPath
  }
}

object SqlKeyGenerator {
  val PARTITION_SCHEMA = "hoodie.sql.partition.schema"
  val ORIGINAL_KEYGEN_CLASS_NAME = "hoodie.sql.origin.keygen.class"
  private val timestampTimeFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
  private val sqlTimestampFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.S")
}
