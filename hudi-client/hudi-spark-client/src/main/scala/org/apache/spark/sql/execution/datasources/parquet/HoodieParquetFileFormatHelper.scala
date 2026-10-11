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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.FileMetaData
import org.apache.spark.sql.types._

object HoodieParquetFileFormatHelper {
  def buildImplicitSchemaChangeInfo(hadoopConf: Configuration,
                                    parquetFileMetaData: FileMetaData,
                                    requiredSchema: StructType): (java.util.Map[Integer, org.apache.hudi.common.util.collection.Pair[DataType, DataType]], StructType) = {
    val convert = new ParquetToSparkSchemaConverter(hadoopConf)
    val fileStruct = convert.convert(parquetFileMetaData.getSchema)
    buildImplicitSchemaChangeInfo(fileStruct, requiredSchema)
  }

  def buildImplicitSchemaChangeInfo(fileStruct: StructType,
                                    requiredSchema: StructType): (java.util.Map[Integer, org.apache.hudi.common.util.collection.Pair[DataType, DataType]], StructType) = {
    val implicitTypeChangeInfo: java.util.Map[Integer, org.apache.hudi.common.util.collection.Pair[DataType, DataType]] = new java.util.HashMap()

    val fileStructMap = fileStruct.fields.map(f => (f.name, f.dataType)).toMap
    // if there are missing fields or if field's data type needs to be changed while reading, we handle it here.
    val sparkRequestStructFields = requiredSchema.map(f => {
      val requiredType = f.dataType
      if (fileStructMap.contains(f.name) && !isDataTypeEqual(requiredType, fileStructMap(f.name))) {
        val readerType = addMissingFields(requiredType, fileStructMap(f.name))
        implicitTypeChangeInfo.put(new Integer(requiredSchema.fieldIndex(f.name)), org.apache.hudi.common.util.collection.Pair.of(requiredType, readerType))
        StructField(f.name, readerType, f.nullable)
      } else {
        f
      }
    })
    (implicitTypeChangeInfo, StructType(sparkRequestStructFields))
  }

  def isDataTypeEqual(requiredType: DataType, fileType: DataType): Boolean = (requiredType, fileType) match {

    case (requiredType, fileType) if requiredType == fileType => true

    case (ArrayType(rt, _), ArrayType(ft, _)) =>
      // Do not care about nullability as schema evolution require fields to be nullable
      isDataTypeEqual(rt, ft)

    case (MapType(requiredKey, requiredValue, _), MapType(fileKey, fileValue, _)) =>
      // Likewise, do not care about nullability as schema evolution require fields to be nullable
      isDataTypeEqual(requiredKey, fileKey) && isDataTypeEqual(requiredValue, fileValue)

    case (StructType(requiredFields), StructType(fileFields)) =>
      // Find fields that are in requiredFields and fileFields as they might not be the same during add column + change column operations
      val commonFieldNames = requiredFields.map(_.name) intersect fileFields.map(_.name)

      // Need to match by name instead of StructField as name will stay the same whilst type may change
      val fileFilteredFields = fileFields.filter(f => commonFieldNames.contains(f.name)).sortWith(_.name < _.name)
      val requiredFilteredFields = requiredFields.filter(f => commonFieldNames.contains(f.name)).sortWith(_.name < _.name)

      // Sorting ensures that the same field names are being compared for type differences
      requiredFilteredFields.zip(fileFilteredFields).forall {
        case (requiredField, fileFilteredField) =>
          isDataTypeEqual(requiredField.dataType, fileFilteredField.dataType)
      }

    case _ => false
  }

  def addMissingFields(requiredType: DataType, fileType: DataType): DataType = (requiredType, fileType) match {
    case (requiredType, fileType) if requiredType == fileType => fileType
    case (ArrayType(rt, _), ArrayType(ft, _)) => ArrayType(addMissingFields(rt, ft))
    case (MapType(requiredKey, requiredValue, _), MapType(fileKey, fileValue, _)) => MapType(addMissingFields(requiredKey, fileKey), addMissingFields(requiredValue, fileValue))
    case (StructType(requiredFields), StructType(fileFields)) =>
      val fileFieldMap = fileFields.map(f => f.name -> f).toMap
      StructType(requiredFields.map(f => {
        fileFieldMap.get(f.name) match {
          case Some(ff) => StructField(ff.name, addMissingFields(f.dataType, ff.dataType), ff.nullable, ff.metadata)
          case None => f
        }
      }))
    case _ => fileType
  }
}
