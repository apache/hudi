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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.fs.Path
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.util
import org.apache.hudi.keygen.CustomAvroKeyGenerator.PartitionKeyType
import org.apache.hudi.keygen.{BaseKeyGenerator, CustomAvroKeyGenerator, CustomKeyGenerator, TimestampBasedAvroKeyGenerator, TimestampBasedKeyGenerator}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

import java.util.TimeZone

trait SparkParsePartitionUtil extends Serializable with Logging {

  def parsePartition(path: Path,
                     typeInference: Boolean,
                     basePaths: Set[Path],
                     userSpecifiedDataTypes: Map[String, DataType],
                     timeZone: TimeZone,
                     validatePartitionValues: Boolean = false): InternalRow

  def getPartitionSchema(tableConfig: HoodieTableConfig, schema: StructType, handleCustomKeyGenerator: Boolean): StructType = {
    val nameFieldMap: Map[String, StructField] = generateFieldMap(schema)
    val partitionColumns = tableConfig.getPartitionFields

    def validateAndGetPartitionFieldsStruct(partitionFields: Array[StructField]) = {
      if (partitionFields.length != partitionColumns.get().length) {
        val isBootstrapTable = tableConfig.getBootstrapBasePath.isPresent
        if (isBootstrapTable) {
          // For bootstrapped tables its possible the schema does not contain partition field when source table
          // is hive style partitioned. In this case we would like to treat the table as non-partitioned
          // as opposed to failing
          new StructType()
        } else {
          throw new IllegalArgumentException(s"Cannot find columns: " +
            s"'${partitionColumns.get().filter(col => !nameFieldMap.contains(col)).mkString(",")}' " +
            s"in the schema[${nameFieldMap.keys.mkString(",")}]")
        }
      } else {
        new StructType(partitionFields)
      }
    }

    def getPartitionStructFields(keyGeneratorPartitionFieldsOpt: util.Option[String], keyGeneratorClassName: String) = {
      val partitionFields: Array[StructField] = if (handleCustomKeyGenerator && keyGeneratorPartitionFieldsOpt.isPresent && (classOf[CustomKeyGenerator].getName.equalsIgnoreCase(keyGeneratorClassName)
        || classOf[CustomAvroKeyGenerator].getName.equalsIgnoreCase(keyGeneratorClassName))) {
        val keyGeneratorPartitionFields = keyGeneratorPartitionFieldsOpt.get().split(BaseKeyGenerator.FIELD_SEPARATOR)
        keyGeneratorPartitionFields.map(field => CustomAvroKeyGenerator.getPartitionFieldAndKeyType(field))
          .map(pair => {
            val partitionField = pair.getLeft
            val partitionKeyType = pair.getRight
            partitionKeyType match {
              case PartitionKeyType.SIMPLE => if (nameFieldMap.contains(partitionField)) {
                nameFieldMap.apply(partitionField)
              } else {
                null
              }
              case PartitionKeyType.TIMESTAMP => StructField(partitionField, StringType)
            }
          })
          .filter(structField => structField != null)
          .array
      } else {
        partitionColumns.get().filter(column => nameFieldMap.contains(column))
          .map(column => nameFieldMap.apply(column))
      }
      partitionFields
    }

    if (partitionColumns.isPresent) {
      // Note that key generator class name could be null
      val keyGeneratorPartitionFieldsOpt = HoodieTableConfig.getPartitionFieldPropForKeyGenerator(tableConfig)
      val keyGeneratorClassName = tableConfig.getKeyGeneratorClassName
      if (classOf[TimestampBasedKeyGenerator].getName.equalsIgnoreCase(keyGeneratorClassName)
        || classOf[TimestampBasedAvroKeyGenerator].getName.equalsIgnoreCase(keyGeneratorClassName)) {
        val partitionFields: Array[StructField] = partitionColumns.get().map(column => StructField(column, StringType))
        StructType(partitionFields)
      } else {
        val partitionFields: Array[StructField] = getPartitionStructFields(keyGeneratorPartitionFieldsOpt, keyGeneratorClassName)
        validateAndGetPartitionFieldsStruct(partitionFields)
      }
    } else {
      // If the partition columns have not stored in hoodie.properties(the table that was
      // created earlier), we trait it as a non-partitioned table.
      logWarning("No partition columns available from hoodie.properties." +
        " Partition pruning will not work")
      new StructType()
    }
  }

  /**
   * This method unravels [[StructType]] into a [[Map]] of pairs of dot-path notation with corresponding
   * [[StructField]] object for every field of the provided [[StructType]], recursively.
   *
   * For example, following struct
   * <pre>
   * StructType(
   * StructField("a",
   * StructType(
   * StructField("b", StringType),
   * StructField("c", IntType)
   * )
   * )
   * )
   * </pre>
   *
   * will be converted into following mapping:
   *
   * <pre>
   * "a.b" -> StructField("b", StringType),
   * "a.c" -> StructField("c", IntType),
   * </pre>
   */
  private def generateFieldMap(structType: StructType): Map[String, StructField] = {
    def traverse(structField: Either[StructField, StructType]): Map[String, StructField] = {
      structField match {
        case Right(struct) => struct.fields.flatMap(f => traverse(Left(f))).toMap
        case Left(field) => field.dataType match {
          case struct: StructType => traverse(Right(struct)).map {
            case (key, structField) => (s"${field.name}.$key", structField)
          }
          case _ => Map(field.name -> field)
        }
      }
    }

    traverse(Right(structType))
  }

}
