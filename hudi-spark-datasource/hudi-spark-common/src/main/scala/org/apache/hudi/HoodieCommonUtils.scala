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

package org.apache.hudi

import org.apache.hadoop.fs.Path
import org.apache.hudi.BaseHoodieTableFileIndex.PartitionPath
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.keygen.{TimestampBasedAvroKeyGenerator, TimestampBasedKeyGenerator}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BoundReference, Expression, InterpretedPredicate}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LocalRelation}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.catalyst.{InternalRow, expressions}
import org.apache.spark.sql.execution.datasources.SparkParsePartitionUtil
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.withSparkConf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.JavaConverters.mapAsJavaMapConverter
import scala.collection.immutable.Map

object HoodieCommonUtils extends Logging {

  def createHoodieClientFromPath(sparkSession: SparkSession, basePath: String,
                                 conf: Map[String, String]): SparkRDDWriteClient[_] = {
    val metaClient = HoodieTableMetaClient.builder().setBasePath(basePath)
      .setConf(sparkSession.sessionState.newHadoopConf()).build()
    val schemaUtil = new TableSchemaResolver(metaClient)
    val schemaStr = schemaUtil.getTableAvroSchemaWithoutMetadataFields.toString
    val finalParameters = HoodieWriterUtils.parametersWithWriteDefaults(
      withSparkConf(sparkSession, Map.empty)(
        conf + (DataSourceWriteOptions.TABLE_TYPE.key() -> metaClient.getTableType.name()))
    )

    val jsc = new JavaSparkContext(sparkSession.sparkContext)
    DataSourceUtils.createHoodieClient(jsc, schemaStr, basePath,
      metaClient.getTableConfig.getTableName, finalParameters.asJava)
  }

  def getPartitionSchemaFromProperty(metaClient: HoodieTableMetaClient,
                                     schemaSpec: Option[StructType]): StructType = {
    val schema = schemaSpec.getOrElse({
      val schemaUtil = new TableSchemaResolver(metaClient)
      AvroConversionUtils.convertAvroSchemaToStructType(schemaUtil.getTableAvroSchema)
    })

    val tableConfig = metaClient.getTableConfig
    val partitionColumns = tableConfig.getPartitionFields
    val nameFieldMap = generateFieldMap(schema)

    if (partitionColumns.isPresent) {
      // Note that key generator class name could be null
      val keyGeneratorClassName = tableConfig.getKeyGeneratorClassName
      if (classOf[TimestampBasedKeyGenerator].getName.equalsIgnoreCase(keyGeneratorClassName)
        || classOf[TimestampBasedAvroKeyGenerator].getName.equalsIgnoreCase(keyGeneratorClassName)) {
        val partitionFields = partitionColumns.get().map(column => StructField(column, StringType))
        StructType(partitionFields)
      } else {
        val partitionFields = partitionColumns.get().map(column =>
          nameFieldMap.getOrElse(column, throw new IllegalArgumentException(s"Cannot find column: '" +
            s"$column' in the schema[${schema.fields.mkString(",")}]")))
        StructType(partitionFields)
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
  def generateFieldMap(structType: StructType): Map[String, StructField] = {
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

  /**
   * Prune the partition by the filter.This implementation is fork from
   * org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex#prunePartitions.
   *
   * @param partitionPaths All the partition paths.
   * @param predicates     The filter condition.
   * @return The Pruned partition paths.
   */
  def prunePartition(partitionSchema: StructType,
                     partitionPaths: Seq[PartitionPath],
                     predicates: Seq[Expression]): Seq[PartitionPath] = {
    val partitionColumnNames = partitionSchema.fields.map(_.name).toSet
    val partitionPruningPredicates = predicates.filter {
      _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
    }
    if (partitionPruningPredicates.nonEmpty) {
      val predicate = partitionPruningPredicates.reduce(expressions.And)
      prunePartition(partitionSchema, partitionPaths, predicate)
    } else {
      partitionPaths
    }
  }

  def prunePartition(partitionSchema: StructType,
                     partitionPaths: Seq[PartitionPath],
                     predicate: Expression): Seq[PartitionPath] = {
    val boundPredicate = InterpretedPredicate(predicate.transform {
      case a: AttributeReference =>
        val index = partitionSchema.indexWhere(a.name == _.name)
        BoundReference(index, partitionSchema(index).dataType, nullable = true)
    })

    val prunedPartitionPaths = partitionPaths.filter {
      partitionPath => boundPredicate.eval(InternalRow.fromSeq(partitionPath.values))
    }

    logInfo(s"Total partition size is: ${partitionPaths.size}," +
      s" after partition prune size is: ${prunedPartitionPaths.size}")
    prunedPartitionPaths
  }

  def parsePartitionColumnValues(sparkParsePartitionUtil: SparkParsePartitionUtil,
                                 configProperties: TypedProperties,
                                 basePath: String,
                                 partitionSchema: StructType,
                                 partitionColumns: Array[String],
                                 partitionPath: String): Array[Object] = {
    if (partitionColumns.length == 0) {
      // This is a non-partitioned table
      Array.empty
    } else {
      val partitionFragments = partitionPath.split("/")

      if (partitionFragments.length != partitionColumns.length &&
        partitionColumns.length == 1) {
        // If the partition column size is not equal to the partition fragment size
        // and the partition column size is 1, we map the whole partition path
        // to the partition column which can benefit from the partition prune.
        val prefix = s"${partitionColumns.head}="
        val partitionValue = if (partitionPath.startsWith(prefix)) {
          // support hive style partition path
          partitionPath.substring(prefix.length)
        } else {
          partitionPath
        }
        Array(UTF8String.fromString(partitionValue))
      } else if (partitionFragments.length != partitionColumns.length &&
        partitionColumns.length > 1) {
        // If the partition column size is not equal to the partition fragments size
        // and the partition column size > 1, we do not know how to map the partition
        // fragments to the partition columns. So we trait it as a Non-Partitioned Table
        // for the query which do not benefit from the partition prune.
        logWarning(s"Cannot do the partition prune for table $basePath." +
          s"The partitionFragments size (${partitionFragments.mkString(",")})" +
          s" is not equal to the partition columns size(${partitionColumns.mkString(",")})")
        Array.empty
      } else {
        // If partitionSeqs.length == partitionSchema.fields.length
        // Append partition name to the partition value if the
        // HIVE_STYLE_PARTITIONING is disable.
        // e.g. convert "/xx/xx/2021/02" to "/xx/xx/year=2021/month=02"
        val partitionWithName =
        partitionFragments.zip(partitionColumns).map {
          case (partition, columnName) =>
            if (partition.indexOf("=") == -1) {
              s"${columnName}=$partition"
            } else {
              partition
            }
        }.mkString("/")

        val pathWithPartitionName = new Path(basePath, partitionWithName)
        val partitionValues = parsePartitionPath(sparkParsePartitionUtil, configProperties, basePath,
          pathWithPartitionName, partitionSchema)

        partitionValues.map(_.asInstanceOf[Object]).toArray
      }
    }
  }

  private def parsePartitionPath(sparkParsePartitionUtil: SparkParsePartitionUtil,
                                 configProperties: TypedProperties,
                                 basePath: String,
                                 partitionPath: Path,
                                 partitionSchema: StructType): Seq[Any] = {
    val timeZoneId = configProperties.getString(DateTimeUtils.TIMEZONE_OPTION, SQLConf.get.sessionLocalTimeZone)
    val partitionDataTypes = partitionSchema.map(f => f.name -> f.dataType).toMap

    sparkParsePartitionUtil.parsePartition(
      partitionPath,
      typeInference = false,
      Set(new Path(basePath)),
      partitionDataTypes,
      DateTimeUtils.getTimeZone(timeZoneId)
    )
      .toSeq(partitionSchema)
  }

  def getConfigProperties(spark: SparkSession, options: Map[String, String]): TypedProperties = {
    val sqlConf: SQLConf = spark.sessionState.conf
    val properties = new TypedProperties()

    // To support metadata listing via Spark SQL we allow users to pass the config via SQL Conf in spark session. Users
    // would be able to run SET hoodie.metadata.enable=true in the spark sql session to enable metadata listing.
    properties.setProperty(HoodieMetadataConfig.ENABLE.key(),
      sqlConf.getConfString(HoodieMetadataConfig.ENABLE.key(),
        HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS.toString))
    properties.putAll(options.asJava)
    properties
  }

  def resolveFilterExpr(spark: SparkSession, exprString: String, tableSchema: StructType): Expression = {
    val expr = spark.sessionState.sqlParser.parseExpression(exprString)
    resolveFilterExpr(spark, expr, tableSchema)
  }

  def resolveFilterExpr(spark: SparkSession, expr: Expression, tableSchema: StructType): Expression = {
    val schemaFields = tableSchema.fields
    val resolvedExpr = spark.sessionState.analyzer.ResolveReferences(
      Filter(expr, LocalRelation(schemaFields.head, schemaFields.drop(1): _*))
    )
      .asInstanceOf[Filter].condition

    checkForUnresolvedRefs(resolvedExpr)
  }

  private def checkForUnresolvedRefs(resolvedExpr: Expression): Expression =
    resolvedExpr match {
      case UnresolvedAttribute(_) => throw new IllegalStateException("unresolved attribute")
      case _ => resolvedExpr.mapChildren(e => checkForUnresolvedRefs(e))
    }
}
