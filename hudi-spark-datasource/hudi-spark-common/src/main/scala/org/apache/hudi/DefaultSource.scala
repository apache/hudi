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

package org.apache.hudi

import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions.{BOOTSTRAP_OPERATION_OPT_VAL, OPERATION, STREAMING_CHECKPOINT_IDENTIFIER}
import org.apache.hudi.cdc.CDCRelation
import org.apache.hudi.common.HoodieSchemaNotFoundException
import org.apache.hudi.common.config.{HoodieReaderConfig, HoodieStorageConfig}
import org.apache.hudi.common.model.HoodieTableType.{COPY_ON_WRITE, MERGE_ON_READ}
import org.apache.hudi.common.model.WriteConcurrencyMode
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion, TableSchemaResolver}
import org.apache.hudi.common.table.log.InstantRange.RangeType
import org.apache.hudi.common.util.{ConfigUtils, TablePathUtils}
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.config.HoodieBootstrapConfig.DATA_QUERIES_ONLY
import org.apache.hudi.config.HoodieWriteConfig.{WRITE_CONCURRENCY_MODE, WRITE_TABLE_VERSION}
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.io.storage.HoodieSparkIOFactory
import org.apache.hudi.storage.{HoodieStorageUtils, StoragePath}
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage
import org.apache.hudi.util.{SparkConfigUtils}

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, SQLContext}
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.hudi.HoodieSqlCommonUtils.isUsingHiveCatalog
import org.apache.spark.sql.hudi.streaming.{HoodieEarliestOffsetRangeLimit, HoodieLatestOffsetRangeLimit, HoodieSpecifiedOffsetRangeLimit, HoodieStreamSourceV1, HoodieStreamSourceV2}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
  * Hoodie Spark Datasource, for reading and writing hoodie tables
  *
  */
class DefaultSource extends RelationProvider
  with SchemaRelationProvider
  with CreatableRelationProvider
  with DataSourceRegister
  with StreamSinkProvider
  with StreamSourceProvider
  with SparkAdapterSupport
  with Serializable {

  SparkSession.getActiveSession.foreach { spark =>
    val sparkVersion = spark.version
    if (sparkVersion.startsWith("0.") || sparkVersion.startsWith("1.") || sparkVersion.startsWith("2.")) {
      // Enable "passPartitionByAsOptions" to support "write.partitionBy(...)"
      spark.conf.set("spark.sql.legacy.sources.write.passPartitionByAsOptions", "true")
    }
    // Always use spark io factory
    spark.sparkContext.hadoopConfiguration.set(HoodieStorageConfig.HOODIE_IO_FACTORY_CLASS.key(),
      classOf[HoodieSparkIOFactory].getName)
    // Revisit EMRFS incompatibilities, for now disable
    spark.sparkContext.hadoopConfiguration.set("fs.s3.metadata.cache.expiration.seconds", "0")
  }

  private val log = LoggerFactory.getLogger(classOf[DefaultSource])

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    try {
      val relation = createRelation(sqlContext, parameters, null)
      if (relation.schema.isEmpty) {
        new EmptyRelation(sqlContext, new StructType())
      } else {
        relation
      }
    } catch {
      case _: HoodieSchemaNotFoundException => new EmptyRelation(sqlContext, new StructType())
      case e => throw e
    }
  }

  override def createRelation(sqlContext: SQLContext,
                              optParams: Map[String, String],
                              schema: StructType): BaseRelation = {
    val path = optParams.get("path")
    val readPathsStr = optParams.get(DataSourceReadOptions.READ_PATHS.key)

    if (path.isEmpty && readPathsStr.isEmpty) {
      throw new HoodieException(s"'path' or '${READ_PATHS.key()}' or both must be specified.")
    }

    val readPaths = readPathsStr.map(p => p.split(",").toSeq).getOrElse(Seq())
    val allPaths = path.map(p => Seq(p)).getOrElse(Seq()) ++ readPaths

    val storage = HoodieStorageUtils.getStorage(
      allPaths.head, HadoopFSUtils.getStorageConf(sqlContext.sparkContext.hadoopConfiguration))

    if (path.exists(_.contains("*")) || readPaths.nonEmpty) {
      throw new HoodieException("Glob paths are not supported for read paths as of Hudi 1.2.0")
    }

    // Add default options for unspecified read options keys.
    val parameters = DataSourceOptionsHelper.parametersWithReadDefaults(sqlContext.getAllConfs.filter(k => k._1.startsWith("hoodie.")) ++ optParams)

    // Get the table base path
    val tablePath = DataSourceUtils.getTablePath(storage, Seq(new StoragePath(path.get)).asJava)
    log.info("Obtained hudi table path: " + tablePath)

    val metaClient = HoodieTableMetaClient.builder().setMetaserverConfig(parameters.toMap.asJava)
      .setConf(storage.getConf.newInstance())
      .setBasePath(tablePath).build()

    // Add preCombineField to options for buildReaderWithPartitionValues properly
    val options = if (metaClient.getTableConfig.getOrderingFieldsStr.isPresent) {
      parameters ++ Map(HoodieTableConfig.ORDERING_FIELDS.key -> metaClient.getTableConfig.getOrderingFieldsStr.orElse(null))
    } else {
      parameters
    }

    DefaultSource.createRelation(sqlContext, metaClient, schema, options.toMap)
  }

  /**
   * This DataSource API is used for writing the DataFrame at the destination. For now, we are returning a dummy
   * relation here because Spark does not really make use of the relation returned, and just returns an empty
   * dataset at [[org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand.run()]]. This saves us the cost
   * of creating and returning a parquet relation here.
   *
   * TODO: Revisit to return a concrete relation here when we support CREATE TABLE AS for Hudi with DataSource API.
   * That is the only case where Spark seems to actually need a relation to be returned here
   * [[org.apache.spark.sql.execution.datasources.DataSource.writeAndRead()]]
   *
   * @param sqlContext Spark SQL Context
   * @param mode       Mode for saving the DataFrame at the destination
   * @param optParams  Parameters passed as part of the DataFrame write operation
   * @param df         Spark DataFrame to be written
   * @return Spark Relation
   */
  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              optParams: Map[String, String],
                              df: DataFrame): BaseRelation = {
    try {
      if (optParams.get(OPERATION.key).contains(BOOTSTRAP_OPERATION_OPT_VAL)) {
        HoodieSparkSqlWriter.bootstrap(sqlContext, mode, optParams, df)
      } else {
        val (success, _, _, _, _, _) = HoodieSparkSqlWriter.write(sqlContext, mode, optParams, df)
        if (!success) {
          throw new HoodieException("Failed to write to Hudi")
        }
      }
    }
    finally {
      HoodieSparkSqlWriter.cleanup()
    }

    new HoodieEmptyRelation(sqlContext, df.schema)
  }

  override def createSink(sqlContext: SQLContext,
                          optParams: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {
    validateMultiWriterConfigs(optParams)
    new HoodieStreamingSink(
      sqlContext,
      optParams,
      partitionColumns,
      outputMode)
  }

  def validateMultiWriterConfigs(options: Map[String, String]) : Unit = {
    if (ConfigUtils.resolveEnum(classOf[WriteConcurrencyMode], options.getOrElse(WRITE_CONCURRENCY_MODE.key(),
      WRITE_CONCURRENCY_MODE.defaultValue())).supportsMultiWriter()) {
      // ensure some valid value is set for identifier
      checkState(options.contains(STREAMING_CHECKPOINT_IDENTIFIER.key()), "For multi-writer scenarios, please set "
        + STREAMING_CHECKPOINT_IDENTIFIER.key() + ". Each writer should set different values for this identifier")
    }
  }

  override def shortName(): String = "hudi_v1"

  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {
    val path = parameters.get("path")
    if (path.isEmpty || path.get == null) {
      throw new HoodieException(s"'path'  must be specified.")
    }
    val metaClient = HoodieTableMetaClient.builder().setConf(
      HadoopFSUtils.getStorageConf(sqlContext.sparkSession.sessionState.newHadoopConf()))
      .setBasePath(path.get).build()

    val sqlSchema = DefaultSource.resolveSchema(metaClient, parameters, schema)
    (shortName(), sqlSchema)
  }

  override def createSource(sqlContext: SQLContext,
                            metadataPath: String,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): Source = {
    val offsetRangeLimit = parameters.getOrElse(START_OFFSET.key(), START_OFFSET.defaultValue()) match {
      case offset if offset.equalsIgnoreCase("earliest") =>
        HoodieEarliestOffsetRangeLimit
      case offset if offset.equalsIgnoreCase("latest") =>
        HoodieLatestOffsetRangeLimit
      case instantTime =>
        HoodieSpecifiedOffsetRangeLimit(instantTime)
    }

    // NOTE: In cases when Hive Metastore is used as catalog and the table is partitioned, schema in the HMS might contain
    //       Hive-specific partitioning columns created specifically for HMS to handle partitioning appropriately. In that
    //       case  we opt in to not be providing catalog's schema, and instead force Hudi relations to fetch the schema
    //       from the table itself
    val userSchema = if (isUsingHiveCatalog(sqlContext.sparkSession)) {
      None
    } else {
      schema
    }

    val storageConf = HadoopFSUtils.getStorageConf(sqlContext.sparkSession.sessionState.newHadoopConf())
    val tablePath: StoragePath = {
      val path = new StoragePath(parameters.getOrElse("path", "Missing 'path' option"))
      val fs = new HoodieHadoopStorage(path, storageConf)
      TablePathUtils.getTablePath(fs, path).get()
    }
    val metaClient = HoodieTableMetaClient.builder()
      .setConf(storageConf.newInstance()).setBasePath(tablePath.toString).build()
    // Check if the streaming table read version is set. If set, use the corresponding source
    // which uses the version corresponding to IncrementalRelation to read the data. And, also
    // does the checkpoint management based on the version.
    val targetTableVersion: Integer = Integer.parseInt(
      parameters.getOrElse(WRITE_TABLE_VERSION.key, WRITE_TABLE_VERSION.defaultValue.toString))
    val readTableVersion = if (SparkConfigUtils.containsConfigProperty(parameters, STREAMING_READ_TABLE_VERSION)) {
      Integer.parseInt(parameters(STREAMING_READ_TABLE_VERSION.key))
    } else {
      metaClient.getTableConfig.getTableVersion.versionCode()
    }
    if (readTableVersion >= HoodieTableVersion.EIGHT.versionCode()) {
      new HoodieStreamSourceV2(
        sqlContext, metaClient, metadataPath, userSchema, parameters, offsetRangeLimit, HoodieTableVersion.fromVersionCode(targetTableVersion))
    } else {
      new HoodieStreamSourceV1(
        sqlContext, metaClient, metadataPath, userSchema, parameters, offsetRangeLimit, HoodieTableVersion.fromVersionCode(targetTableVersion))
    }
  }
}

object DefaultSource {

  private val log = LoggerFactory.getLogger(classOf[DefaultSource])

  def createRelation(sqlContext: SQLContext,
                     metaClient: HoodieTableMetaClient,
                     schema: StructType,
                     parameters: Map[String, String]): BaseRelation = {
    val tableType = metaClient.getTableType
    val isBootstrappedTable = metaClient.getTableConfig.getBootstrapBasePath.isPresent
    val queryType = SparkConfigUtils.getStringWithAltKeys(parameters, QUERY_TYPE)
    val isCdcQuery = queryType == QUERY_TYPE_INCREMENTAL_OPT_VAL &&
      parameters.get(INCREMENTAL_FORMAT.key).contains(INCREMENTAL_FORMAT_CDC_VAL)

    val createTimeLineRln = parameters.get(DataSourceReadOptions.CREATE_TIMELINE_RELATION.key())
    val createFSRln = parameters.get(DataSourceReadOptions.CREATE_FILESYSTEM_RELATION.key())

    if (createTimeLineRln.isDefined) {
      new TimelineRelation(sqlContext, parameters, metaClient)
    } else if (createFSRln.isDefined) {
      new FileSystemRelation(sqlContext, parameters, metaClient)
    } else {
      log.info(s"Is bootstrapped table => $isBootstrappedTable, tableType is: $tableType, queryType is: $queryType")

      // NOTE: In cases when Hive Metastore is used as catalog and the table is partitioned, schema in the HMS might contain
      //       Hive-specific partitioning columns created specifically for HMS to handle partitioning appropriately. In that
      //       case  we opt in to not be providing catalog's schema, and instead force Hudi relations to fetch the schema
      //       from the table itself
      val userSchema = if (isUsingHiveCatalog(sqlContext.sparkSession)) {
        None
      } else {
        Option(schema)
      }

      lazy val isNotMetadataTable = !metaClient.isMetadataTable
      lazy val tableVersion = if (SparkConfigUtils.containsConfigProperty(parameters, INCREMENTAL_READ_TABLE_VERSION)) {
        Integer.parseInt(parameters(INCREMENTAL_READ_TABLE_VERSION.key))
      } else {
        metaClient.getTableConfig.getTableVersion.versionCode()
      }
      lazy val hoodieTableSupportsCompletionTime = tableVersion >= HoodieTableVersion.EIGHT.versionCode()
      if (metaClient.getCommitsTimeline.filterCompletedInstants.countInstants() == 0) {
        new EmptyRelation(sqlContext, resolveSchema(metaClient, parameters, Some(schema)))
      } else if (isCdcQuery) {
        if (isNotMetadataTable) {
          if (tableType == COPY_ON_WRITE) {
            new HoodieCopyOnWriteCDCHadoopFsRelationFactory(
              sqlContext, metaClient, parameters, userSchema, isBootstrap = false).build()
          } else {
            new HoodieMergeOnReadCDCHadoopFsRelationFactory(
              sqlContext, metaClient, parameters, userSchema, isBootstrap = false).build()
          }
        } else {
          CDCRelation.getCDCRelation(sqlContext, metaClient, parameters)
        }
      } else {

        (tableType, queryType, isBootstrappedTable) match {
          case (COPY_ON_WRITE, QUERY_TYPE_SNAPSHOT_OPT_VAL, false) |
               (COPY_ON_WRITE, QUERY_TYPE_READ_OPTIMIZED_OPT_VAL, false) |
               (MERGE_ON_READ, QUERY_TYPE_READ_OPTIMIZED_OPT_VAL, false) =>
            if (isNotMetadataTable) {
              new HoodieCopyOnWriteSnapshotHadoopFsRelationFactory(
                sqlContext, metaClient, parameters, userSchema, isBootstrap = false).build()
            } else {
              resolveBaseFileOnlyRelation(sqlContext, userSchema, metaClient, parameters)
            }
          case (COPY_ON_WRITE, QUERY_TYPE_INCREMENTAL_OPT_VAL, _) =>
            if (hoodieTableSupportsCompletionTime) {
              new HoodieCopyOnWriteIncrementalHadoopFsRelationFactoryV2(
                sqlContext, metaClient, parameters, userSchema, isBootstrappedTable).build()
            } else {
              new HoodieCopyOnWriteIncrementalHadoopFsRelationFactoryV1(
                sqlContext, metaClient, parameters, userSchema, isBootstrappedTable).build()
            }

          case (MERGE_ON_READ, QUERY_TYPE_SNAPSHOT_OPT_VAL, false) =>
            if (isNotMetadataTable) {
              new HoodieMergeOnReadSnapshotHadoopFsRelationFactory(
                sqlContext, metaClient, parameters, userSchema, isBootstrap = false).build()
            } else {
              new MergeOnReadSnapshotRelation(sqlContext, parameters, metaClient, userSchema)
            }

          case (MERGE_ON_READ, QUERY_TYPE_SNAPSHOT_OPT_VAL, true) =>
            if (isNotMetadataTable) {
              new HoodieMergeOnReadSnapshotHadoopFsRelationFactory(
                sqlContext, metaClient, parameters, userSchema, isBootstrap = true).build()
            } else {
              HoodieBootstrapMORRelation(sqlContext, userSchema, metaClient, parameters)
            }

          case (MERGE_ON_READ, QUERY_TYPE_INCREMENTAL_OPT_VAL, _) =>
            if (hoodieTableSupportsCompletionTime) {
              new HoodieMergeOnReadIncrementalHadoopFsRelationFactoryV2(
                sqlContext, metaClient, parameters, userSchema, isBootstrappedTable).build()
            } else {
              new HoodieMergeOnReadIncrementalHadoopFsRelationFactoryV1(
                sqlContext, metaClient, parameters, userSchema, isBootstrappedTable).build()
            }

          case (_, _, true) =>
            new HoodieCopyOnWriteSnapshotHadoopFsRelationFactory(
              sqlContext, metaClient, parameters, userSchema, isBootstrap = true).build()

          case (_, _, _) =>
            throw new HoodieException(s"Invalid query type : $queryType for tableType: $tableType," +
              s"isBootstrappedTable: $isBootstrappedTable ")
        }
      }
    }
  }

  private def resolveBaseFileOnlyRelation(sqlContext: SQLContext,
                                          userSchema: Option[StructType],
                                          metaClient: HoodieTableMetaClient,
                                          optParams: Map[String, String]): BaseRelation = {
    val baseRelation = new BaseFileOnlyRelation(sqlContext, metaClient, optParams, userSchema)

    // NOTE: We fallback to [[HadoopFsRelation]] in all of the cases except ones requiring usage of
    //       [[BaseFileOnlyRelation]] to function correctly. This is necessary to maintain performance parity w/
    //       vanilla Spark, since some of the Spark optimizations are predicated on the using of [[HadoopFsRelation]].
    //
    //       You can check out HUDI-3896 for more details
    if (baseRelation.hasSchemaOnRead) {
      baseRelation
    } else {
      baseRelation.toHadoopFsRelation
    }
  }

  private def resolveSchema(metaClient: HoodieTableMetaClient,
                            parameters: Map[String, String],
                            schema: Option[StructType]): StructType = {
    val isCdcQuery = CDCRelation.isCDCEnabled(metaClient) &&
      parameters.get(QUERY_TYPE.key).contains(QUERY_TYPE_INCREMENTAL_OPT_VAL) &&
      parameters.get(INCREMENTAL_FORMAT.key).contains(INCREMENTAL_FORMAT_CDC_VAL)
    if (isCdcQuery) {
      CDCRelation.FULL_CDC_SPARK_SCHEMA
    } else {
      val schemaResolver = new TableSchemaResolver(metaClient)
      try {
        val avroSchema = schemaResolver.getTableAvroSchema
        AvroConversionUtils.convertAvroSchemaToStructType(avroSchema)
      } catch {
        case _: Exception =>
          if (schema.isEmpty || schema.get == null) {
            throw new HoodieSchemaNotFoundException("Failed to resolve source schema")
          }
          schema.get
      }
    }
  }
}
