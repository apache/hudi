/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.execution

import java.io.File
import java.util
import java.util.{Locale, Properties}

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hudi.DataSourceReadOptions.{QUERY_TYPE_OPT_KEY, QUERY_TYPE_READ_OPTIMIZED_OPT_VAL, QUERY_TYPE_SNAPSHOT_OPT_VAL}
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.HoodieSparkSqlWriter.{TableInstantInfo, toProperties}
import org.apache.hudi.{AvroConversionUtils, DataSourceReadOptions, DataSourceUtils, DataSourceWriteOptions, DefaultSource, HoodieBootstrapPartition, HoodieBootstrapRelation, HoodieSparkSqlWriter, HoodieSparkUtils, HoodieWriterUtils, MergeOnReadSnapshotRelation}
import org.apache.hudi.common.model.{OverwriteNonDefaultsWithLatestAvroPayload, OverwriteWithLatestAvroPayload}
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.client.{HoodieWriteResult, SparkRDDWriteClient}
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.model._
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.exception.{HoodieException, HoodieIOException}
import org.apache.hudi.hive.{HiveSyncConfig, HiveSyncTool}
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.payload.AWSDmsAvroPayload
import org.apache.hudi.spark3.internal.HoodieDataSourceInternalTable
import org.apache.log4j.LogManager
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, HiveTableRelation}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer



/**
  * hudi IUD utils
  */
object HudiSQLUtils {

  val AWSDMSAVROPAYLOAD = classOf[AWSDmsAvroPayload].getName
  val OVERWRITEWITHLATESTVROAYLOAD = classOf[OverwriteWithLatestAvroPayload].getName
  val OVERWRITENONDEFAULTSWITHLATESTAVROPAYLOAD = classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName
  val MERGE_MARKER = "_hoodie_merge_marker"

  private val log = LogManager.getLogger(getClass)

  private val tableConfigCache = CacheBuilder
    .newBuilder()
    .maximumSize(1000)
    .build(new CacheLoader[String, Properties] {
      override def load(k: String): Properties = {
        try {
          HoodieTableMetaClient.builder().setConf(SparkSession.active.sparkContext.hadoopConfiguration)
            .setBasePath(k).build().getTableConfig.getProperties
        } catch {
          // we catch expected error here
          case e: HoodieIOException =>
            log.error(e.getMessage)
            new Properties()
          case t: Throwable =>
            throw t
        }
      }
    })

  def getPropertiesFromTableConfigCache(path: String): Properties = {
    if (path.isEmpty) {
      throw new HoodieIOException("unexpected empty hoodie table basePath")
    }
    tableConfigCache.get(path)
  }

  private def matchHoodieRelation(relation: BaseRelation): Boolean = relation match {
    case h: HadoopFsRelation if (h.options.get("hoodie.datasource.write.table.type") != null) => true
    case m: MergeOnReadSnapshotRelation => true
    case b: HoodieBootstrapRelation => true
    case _ => false
  }

  def isHudiRelation(table: LogicalPlan): Boolean = {
    table.collect {
      case h: HiveTableRelation if (h.tableMeta.storage.inputFormat.getOrElse("").contains("Hoodie")) =>
        true
      case DataSourceV2Relation(_: HoodieDataSourceInternalTable, _, _, _, _) => true
      case LogicalRelation(r: BaseRelation, _, _, _) if (matchHoodieRelation(r)) => true
    }.nonEmpty
  }

  def isHudiRelation(table: CatalogTable): Boolean = {
    table.provider.map(_.toLowerCase(Locale.ROOT)).getOrElse("").equals("hudi")
  }

  def isHudiRelation(tableId: TableIdentifier, spark: SparkSession): Boolean = {
    val table = spark.sessionState.catalog.getTableMetadata(tableId)
    isHudiRelation(table)
  }

  def getTablePath(spark: SparkSession, table: CatalogTable): String = {
    val url = if (table.tableType == CatalogTableType.MANAGED) {
      Some(spark.sessionState.catalog.defaultTablePath(table.identifier))
    } else {
      table.storage.locationUri
    }
    val fs = new Path(url.get).getFileSystem(spark.sparkContext.hadoopConfiguration)
    val rawPath = fs.makeQualified(new Path(url.get)).toUri.toString
    // remove placeHolder
    if (rawPath.endsWith("-PLACEHOLDER")) {
      rawPath.substring(0, rawPath.length() - 16)
    } else {
      rawPath
    }
  }

  def buildDefaultParameter(parameters: Map[String, String], enableHive: Boolean = true): Map[String, String] = {
    val newParameters = HoodieWriterUtils.parametersWithWriteDefaults(parameters) ++ Map(
      HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY -> "org.apache.hudi.hive.MultiPartKeysValueExtractor",
      KEYGENERATOR_CLASS_PROP -> "org.apache.hudi.keygen.ComplexKeyGenerator",
      HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY -> "org.apache.hudi.hive.MultiPartKeysValueExtractor")
    val properties = new Properties()
    properties.putAll(newParameters.asJava)
    // correct partition info
    if (properties.getProperty(PARTITIONPATH_FIELD_OPT_KEY, "").isEmpty) {
      properties.put(KEYGENERATOR_CLASS_PROP, "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
    }
    // set HIVE_STYLE_PARTITIONING_OPT_KEY to be true
    properties.put(HIVE_STYLE_PARTITIONING_OPT_KEY, "true")

    if (enableHive) {
      properties.put(HIVE_SYNC_ENABLED_OPT_KEY, "true")
      properties.put(HIVE_USE_JDBC_OPT_KEY, "false")
      // correct hive partition info
      if (properties.getProperty(PARTITIONPATH_FIELD_OPT_KEY, "").isEmpty) {
        properties.put(HIVE_PARTITION_FIELDS_OPT_KEY, "")
        properties.put(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, "org.apache.hudi.hive.NonPartitionedExtractor")
      }
    } else {
      // disable Hive Sync
      properties.put(HIVE_SYNC_ENABLED_OPT_KEY, "false")

    }
    properties.asScala.toMap
  }

  def getTablePathFromRelation(table: LogicalPlan, sparkSession: SparkSession): String = {
    getHoodiePropsFromRelation(table, sparkSession).getOrElse("path", "")
  }

  /**
    * build props form relation
    */
  def getHoodiePropsFromRelation(table: LogicalPlan, sparkSession: SparkSession): Map[String, String] = {
    table.collect {
      case h: HiveTableRelation =>
        val table = h.asInstanceOf[HiveTableRelation].tableMeta
        val db = table.identifier.database.getOrElse("default")
        val rawTableName = table.identifier.table
        val tableName = if (rawTableName.endsWith("_ro") || rawTableName.endsWith("_rt")) {
          rawTableName.substring(0, rawTableName.size - 3)
        } else {
          rawTableName
        }
        val savePath = HudiSQLUtils.getTablePath(sparkSession, table)
        table.properties ++ Map(HIVE_DATABASE_OPT_KEY -> db,
          "inputformat" -> table.storage.inputFormat.getOrElse(""),
          HIVE_TABLE_OPT_KEY -> tableName,
          "path" -> savePath, "currentTable" -> table.identifier.quotedString) ++ getPropertiesFromTableConfigCache(savePath).asScala.toMap
      case l @ LogicalRelation(r: BaseRelation, _, _, _) =>
        val catalogTable = l.catalogTable
        val catalogProp = if (catalogTable.isEmpty) {
          Map.empty[String, String]
        } else {
          val savePath = HudiSQLUtils.getTablePath(sparkSession, catalogTable.get)
          val tablePath = if (savePath.endsWith("/*")) {
            savePath.dropRight(catalogTable.get.partitionColumnNames.size*2 + 1)
          } else {
            savePath
          }
          catalogTable.get.storage.properties ++ Map("currentTable" -> catalogTable.get.identifier.unquotedString,
            "path" -> tablePath) ++ getPropertiesFromTableConfigCache(tablePath).asScala.toMap
        }

        val relationProp = r match {
          case h: HadoopFsRelation => h.options.updated("path", h.options.getOrElse("hoodie.base.path", ""))
          case m: MergeOnReadSnapshotRelation => m.optParams.updated("path", m.optParams.getOrElse("hoodie.base.path", ""))
          case b: HoodieBootstrapRelation => b.optParams.updated("path", b.optParams.getOrElse("hoodie.base.path", ""))
          case _ => Map.empty[String, String]
        }
        relationProp ++ catalogProp
    }.headOption.getOrElse(Map.empty[String, String])
  }

  def getRequredFieldsFromTableConf(tablePath: String): (String, Seq[String]) = {
    val tableConf = tableConfigCache.get(tablePath)
    val recordKeyFields = tableConf.getProperty(RECORDKEY_FIELD_OPT_KEY).split(",").map(_.trim).filter(!_.isEmpty)
    val partitionPathFields = tableConf.getProperty(PARTITIONPATH_FIELD_OPT_KEY).split(",").map(_.trim).filter(!_.isEmpty)
    val preCombineField = tableConf.getProperty(PRECOMBINE_FIELD_OPT_KEY).trim
    (preCombineField, recordKeyFields ++ partitionPathFields)
  }

  def merge(
      df: DataFrame,
      spark: SparkSession,
      tableMeta: Map[String, String],
      trySkipIndex: Boolean = true,
      enableHive: Boolean = true): Unit = {
    val savePath = tableMeta.getOrElse("path", throw new HoodieException("cannot find table Path, pls check your hoodie table!!!"))
    val payload = HudiSQLUtils.tableConfigCache.get(savePath).getProperty(PAYLOAD_CLASS_OPT_KEY, DEFAULT_PAYLOAD_OPT_VAL)

    val df2Merge = payload match {
      case AWSDMSAVROPAYLOAD =>
        df.drop("Op").withColumnRenamed(MERGE_MARKER, "Op")
      case _ =>
        val f: (String) => Boolean = {x => {if (x == "D") true else false}}
        val markDeleteUdf = udf(f)
        df.withColumn(MERGE_MARKER, markDeleteUdf(col(MERGE_MARKER)))
    }
    update(df2Merge, buildDefaultParameter(tableMeta, enableHive), spark, WriteOperationType.UPSERT, trySkipIndex, enableHive)
  }

  def update(
      df: DataFrame,
      parameters: Map[String, String],
      spark: SparkSession,
      writeOperationType: WriteOperationType = WriteOperationType.UPSERT,
      trySkipIndex: Boolean = true, enableHive: Boolean = true): Unit = {
    val tablePath = parameters.get("path").get

    val tableConfig = tableConfigCache.get(tablePath)
    tableConfig.put(TABLE_TYPE_OPT_KEY, tableConfig.getProperty("hoodie.table.type"))
    val tblName = tableConfig.get("hoodie.table.name").toString
    tableConfig.put(HIVE_TABLE_OPT_KEY, tblName)
    val dataWriteOptions = parameters ++ addSetProperties(parameters, spark) ++ tableConfig.asScala

    checkWriteOptions(dataWriteOptions, enableHive)

    if (df.schema.exists(f => f.name.endsWith(HoodieRecord.FILENAME_METADATA_FIELD)) && trySkipIndex) {
      log.info("try to upsert table skip index")
      val par = dataWriteOptions.getOrElse("par", "1500")
      upsertSkipIndex(df, dataWriteOptions ++ Map("hoodie.tagging.before.insert" -> "false",
          UPSERT_PARALLELISM -> par,
          COMBINE_BEFORE_UPSERT_PROP -> "false"),
        spark, dataWriteOptions.getOrElse("hoodie.table.name", tblName))
      if (enableHive) {
        doSyncToHive(dataWriteOptions, new Path(tablePath), spark.sparkContext.hadoopConfiguration)
      }
    } else {
      log.info("try to upsert table directly")
      upsertDirectly(df, (dataWriteOptions ++ Map("hoodie.tagging.before.insert" -> "true")).asJava, writeOperationType, SaveMode.Append, tablePath)
    }
  }

  def checkWriteOptions(parameters: Map[String, String], enableHive: Boolean = true): Unit = {
    def checkNeededOption(option: String): Unit = {
      parameters.getOrElse(option, throw new HoodieException(s"cannot find ${option}, pls set it when you create hudi table"))
    }
    checkNeededOption(PRECOMBINE_FIELD_OPT_KEY)
    checkNeededOption(RECORDKEY_FIELD_OPT_KEY)
    checkNeededOption(PARTITIONPATH_FIELD_OPT_KEY)

    if (parameters.get(PARTITIONPATH_FIELD_OPT_KEY).isEmpty) {
      if (!parameters.get(KEYGENERATOR_CLASS_PROP).equals("org.apache.hudi.keygen.NonpartitionedKeyGenerator")) {
        throw new HoodieException(s"hooide.datasource.write.keygenerator.class for NonPartition table " +
          s"should set to be org.apache.hudi.keygen.NonPartitionedKeyGenerator")
      }
    }
    // do hive check
    if (enableHive) {
      val hiveTableName = parameters.getOrElse(HIVE_TABLE_OPT_KEY, "")
      if (hiveTableName.isEmpty || hiveTableName.equalsIgnoreCase("unknow")) {
        throw new HoodieException(s"find incorret vale ${hiveTableName} for ${HIVE_TABLE_OPT_KEY}")
      }
    }

  }

  def tableExists(tablePath: String, conf: Configuration): Boolean = {
    val basePath = new Path(tablePath)
    val fs = basePath.getFileSystem(conf)
    val metaPath = new Path(basePath, HoodieTableMetaClient.METAFOLDER_NAME)
    fs.exists(metaPath)
  }

  /**
    * do merge skipping index tag, which is only used by sql
    * we can use _hoodie_record_key, _hoodie_partition_path, _hoodie_file_name to build tagged hudi rdd directly
    */
  private def upsertSkipIndex(
      df: DataFrame,
      parameters: Map[String, String],
      spark: SparkSession,
      tblName: String): Unit = {

    val (structName, nameSpace) = AvroConversionUtils.getAvroRecordNameAndNamespace(tblName)
    spark.sparkContext.getConf.registerKryoClasses(
      Array(classOf[org.apache.avro.generic.GenericData],
        classOf[org.apache.avro.Schema]))
    // change mark delete filedStruct
    val fixedSchema = {
      val last = df.schema.last
      StructType(df.schema.dropRight(1)).add(last.name, last.dataType, true)
    }
    val schema = AvroConversionUtils.convertStructTypeToAvroSchema(df.schema, structName, nameSpace)
    spark.sparkContext.getConf.registerAvroSchemas(schema)

    // delete MERGE_MARKER in schema
    val fixedAvroSchema = if (fixedSchema.fields.exists(p => p.name.equals(MERGE_MARKER))) {
      AvroConversionUtils.convertStructTypeToAvroSchema(StructType(fixedSchema.dropRight(1)), structName, nameSpace)
    } else {
      schema
    }

    log.info(s"Registered avro schema: ${schema.toString(true)}")
    //
    val keyGenerator = DataSourceUtils.createKeyGenerator(toProperties(parameters))
    // create hoodieRecord directly, insert record is should be considered
    val genericRecords: RDD[GenericRecord] = HoodieSparkUtils.createRdd(df, schema, structName, nameSpace)
    val combineKey = parameters.get(PRECOMBINE_FIELD_OPT_KEY).get.trim
    val hoodieRecords = genericRecords.map { gr =>

      val orderingVal = HoodieAvroUtils
        .getNestedFieldVal(gr, combineKey, false).asInstanceOf[Comparable[_]]

      val canSetLocation: Boolean = if (gr.get(RECORDKEY_FIELD_OPT_KEY) == null
        || gr.get(PARTITIONPATH_FIELD_OPT_KEY) == null
        || gr.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD) == null) {
        false
      } else {
        true
      }

      val hoodieRecord = if (!canSetLocation) {
        if (gr.get(MERGE_MARKER) != null && gr.get(MERGE_MARKER).toString.toBoolean) {
          DataSourceUtils.createHoodieRecord(gr, orderingVal, keyGenerator.getKey(gr), "org.apache.hudi.common.model.EmptyHoodieRecordPayload")
        } else {
          DataSourceUtils.createHoodieRecord(gr, orderingVal, keyGenerator.getKey(gr), parameters(PAYLOAD_CLASS_OPT_KEY))
        }
      } else {
        DataSourceUtils.createHoodieRecord(gr,
          orderingVal,
          new HoodieKey(gr.get(RECORDKEY_FIELD_OPT_KEY).toString, gr.get(PARTITIONPATH_FIELD_OPT_KEY).toString),
          parameters(PARTITIONPATH_FIELD_OPT_KEY))
      }

      if (canSetLocation) {
        hoodieRecord.unseal()
        // set location
        hoodieRecord.setCurrentLocation( new HoodieRecordLocation(gr.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString
          , gr.get(HoodieRecord.FILENAME_METADATA_FIELD).toString.split("_")(0)))
        hoodieRecord.seal()
      }

      hoodieRecord
    }.toJavaRDD()

    // create a HoodieWriteClient $ issue the write
    val jsc = new JavaSparkContext(spark.sparkContext)
    val path = parameters.get("path")
    val basePath = new Path(path.get)
    val client = DataSourceUtils.createHoodieClient(jsc,
      fixedAvroSchema.toString, path.get, tblName, mapAsJavaMap(parameters - HOODIE_AUTO_COMMIT_PROP))
      .asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]]

    val instantTime = HoodieActiveTimeline.createNewInstantTime()
    val commitActionType = DataSourceUtils.getCommitActionType(WriteOperationType.UPSERT,
      if (parameters.getOrElse("hoodie.table.type", "COPY_ON_WRITE") == "COPY_ON_WRITE") HoodieTableType.COPY_ON_WRITE else HoodieTableType.MERGE_ON_READ)

    client.startCommitWithTime(instantTime, commitActionType)
    val writeResult = DataSourceUtils.doWriteOperation(client, hoodieRecords, instantTime, WriteOperationType.UPSERT)

    // commit and post commit
    commitAndPerformPostOperations(writeResult,
      parameters,
      client,
      HoodieTableMetaClient.builder().setConf(spark.sparkContext.hadoopConfiguration).setBasePath(path.get)
        .build().getTableConfig,
      jsc,
      TableInstantInfo(basePath, instantTime, commitActionType, WriteOperationType.UPSERT))
  }

  private def commitAndPerformPostOperations(
      writeResult: HoodieWriteResult,
      parameters: Map[String, String],
      client: SparkRDDWriteClient[HoodieRecordPayload[Nothing]],
      tableConfig: HoodieTableConfig,
      jsc: JavaSparkContext,
      tableInstantInfo: TableInstantInfo): Unit = {
    val errorCount = writeResult.getWriteStatuses.rdd.filter(ws => ws.hasErrors).count()
    if (errorCount == 0) {
      log.info("No errors. Proceeding to commit the write.")
      val metaMap = parameters.filter(kv => kv._1.startsWith(parameters(COMMIT_METADATA_KEYPREFIX_OPT_KEY)))
      val commitSuccess =
        client.commit(tableInstantInfo.instantTime, writeResult.getWriteStatuses,
          org.apache.hudi.common.util.Option.of(new util.HashMap[String, String](mapAsJavaMap(metaMap))),
          tableInstantInfo.commitActionType, writeResult.getPartitionToReplaceFileIds)
      if (commitSuccess) {
        log.info("Commit" + tableInstantInfo.instantTime + " successful!")
      } else {
        log.info("Commit " + tableInstantInfo.instantTime + " failed!")
      }
    } else {
      log.info(s"${tableInstantInfo.operation} failed with $errorCount errors :")
      if (log.isTraceEnabled) {
        log.trace("Printing out the top 100 errors")
        writeResult.getWriteStatuses.rdd.filter(ws => ws.hasErrors)
          .take(100)
          .foreach(ws => {
            log.trace("Global error :", ws.getGlobalError)
            if (ws.getErrors.size() > 0) {
              ws.getErrors.asScala.foreach(kt => log.trace(s"Error for key: ${kt._1}", kt._2))
            }
          })
      }
    }
  }

  /**
    * do merge directly, just use hoodie dataFrame api
    */
  private def upsertDirectly(
      df: DataFrame,
      options: java.util.Map[String, String],
      writeOperationType: WriteOperationType = WriteOperationType.UPSERT,
      mode: SaveMode,
      savePath: String): Unit = {
    val par = options.getOrDefault("par", "1500")
    df.write.format("hudi")
      .option(BULKINSERT_PARALLELISM, par)
      .option(INSERT_PARALLELISM, par)
      .option(UPSERT_PARALLELISM, par)
      .option(DELETE_PARALLELISM, par)
      .options(options)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, writeOperationType.value())
      .mode(mode)
      .save(savePath)
  }

  /**
    * now just sync hudi table info to hive
    * we use hive driver to do sync, maybe it's better to use spark
    */
  private def doSyncToHive(parameters: Map[String, String], basePath: Path, hadoopConf: Configuration): Unit = {
    log.info("Syncing to Hive Start!!!")
    val hiveSyncConfig = buildSyncConfig(basePath, parameters)
    val hiveConf = new HiveConf()
    val fs = basePath.getFileSystem(hadoopConf)
    hiveConf.addResource(fs.getConf)
    new HiveSyncTool(hiveSyncConfig, hiveConf, fs).syncHoodieTable()
  }

  private def buildSyncConfig(basePath: Path, parameters: Map[String, String]): HiveSyncConfig = {
    val hiveSyncConfig: HiveSyncConfig = new HiveSyncConfig()
    hiveSyncConfig.basePath = basePath.toString
    hiveSyncConfig.baseFileFormat = parameters(HIVE_BASE_FILE_FORMAT_OPT_KEY);
    hiveSyncConfig.usePreApacheInputFormat =
      parameters.get(HIVE_USE_PRE_APACHE_INPUT_FORMAT_OPT_KEY).exists(r => r.toBoolean)
    hiveSyncConfig.databaseName = parameters(HIVE_DATABASE_OPT_KEY)
    hiveSyncConfig.tableName = parameters(HIVE_TABLE_OPT_KEY)
    hiveSyncConfig.partitionFields =
      ListBuffer(parameters(HIVE_PARTITION_FIELDS_OPT_KEY).split(",").map(_.trim).filter(!_.isEmpty).toList: _*).asJava
    hiveSyncConfig.partitionValueExtractorClass = parameters(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY)
    hiveSyncConfig.useJdbc = false
    hiveSyncConfig.supportTimestamp = parameters.get(HIVE_SUPPORT_TIMESTAMP).exists(r => r.toBoolean)
    hiveSyncConfig.autoCreateDatabase = parameters.get(HIVE_AUTO_CREATE_DATABASE_OPT_KEY).exists(r => r.toBoolean)
    hiveSyncConfig.decodePartition = parameters.getOrElse(URL_ENCODE_PARTITIONING_OPT_KEY,
      DEFAULT_URL_ENCODE_PARTITIONING_OPT_VAL).toBoolean
    hiveSyncConfig
  }

  /**
    * user can set some properties by sql statement: set xxxx=xx
    * we try to grab some useful property for hudi
    */
  private def addSetProperties(parameters: Map[String, String], spark: SparkSession): Map[String, String] = {
    val par = spark.sessionState.conf.getConfString("spark.hoodie.shuffle.parallelism", "1500")
    parameters ++ spark.sessionState.conf.getAllConfs ++ Map("par" -> par)
  }

  def initFirstCommit(spark: SparkSession, schema: StructType, properties: Map[String, String], conf: Configuration, tablePath: String, tableName: String): Unit = {
    val tableMetaClient = HoodieTableMetaClient.builder().setConf(conf).setBasePath(tablePath).build()
    if (tableMetaClient.getActiveTimeline.getCommitsTimeline.empty()) {
      val (structType, nameSpace) = AvroConversionUtils.getAvroRecordNameAndNamespace(tableName)
      val schemaStr = AvroConversionUtils.convertStructTypeToAvroSchema(schema, structType, nameSpace).toString
      val jsc = new JavaSparkContext(spark.sparkContext)
      val client = DataSourceUtils.createHoodieClient(jsc, schemaStr, tablePath, tableName, properties.asJava)
        .asInstanceOf[SparkRDDWriteClient[HoodieRecordPayload[Nothing]]]
      val instance = "00000000000000"
      client.startCommitWithTime(instance)
      val writeRecords = jsc.parallelize(Seq[HoodieRecord[HoodieRecordPayload[Nothing]]](), 1)
      val result = client.bulkInsert(writeRecords, instance)
      client.commit(instance, result)
    }
  }

  object HoodieV1Relation {
    def unapply(plan: LogicalPlan): Option[LogicalPlan] = plan match {
      case dsv2 @ DataSourceV2Relation(t: HoodieDataSourceInternalTable, _, _, _, options) => Some(fromV2Relation(dsv2, t, options))
      case _ => None
    }

    def fromV2Relation(dsv2: DataSourceV2Relation, table: HoodieDataSourceInternalTable, options: CaseInsensitiveStringMap): LogicalRelation = {
      val datasourceV1 = new DefaultSource
      var properties = table.properties().asScala.toMap ++ options.asScala
      val partitionPath = table.properties().asScala.getOrElse(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY, "")
        .split(",").map(_ => "*").mkString(java.io.File.separator)

      val tableName = properties.get(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY).getOrElse(table.name())
      if (tableName.endsWith("_rt")) {
        properties = properties ++ Map[String, String](DataSourceReadOptions.QUERY_TYPE_OPT_KEY -> DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL)
      } else if (tableName.endsWith("_ro")) {
        properties = properties ++ Map[String, String](DataSourceReadOptions.QUERY_TYPE_OPT_KEY -> DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      }

      properties = properties ++ Map("path" -> (properties("path") + java.io.File.separator + partitionPath),
        HIVE_DATABASE_OPT_KEY -> dsv2.identifier.get.namespace().last,
        HIVE_TABLE_OPT_KEY -> dsv2.identifier.get.name(), "basePath" -> properties("path"))

      val relation = datasourceV1.createRelation(SparkSession.active.sqlContext, properties, table.schema())

      LogicalRelation(relation.asInstanceOf[BaseRelation], dsv2.output, None, false)
    }
  }
}
