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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hudi.{AvroConversionUtils, HoodieFileIndex, HoodiePartitionCDCFileGroupMapping, HoodiePartitionFileSliceMapping, HoodieTableSchema, HoodieTableState, SparkAdapterSupport, SparkFileFormatInternalRowReaderContext}
import org.apache.hudi.avro.AvroSchemaUtils
import org.apache.hudi.cdc.{CDCFileGroupIterator, CDCRelation, HoodieCDCFileGroupSplit}
import org.apache.hudi.client.utils.SparkInternalSchemaConverter
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.read.HoodieFileGroupReader
import org.apache.hudi.data.CloseableIteratorListener
import org.apache.hudi.internal.schema.InternalSchema
import org.apache.hudi.storage.StorageConfiguration
import org.apache.hudi.storage.hadoop.{HadoopStorageConfiguration, HoodieHadoopStorage}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.HoodieCatalystExpressionUtils.generateUnsafeProjection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JoinedRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnarBatchUtils}
import org.apache.spark.util.SerializableConfiguration

import java.io.Closeable

trait HoodieFormatTrait {

  // Used so that the planner only projects once and does not stack overflow
  var isProjected: Boolean = false
  def getRequiredFilters: Seq[Filter]
}

/**
 * This class utilizes {@link HoodieFileGroupReader} and its related classes to support reading
 * from Parquet formatted base files and their log files.
 */
class HoodieFileGroupReaderBasedParquetFileFormat(tablePath: String,
                                                  tableSchema: HoodieTableSchema,
                                                  tableName: String,
                                                  queryTimestamp: String,
                                                  mandatoryFields: Seq[String],
                                                  isMOR: Boolean,
                                                  isBootstrap: Boolean,
                                                  isIncremental: Boolean,
                                                  isCDC: Boolean,
                                                  validCommits: String,
                                                  shouldUseRecordPosition: Boolean,
                                                  requiredFilters: Seq[Filter]
                                                 ) extends ParquetFileFormat with SparkAdapterSupport with HoodieFormatTrait {

  def getRequiredFilters: Seq[Filter] = requiredFilters

  private val sanitizedTableName = AvroSchemaUtils.getAvroRecordQualifiedName(tableName)

  /**
   * Support batch needs to remain consistent, even if one side of a bootstrap merge can support
   * while the other side can't
   */
  private var supportBatchCalled = false
  private var supportBatchResult = false

  override def supportBatch(sparkSession: SparkSession, schema: StructType): Boolean = {
    if (!supportBatchCalled || supportBatchResult) {
      supportBatchCalled = true
      supportBatchResult = !isMOR && !isIncremental && !isBootstrap && super.supportBatch(sparkSession, schema)
    }
    supportBatchResult
  }

  //for partition columns that we read from the file, we don't want them to be constant column vectors so we
  //modify the vector types in this scenario
  override def vectorTypes(requiredSchema: StructType,
                           partitionSchema: StructType,
                           sqlConf: SQLConf): Option[Seq[String]] = {
    val originalVectorTypes = super.vectorTypes(requiredSchema, partitionSchema, sqlConf)
    if (mandatoryFields.isEmpty) {
      originalVectorTypes
    } else {
      val regularVectorType = if (!sqlConf.offHeapColumnVectorEnabled) {
        classOf[OnHeapColumnVector].getName
      } else {
        classOf[OffHeapColumnVector].getName
      }
      originalVectorTypes.map {
        o: Seq[String] => o.zipWithIndex.map(a => {
          if (a._2 >= requiredSchema.length && mandatoryFields.contains(partitionSchema.fields(a._2 - requiredSchema.length).name)) {
            regularVectorType
          } else {
            a._1
          }
        })
      }
    }
  }

  private lazy val internalSchemaOpt: org.apache.hudi.common.util.Option[InternalSchema] = if (tableSchema.internalSchema.isEmpty) {
    org.apache.hudi.common.util.Option.empty()
  } else {
    org.apache.hudi.common.util.Option.of(tableSchema.internalSchema.get)
  }

  override def isSplitable(sparkSession: SparkSession,
                           options: Map[String, String],
                           path: Path): Boolean = false

  override def buildReaderWithPartitionValues(spark: SparkSession,
                                              dataSchema: StructType,
                                              partitionSchema: StructType,
                                              requiredSchema: StructType,
                                              filters: Seq[Filter],
                                              options: Map[String, String],
                                              hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val outputSchema = StructType(requiredSchema.fields ++ partitionSchema.fields)
    val isCount = requiredSchema.isEmpty && !isMOR && !isIncremental
    val augmentedStorageConf = new HadoopStorageConfiguration(hadoopConf).getInline
    setSchemaEvolutionConfigs(augmentedStorageConf)
    val (remainingPartitionSchemaArr, fixedPartitionIndexesArr) = partitionSchema.fields.toSeq.zipWithIndex.filter(p => !mandatoryFields.contains(p._1.name)).unzip

    // The schema of the partition cols we want to append the value instead of reading from the file
    val remainingPartitionSchema = StructType(remainingPartitionSchemaArr)

    // index positions of the remainingPartitionSchema fields in partitionSchema
    val fixedPartitionIndexes = fixedPartitionIndexesArr.toSet

    // schema that we want fg reader to output to us
    val requestedSchema = StructType(requiredSchema.fields ++ partitionSchema.fields.filter(f => mandatoryFields.contains(f.name)))
    val requestedAvroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(requestedSchema, sanitizedTableName)
    val dataAvroSchema = AvroConversionUtils.convertStructTypeToAvroSchema(dataSchema, sanitizedTableName)
    val parquetFileReader = spark.sparkContext.broadcast(sparkAdapter.createParquetFileReader(supportBatchResult,
      spark.sessionState.conf, options, augmentedStorageConf.unwrap()))
    val broadcastedStorageConf = spark.sparkContext.broadcast(new SerializableConfiguration(augmentedStorageConf.unwrap()))
    val fileIndexProps: TypedProperties = HoodieFileIndex.getConfigProperties(spark, options, null)

    (file: PartitionedFile) => {
      val storageConf = new HadoopStorageConfiguration(broadcastedStorageConf.value.value)
      val iter = file.partitionValues match {
        // Snapshot or incremental queries.
        case fileSliceMapping: HoodiePartitionFileSliceMapping =>
          val filegroupName = FSUtils.getFileIdFromFilePath(sparkAdapter
            .getSparkPartitionedFileUtils.getPathFromPartitionedFile(file))
          fileSliceMapping.getSlice(filegroupName) match {
            case Some(fileSlice) if !isCount && (requiredSchema.nonEmpty || fileSlice.getLogFiles.findAny().isPresent) =>
              val readerContext = new SparkFileFormatInternalRowReaderContext(parquetFileReader.value, filters, requiredFilters)
              val metaClient: HoodieTableMetaClient = HoodieTableMetaClient
                .builder().setConf(storageConf).setBasePath(tablePath).build
              val props = metaClient.getTableConfig.getProps
              options.foreach(kv => props.setProperty(kv._1, kv._2))
              val reader = new HoodieFileGroupReader[InternalRow](readerContext, new HoodieHadoopStorage(metaClient.getBasePath, storageConf), tablePath, queryTimestamp,
                fileSlice, dataAvroSchema, requestedAvroSchema, internalSchemaOpt, metaClient, props, file.start, file.length, shouldUseRecordPosition, false)
              reader.initRecordIterators()
              // Append partition values to rows and project to output schema
              appendPartitionAndProject(
                reader.getClosableIterator,
                requestedSchema,
                remainingPartitionSchema,
                outputSchema,
                fileSliceMapping.getPartitionValues,
                fixedPartitionIndexes)

            case _ =>
              readBaseFile(file, parquetFileReader.value, requestedSchema, remainingPartitionSchema, fixedPartitionIndexes,
                requiredSchema, partitionSchema, outputSchema, filters, storageConf)
          }
        // CDC queries.
        case hoodiePartitionCDCFileGroupSliceMapping: HoodiePartitionCDCFileGroupMapping =>
          buildCDCRecordIterator(hoodiePartitionCDCFileGroupSliceMapping, parquetFileReader.value, storageConf, fileIndexProps, requiredSchema)

        case _ =>
          readBaseFile(file, parquetFileReader.value, requestedSchema, remainingPartitionSchema, fixedPartitionIndexes,
            requiredSchema, partitionSchema, outputSchema, filters, storageConf)
      }
      CloseableIteratorListener.addListener(iter)
    }
  }

  private def setSchemaEvolutionConfigs(conf: StorageConfiguration[Configuration]): Unit = {
    if (internalSchemaOpt.isPresent) {
      conf.set(SparkInternalSchemaConverter.HOODIE_TABLE_PATH, tablePath)
      conf.set(SparkInternalSchemaConverter.HOODIE_VALID_COMMITS_LIST, validCommits)
    }
  }

  private def buildCDCRecordIterator(hoodiePartitionCDCFileGroupSliceMapping: HoodiePartitionCDCFileGroupMapping,
                                     parquetFileReader: SparkParquetReader,
                                     storageConf: StorageConfiguration[Configuration],
                                     props: TypedProperties,
                                     requiredSchema: StructType): Iterator[InternalRow] = {
    val fileSplits = hoodiePartitionCDCFileGroupSliceMapping.getFileSplits().toArray
    val cdcFileGroupSplit: HoodieCDCFileGroupSplit = HoodieCDCFileGroupSplit(fileSplits)
    props.setProperty(HoodieTableConfig.HOODIE_TABLE_NAME_KEY, tableName)
    val cdcSchema = CDCRelation.FULL_CDC_SPARK_SCHEMA
    val metaClient = HoodieTableMetaClient.builder
      .setBasePath(tablePath).setConf(storageConf.newInstance()).build()
    new CDCFileGroupIterator(
      cdcFileGroupSplit,
      metaClient,
      storageConf,
      (file: PartitionedFile) =>
        parquetFileReader.read(file, tableSchema.structTypeSchema, new StructType(), internalSchemaOpt, Seq.empty, storageConf),
      tableSchema,
      cdcSchema,
      requiredSchema,
      props)
  }

  private def appendPartitionAndProject(iter: HoodieFileGroupReader.HoodieFileGroupReaderIterator[InternalRow],
                                        inputSchema: StructType,
                                        partitionSchema: StructType,
                                        to: StructType,
                                        partitionValues: InternalRow,
                                        fixedPartitionIndexes: Set[Int]): Iterator[InternalRow] = {
    if (partitionSchema.isEmpty) {
      //'inputSchema' and 'to' should be the same so the projection will just be an identity func
      projectSchema(iter, inputSchema, to)
    } else {
      val fixedPartitionValues = if (partitionSchema.length == partitionValues.numFields) {
        //need to append all of the partition fields
        partitionValues
      } else {
        //some partition fields read from file, some were not
        getFixedPartitionValues(partitionValues, partitionSchema, fixedPartitionIndexes)
      }
      val unsafeProjection = generateUnsafeProjection(StructType(inputSchema.fields ++ partitionSchema.fields), to)
      val joinedRow = new JoinedRow()
      makeCloseableFileGroupMappingRecordIterator(iter, d => unsafeProjection(joinedRow(d, fixedPartitionValues)))
    }
  }

  private def projectSchema(iter: HoodieFileGroupReader.HoodieFileGroupReaderIterator[InternalRow],
                            from: StructType,
                            to: StructType): Iterator[InternalRow] = {
    val unsafeProjection = generateUnsafeProjection(from, to)
    makeCloseableFileGroupMappingRecordIterator(iter, d => unsafeProjection(d))
  }

  private def makeCloseableFileGroupMappingRecordIterator(closeableFileGroupRecordIterator: HoodieFileGroupReader.HoodieFileGroupReaderIterator[InternalRow],
                                                          mappingFunction: Function[InternalRow, InternalRow]): Iterator[InternalRow] = {
    CloseableIteratorListener.addListener(closeableFileGroupRecordIterator)
    new Iterator[InternalRow] with Closeable {
      override def hasNext: Boolean = closeableFileGroupRecordIterator.hasNext

      override def next(): InternalRow = mappingFunction(closeableFileGroupRecordIterator.next())

      override def close(): Unit = closeableFileGroupRecordIterator.close()
    }
  }

  private def readBaseFile(file: PartitionedFile, parquetFileReader: SparkParquetReader, requestedSchema: StructType,
                           remainingPartitionSchema: StructType, fixedPartitionIndexes: Set[Int], requiredSchema: StructType,
                           partitionSchema: StructType, outputSchema: StructType, filters: Seq[Filter],
                           storageConf: StorageConfiguration[Configuration]): Iterator[InternalRow] = {
    if (remainingPartitionSchema.fields.length == partitionSchema.fields.length) {
      //none of partition fields are read from the file, so the reader will do the appending for us
      parquetFileReader.read(file, requiredSchema, partitionSchema, internalSchemaOpt, filters, storageConf)
    } else if (remainingPartitionSchema.fields.length == 0) {
      //we read all of the partition fields from the file
      val pfileUtils = sparkAdapter.getSparkPartitionedFileUtils
      //we need to modify the partitioned file so that the partition values are empty
      val modifiedFile = pfileUtils.createPartitionedFile(InternalRow.empty, pfileUtils.getPathFromPartitionedFile(file), file.start, file.length)
      //and we pass an empty schema for the partition schema
      parquetFileReader.read(modifiedFile, outputSchema, new StructType(), internalSchemaOpt, filters, storageConf)
    } else {
      //need to do an additional projection here. The case in mind is that partition schema is "a,b,c" mandatoryFields is "a,c",
      //then we will read (dataSchema + a + c) and append b. So the final schema will be (data schema + a + c +b)
      //but expected output is (data schema + a + b + c)
      val pfileUtils = sparkAdapter.getSparkPartitionedFileUtils
      val partitionValues = getFixedPartitionValues(file.partitionValues, partitionSchema, fixedPartitionIndexes)
      val modifiedFile = pfileUtils.createPartitionedFile(partitionValues, pfileUtils.getPathFromPartitionedFile(file), file.start, file.length)
      val iter = parquetFileReader.read(modifiedFile, requestedSchema, remainingPartitionSchema, internalSchemaOpt, filters, storageConf)
      projectIter(iter, StructType(requestedSchema.fields ++ remainingPartitionSchema.fields), outputSchema)
    }
  }

  private def projectIter(iter: Iterator[Any], from: StructType, to: StructType): Iterator[InternalRow] = {
    val unsafeProjection = generateUnsafeProjection(from, to)
    val batchProjection = ColumnarBatchUtils.generateProjection(from, to)
    iter.map {
      case ir: InternalRow => unsafeProjection(ir)
      case cb: ColumnarBatch => batchProjection(cb)
    }.asInstanceOf[Iterator[InternalRow]]
  }

  private def getFixedPartitionValues(allPartitionValues: InternalRow, partitionSchema: StructType, fixedPartitionIndexes: Set[Int]): InternalRow = {
    InternalRow.fromSeq(allPartitionValues.toSeq(partitionSchema).zipWithIndex.filter(p => fixedPartitionIndexes.contains(p._2)).map(p => p._1))
  }
}
