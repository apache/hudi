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

package org.apache.hudi.table.action.commit

import org.apache.hadoop.conf.Configuration
import org.apache.hudi.client.WriteStatus
import org.apache.hudi.client.utils.{SparkPartitionUtils, SparkValidatorUtils}
import org.apache.hudi.common.data.HoodieData
import org.apache.hudi.common.data.HoodieData.HoodieDataCacheKey
import org.apache.hudi.common.engine.HoodieEngineContext
import org.apache.hudi.common.model._
import org.apache.hudi.common.table.timeline.HoodieInstant.State
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils.serializeCommitMetadata
import org.apache.hudi.common.table.timeline.{HoodieActiveTimeline, HoodieInstant}
import org.apache.hudi.common.util.collection.Pair
import org.apache.hudi.common.util.{CommitUtils, Option}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.config.HoodieWriteConfig.WRITE_STATUS_STORAGE_LEVEL_VALUE
import org.apache.hudi.data.HoodieJavaDataFrame
import org.apache.hudi.exception.{HoodieCommitException, HoodieIOException, HoodieUpsertException}
import org.apache.hudi.execution.SparkLazyInsertIterable
import org.apache.hudi.hadoop.fs.HadoopFSUtils.getStorageConf
import org.apache.hudi.io.{CreateHandleFactory, HoodieMergeHandle, HoodieMergeHandleFactory}
import org.apache.hudi.keygen.BaseKeyGenerator
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory
import org.apache.hudi.table.action.HoodieWriteMetadata
import org.apache.hudi.table.{HoodieTable, WorkloadProfile, WorkloadStat}
import org.apache.spark.TaskContext
import org.apache.spark.sql.Encoder

import scala.jdk.CollectionConverters.{asJavaIteratorConverter, asScalaBufferConverter, asScalaIteratorConverter, setAsJavaSetConverter}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, Dataset, Encoders, Row}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.time.{Duration, Instant}
import java.util
import java.util.Collections


object BaseSparkDataFrameCommitActionExecutor {
  private val LOG = LoggerFactory.getLogger(classOf[BaseSparkDataFrameCommitActionExecutor[_]])
}

abstract class BaseSparkDataFrameCommitActionExecutor[T](context: HoodieEngineContext,
                                                config: HoodieWriteConfig,
                                                table: HoodieTable[T,HoodieData[HoodieRecord[T]], HoodieData[HoodieKey], HoodieData[WriteStatus]],
                                                instantTime: String,
                                                operationType: WriteOperationType,
                                                extraMetadata: Option[util.Map[String, String]])
  extends BaseSparkCommitActionExecutor[T](context, config, table, instantTime, operationType, extraMetadata) {

  //  override val keyGeneratorOpt: Option[BaseKeyGenerator] = if (config.populateMetaFields) {
  //      Option.empty[BaseKeyGenerator]()
  //    } else {
  //    try {
  //      Option.of(HoodieSparkKeyGeneratorFactory.createKeyGenerator(this.config.getProps).asInstanceOf[BaseKeyGenerator])
  //    } catch {
  //    case e: IOException =>
  //      throw new HoodieIOException("Only BaseKeyGenerators are supported when meta columns are disabled ", e)
  //    }
  //  }

//  def this(context: HoodieEngineContext, config: HoodieWriteConfig,
//           table: HoodieTable[T,HoodieData[HoodieRecord[T]], HoodieData[HoodieKey], HoodieData[WriteStatus]],
//           instantTime: String, operationType: WriteOperationType) {
//    this(context, config, table, instantTime, operationType, Option.empty.asInstanceOf[Option[util.Map[String, String]]])
//  }

  override def execute(inputRecords: HoodieData[HoodieRecord[T]]): HoodieWriteMetadata[HoodieData[WriteStatus]] = {
    // Cache the tagged records, so we don't end up computing both
    val inputDF = HoodieJavaDataFrame.getDataFrame(inputRecords)
    if (inputDF.storageLevel eq StorageLevel.NONE) HoodieJavaDataFrame.of(inputDF).persist(config.getTaggedRecordStorageLevel, context, HoodieDataCacheKey.of(config.getBasePath, instantTime))
    else BaseSparkDataFrameCommitActionExecutor.LOG.info("RDD PreppedRecords was persisted at: " + inputDF.storageLevel)
    // Handle records update with clustering
    val inputRecordsWithClusteringUpdate = clusteringHandleUpdate(inputDF)
    context.setJobStatus(this.getClass.getSimpleName, "Building workload profile:" + config.getTableName)
    val workloadProfile = new WorkloadProfile(buildProfile(inputRecordsWithClusteringUpdate), operationType, table.getIndex.canIndexLogFiles)
    BaseSparkDataFrameCommitActionExecutor.LOG.debug("Input workload profile :" + workloadProfile)
    // partition using the insert partitioner
    val partitioner: org.apache.spark.Partitioner = getPartitioner(workloadProfile)
    saveWorkloadProfileMetadataToInflight(workloadProfile, instantTime)
    context.setJobStatus(this.getClass.getSimpleName, "Doing partition and writing data: " + config.getTableName)
    val writeStatuses = mapPartitionsAsRDD(inputRecordsWithClusteringUpdate, partitioner)
    val result = new HoodieWriteMetadata[HoodieData[WriteStatus]]
    updateIndexAndCommitIfNeeded(writeStatuses, result)
    result
  }

  /**
   * Count the number of updates/inserts for each file in each partition.
   */
  private def buildProfile(inputRecords: Dataset[HoodieRecord[T]]) = {
    val sparkSession = inputRecords.sparkSession
    import sparkSession.implicits._
    val partitionPathStatMap = new util.HashMap[String, WorkloadStat]
    val globalStat = new WorkloadStat
    // group the records by partitionPath + currentLocation combination, count the number of
    // records in each partition
    // Map<Tuple2<String, Option<HoodieRecordLocation>>, Long>
    val partitionLocationCounts = new util.HashMap[Tuple2[String, Option[HoodieRecordLocation]], Long]
    val statsDataFrame = inputRecords
      .map(row => {
        Tuple2(row.getPartitionPath, row.getCurrentLocation)
      }).toDF("partitionPath", "recordLocation")
    statsDataFrame.show(false)
    statsDataFrame.printSchema()
    statsDataFrame.groupBy("partitionPath", "recordLocation").count.collectAsList.forEach((value: Row) => {
      val recordLocation = value.get(1).asInstanceOf[HoodieRecordLocation]
      val key = new Tuple2[String, Option[HoodieRecordLocation]](value.getString(0), Option.ofNullable(recordLocation))
      partitionLocationCounts.put(key, value.getLong(2))

    })
    // count the number of both inserts and updates in each partition, update the counts to workLoadStats
    import scala.collection.JavaConversions._
    for (e <- partitionLocationCounts.entrySet) {
      val partitionPath = e.getKey._1
      val count = e.getValue
      val locOption = e.getKey._2
      if (!partitionPathStatMap.containsKey(partitionPath)) partitionPathStatMap.put(partitionPath, new WorkloadStat)
      if (locOption.isPresent) {
        // update
        partitionPathStatMap.get(partitionPath).addUpdates(locOption.get, count)
        globalStat.addUpdates(locOption.get, count)
      }
      else {
        // insert
        partitionPathStatMap.get(partitionPath).addInserts(count)
        globalStat.addInserts(count)
      }
    }
    Pair.of(partitionPathStatMap, globalStat)
  }

//  override protected def getPartitioner(profile: WorkloadProfile): Partitioner = {
//    val layoutPartitionerClass: Option[String] = table.getStorageLayout.layoutPartitionerClass
//    if (layoutPartitionerClass.isPresent) {
//      getLayoutPartitioner(profile, layoutPartitionerClass.get)
//    } else if (WriteOperationType.isChangingRecords(operationType)) {
//      getUpsertPartitioner(profile)
//    } else {
//      getInsertPartitioner(profile)
//    }
//  }

  private def mapPartitionsAsRDD(dedupedRecords: Dataset[HoodieRecord[T]],
                                 partitioner: org.apache.spark.Partitioner): HoodieData[WriteStatus] = {
    val sparkSession = dedupedRecords.sparkSession
    import sparkSession.implicits._

    implicit val writeStatusEnc: Encoder[WriteStatus] = Encoders.kryo(classOf[WriteStatus])
    val partitionedKeys = dedupedRecords.map(row => {
        Tuple2(row.getKey.getRecordKey, partitioner.getPartition((row.getKey, Option.ofNullable(row.getCurrentLocation))))
      })

    var partitionedDF = dedupedRecords.joinWith(partitionedKeys, dedupedRecords("key") === partitionedKeys("_1"))
      .repartition(col("partitionKey"))
      .map(x => x._1)(Encoders.kryo)

    if (table.requireSortedRecords) {
      partitionedDF = partitionedDF.sortWithinPartitions(new Column("key.recordKey"))
    }

    val writeStatuses: Dataset[WriteStatus] = partitionedDF.mapPartitions((partition: Iterator[_]) => {
      val outputPartition = if (WriteOperationType.isChangingRecords(operationType)) {
        handleUpsertPartitionNew(instantTime, partition.asInstanceOf[Iterator[HoodieRecord[T]]], partitioner).asScala
      } else {
        handleInsertPartitionNew(instantTime, partition.asInstanceOf[Iterator[HoodieRecord[T]]], partitioner).asScala
      }
      outputPartition.flatMap(_.asScala.toList)
    })

    HoodieJavaDataFrame.of(writeStatuses)
  }

  private def clusteringHandleUpdate(inputRecords: Dataset[HoodieRecord[T]]): Dataset[HoodieRecord[T]] = {
    context.setJobStatus(this.getClass.getSimpleName, "Handling updates which are under clustering: " + config.getTableName)
    val fileGroupsInPendingClusteringPairs: Iterator[Pair[HoodieFileGroupId, HoodieInstant]] = table.getFileSystemView
      .getFileGroupsInPendingClustering.iterator().asScala

    val fileGroupsInPendingClustering: util.Set[HoodieFileGroupId] = fileGroupsInPendingClusteringPairs
      .map((x: Pair[HoodieFileGroupId, HoodieInstant]) => x.getKey).toSet.asJava
    // Skip processing if there is no inflight clustering
    if (fileGroupsInPendingClustering.isEmpty) return inputRecords
    // TODO: Need to implement clustering related logic if there is use case.
    //    UpdateStrategy<T, HoodieData<HoodieRecord<T>>> updateStrategy = (UpdateStrategy<T, HoodieData<HoodieRecord<T>>>) ReflectionUtils
    //        .loadClass(config.getClusteringUpdatesStrategyClass(), new Class<?>[] {HoodieEngineContext.class, HoodieTable.class, Set.class},
    //            this.context, table, fileGroupsInPendingClustering);
    //    // For SparkAllowUpdateStrategy with rollback pending clustering as false, need not handle
    //    // the file group intersection between current ingestion and pending clustering file groups.
    //    // This will be handled at the conflict resolution strategy.
    //    if (updateStrategy instanceof SparkAllowUpdateStrategy && !config.isRollbackPendingClustering()) {
    //      return inputRecords;
    //    }
    //    Pair<HoodieData<HoodieRecord<T>>, Set<HoodieFileGroupId>> recordsAndPendingClusteringFileGroups =
    //        updateStrategy.handleUpdate(inputRecords);
    //
    //    Set<HoodieFileGroupId> fileGroupsWithUpdatesAndPendingClustering = recordsAndPendingClusteringFileGroups.getRight();
    //    if (fileGroupsWithUpdatesAndPendingClustering.isEmpty()) {
    //      return recordsAndPendingClusteringFileGroups.getLeft();
    //    }
    //    // there are file groups pending clustering and receiving updates, so rollback the pending clustering instants
    //    // there could be race condition, for example, if the clustering completes after instants are fetched but before rollback completed
    //    if (config.isRollbackPendingClustering()) {
    //      Set<HoodieInstant> pendingClusteringInstantsToRollback = getAllFileGroupsInPendingClusteringPlans(table.getMetaClient()).entrySet().stream()
    //          .filter(e -> fileGroupsWithUpdatesAndPendingClustering.contains(e.getKey()))
    //          .map(Map.Entry::getValue)
    //          .collect(Collectors.toSet());
    //      pendingClusteringInstantsToRollback.forEach(instant -> {
    //        String commitTime = HoodieActiveTimeline.createNewInstantTime();
    //        table.scheduleRollback(context, commitTime, instant, false, config.shouldRollbackUsingMarkers(), false);
    //        table.rollback(context, commitTime, instant, true, true);
    //      });
    //      table.getMetaClient().reloadActiveTimeline();
    //    }
    //    return recordsAndPendingClusteringFileGroups.getLeft();
    inputRecords
  }

  override protected def updateIndex(writeStatuses: HoodieData[WriteStatus], result: HoodieWriteMetadata[HoodieData[WriteStatus]]): HoodieData[WriteStatus] = {
    // cache writeStatusRDD before updating index, so that all actions before this are not triggered again for future
    // RDD actions that are performed after updating the index.
    writeStatuses.persist(config.getString(WRITE_STATUS_STORAGE_LEVEL_VALUE), context, HoodieDataCacheKey.of(config.getBasePath, instantTime))
    val indexStartTime = Instant.now
    // Update the index back
    val statuses = table.getIndex.updateLocation(writeStatuses, context, table, instantTime)
    result.setIndexUpdateDuration(Duration.between(indexStartTime, Instant.now))
    result.setWriteStatuses(statuses)
    statuses
  }

  override protected def updateIndexAndCommitIfNeeded(writeStatusRDD: HoodieData[WriteStatus], result: HoodieWriteMetadata[HoodieData[WriteStatus]]): Unit = {
    updateIndex(writeStatusRDD, result)
    result.setPartitionToReplaceFileIds(getPartitionToReplacedFileIds(result))
    commitOnAutoCommit(result)
  }

  override protected def getCommitActionType: String = table.getMetaClient.getCommitActionType

  override protected def setCommitMetadata(result: HoodieWriteMetadata[HoodieData[WriteStatus]]): Unit = {
    val writeStats = result.getWriteStatuses.map(x => x.getStat).collectAsList
    result.setWriteStats(writeStats)
    result.setCommitMetadata(Option.of(CommitUtils.buildMetadata(writeStats, result.getPartitionToReplaceFileIds, extraMetadata, operationType, getSchemaToStoreInCommit, getCommitActionType)))
  }

  protected def commit(extraMetadata: Option[util.Map[String, String]], result: HoodieWriteMetadata[HoodieData[WriteStatus]]): Unit = {
    context.setJobStatus(this.getClass.getSimpleName, "Commit write status collect: " + config.getTableName)
    val actionType = getCommitActionType
    BaseSparkDataFrameCommitActionExecutor.LOG.info("Committing " + instantTime + ", action Type " + actionType + ", operation Type " + operationType)
    result.setCommitted(true)
    if (!result.getWriteStats.isPresent) result.setWriteStats(result.getWriteStatuses.map(x => x.getStat).collectAsList)
    // Finalize write
    finalizeWrite(instantTime, result.getWriteStats.get, result)
    try {
      val activeTimeline: HoodieActiveTimeline = table.getActiveTimeline
      val metadata = result.getCommitMetadata.get
      writeTableMetadata(metadata, actionType)
      activeTimeline.saveAsComplete(table.getMetaClient.createNewInstant(State.INFLIGHT, actionType, instantTime),
        serializeCommitMetadata(table.getMetaClient.getCommitMetadataSerDe, metadata))
      BaseSparkDataFrameCommitActionExecutor.LOG.info("Committed " + instantTime)
      result.setCommitMetadata(Option.of(metadata))
    } catch {
      case e: IOException =>
        throw new HoodieCommitException("Failed to complete commit " + config.getBasePath + " at time " + instantTime, e)
    }
  }

  override protected def getPartitionToReplacedFileIds(writeStatuses: HoodieWriteMetadata[HoodieData[WriteStatus]]): util.Map[String, util.List[String]] = {
    Collections.emptyMap[String, util.List[String]]()
  }

  def handleUpsertPartitionNew(instantTime: String, recordItr: Iterator[HoodieRecord[T]],
                               partitioner: org.apache.spark.Partitioner): util.Iterator[util.List[WriteStatus]] = {
    val partitionId = TaskContext.getPartitionId
    val upsertPartitioner = partitioner.asInstanceOf[SparkHoodiePartitioner[_]]
    val binfo = upsertPartitioner.getBucketInfo(partitionId)
    val btype = binfo.bucketType
    try {
      if (btype == BucketType.INSERT) {
        handleInsert(binfo.fileIdPrefix, recordItr.asJava)
      } else if (btype == BucketType.UPDATE) {
        handleUpdate(binfo.partitionPath, binfo.fileIdPrefix, recordItr.asJava)
      } else {
        throw new HoodieUpsertException("Unknown bucketType " + btype + " for partition :" + partitionId)
      }
    }
    catch {
      case t: Throwable =>
        val msg = "Error upserting bucketType " + btype + " for partition :" + partitionId
        BaseSparkDataFrameCommitActionExecutor.LOG.error(msg, t)
        throw new HoodieUpsertException(msg, t)
    }
  }

  protected def handleInsertPartitionNew(instantTime: String, recordItr: Iterator[HoodieRecord[T]],
                                      partitioner: org.apache.spark.Partitioner): util.Iterator[util.List[WriteStatus]] = {
    handleUpsertPartitionNew(instantTime, recordItr, partitioner)
  }

  //  @throws[IOException]
  //  override def handleUpdate(partitionPath: String, fileId: String, recordItr: util.Iterator[HoodieRecord[T]]): util.Iterator[util.List[WriteStatus]] = {
  //    // This is needed since sometimes some buckets are never picked in getPartition() and end up with 0 records
  //    if (!recordItr.hasNext) {
  //      BaseSparkDataFrameCommitActionExecutor.LOG.info("Empty partition with fileId => " + fileId)
  //      return Collections.emptyIterator
  //    }
  //    // Pre-check: if the old file does not exist (which may happen in bucket index case), fallback to insert
  //    if (!(table.getBaseFileOnlyView.getLatestBaseFile(partitionPath, fileId).isPresent)
  //      && HoodieIndex.IndexType.BUCKET == config.getIndexType) {
  //      return handleInsert(fileId, recordItr)
  //    }
  //    // these are updates
  //    val upsertHandle = getUpdateHandle(partitionPath, fileId, recordItr)
  //    handleUpdateInternal(upsertHandle, fileId)
  //  }

  @throws[IOException]
  override protected def handleUpdateInternal(upsertHandle: HoodieMergeHandle[_, _, _, _], fileId: String): util.Iterator[util.List[WriteStatus]] = {
    if (upsertHandle.getOldFilePath == null) throw new HoodieUpsertException("Error in finding the old file path at commit " + instantTime + " for fileId: " + fileId)
    else {
      if (upsertHandle.baseFileForMerge.getBootstrapBaseFile.isPresent) {
        val partitionFields = table.getMetaClient.getTableConfig.getPartitionFields
        val partitionValues = SparkPartitionUtils.getPartitionFieldVals(partitionFields, upsertHandle.getPartitionPath, table.getMetaClient.getTableConfig.getBootstrapBasePath.get, upsertHandle.getWriterSchema, getStorageConf.unwrapAs(classOf[Configuration]))
        upsertHandle.setPartitionFields(partitionFields)
        upsertHandle.setPartitionValues(partitionValues)
      }
      HoodieMergeHelper.newInstance.runMerge(table, upsertHandle)
    }
    // TODO(vc): This needs to be revisited
    if (upsertHandle.getPartitionPath == null) BaseSparkDataFrameCommitActionExecutor.LOG.info("Upsert Handle has partition path as null " + upsertHandle.getOldFilePath + ", " + upsertHandle.getWriteStatusesAsIterator)
    upsertHandle.getWriteStatusesAsIterator
  }

  override protected def getUpdateHandle(partitionPath: String, fileId: String, recordItr: util.Iterator[HoodieRecord[T]]): HoodieMergeHandle[_, _, _, _] = HoodieMergeHandleFactory.create(operationType, config, instantTime, table, recordItr, partitionPath, fileId, taskContextSupplier, keyGeneratorOpt)

  override def handleInsert(idPfx: String, recordItr: util.Iterator[HoodieRecord[T]]): util.Iterator[util.List[WriteStatus]] = {
    // This is needed since sometimes some buckets are never picked in getPartition() and end up with 0 records
    if (!recordItr.hasNext) {
      BaseSparkDataFrameCommitActionExecutor.LOG.info("Empty partition")
      return Collections.emptyIterator[util.List[WriteStatus]]()
    }
    new SparkLazyInsertIterable[T](recordItr, true, config, instantTime, table, idPfx, taskContextSupplier, new CreateHandleFactory[AnyRef, AnyRef, AnyRef, AnyRef])
  }

  override protected def runPrecommitValidators(writeMetadata: HoodieWriteMetadata[HoodieData[WriteStatus]]): Unit = {
    SparkValidatorUtils.runValidators(config, writeMetadata, context, table, instantTime)
  }
}
