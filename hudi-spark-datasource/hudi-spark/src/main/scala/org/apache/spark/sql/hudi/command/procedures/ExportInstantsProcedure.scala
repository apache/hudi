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
import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.avro.model.HoodieArchivedMetaEntry
import org.apache.hudi.common.model.HoodieLogFile
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.log.HoodieLogFormat
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToStoragePath
import org.apache.hudi.storage.{HoodieStorage, HoodieStorageUtils, StoragePath}

import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificData
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}

import java.io.File
import java.util
import java.util.Collections
import java.util.function.Supplier

import scala.collection.JavaConverters._
import scala.util.control.Breaks.break

class ExportInstantsProcedure extends BaseProcedure with ProcedureBuilder with Logging {
  var sortByFieldParameter: ProcedureParameter = _

  val defaultActions = "clean,commit,deltacommit,rollback,savepoint,restore"

  private val PARAMETERS = Array[ProcedureParameter](
    ProcedureParameter.required(0, "table", DataTypes.StringType),
    ProcedureParameter.required(1, "local_folder", DataTypes.StringType),
    ProcedureParameter.optional(2, "limit", DataTypes.IntegerType, -1),
    ProcedureParameter.optional(3, "actions", DataTypes.StringType, defaultActions),
    ProcedureParameter.optional(4, "desc", DataTypes.BooleanType, false)
  )

  private val OUTPUT_TYPE = new StructType(Array[StructField](
    StructField("export_detail", DataTypes.StringType, nullable = true, Metadata.empty)
  ))

  def parameters: Array[ProcedureParameter] = PARAMETERS

  def outputType: StructType = OUTPUT_TYPE

  override def call(args: ProcedureArgs): Seq[Row] = {
    super.checkArgs(PARAMETERS, args)

    val table = getArgValueOrDefault(args, PARAMETERS(0)).get.asInstanceOf[String]
    val localFolder = getArgValueOrDefault(args, PARAMETERS(1)).get.asInstanceOf[String]
    val limit = getArgValueOrDefault(args, PARAMETERS(2)).get.asInstanceOf[Int]
    val actions: String = getArgValueOrDefault(args, PARAMETERS(3)).get.asInstanceOf[String]
    val desc = getArgValueOrDefault(args, PARAMETERS(4)).get.asInstanceOf[Boolean]

    val hoodieCatalogTable = HoodieCLIUtils.getHoodieCatalogTable(sparkSession, table)
    val basePath = hoodieCatalogTable.tableLocation
    val metaClient = createMetaClient(jsc, basePath)
    val archivePath = new Path(basePath + "/.hoodie/.commits_.archive*")
    val actionSet: util.Set[String] = Set(actions.split(","): _*).asJava
    val numExports = if (limit == -1) Integer.MAX_VALUE else limit
    var numCopied = 0

    if (!new File(localFolder).isDirectory) throw new HoodieException(localFolder + " is not a valid local directory")

    // The non archived instants can be listed from the Timeline.
    val nonArchivedInstants: util.List[HoodieInstant] = metaClient
      .getActiveTimeline
      .filterCompletedInstants.getInstants.iterator().asScala
      .filter((i: HoodieInstant) => actionSet.contains(i.getAction))
      .toList.asJava

    // Archived instants are in the commit archive files
    val statuses: Array[FileStatus] = HadoopFSUtils.getFs(basePath, jsc.hadoopConfiguration()).globStatus(archivePath)
    val archivedStatuses = List(statuses: _*)
      .sortWith((f1, f2) => (f1.getModificationTime - f2.getModificationTime).toInt > 0).asJava

    if (desc) {
      Collections.reverse(nonArchivedInstants)
      numCopied = copyNonArchivedInstants(metaClient, nonArchivedInstants, numExports, localFolder)
      if (numCopied < numExports) {
        Collections.reverse(archivedStatuses)
        numCopied += copyArchivedInstants(basePath, archivedStatuses, actionSet, numExports - numCopied, localFolder)
      }
    } else {
      numCopied = copyArchivedInstants(basePath, archivedStatuses, actionSet, numExports, localFolder)
      if (numCopied < numExports) numCopied += copyNonArchivedInstants(metaClient, nonArchivedInstants, numExports - numCopied, localFolder)
    }

    Seq(Row("Exported " + numCopied + " Instants to " + localFolder))
  }

  @throws[Exception]
  private def copyArchivedInstants(basePath: String, statuses: util.List[FileStatus], actionSet: util.Set[String], limit: Int, localFolder: String) = {
    var copyCount = 0
    val storage = HoodieStorageUtils.getStorage(basePath, HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()))
    for (fs <- statuses.asScala) {
      // read the archived file
      val reader = HoodieLogFormat.newReader(
        storage, new HoodieLogFile(convertToStoragePath(fs.getPath)), HoodieSchema.fromAvroSchema(HoodieArchivedMetaEntry.getClassSchema))
      // read the avro blocks
      while ( {
        reader.hasNext && copyCount < limit
      }) {
        val blk = reader.next.asInstanceOf[HoodieAvroDataBlock]
        val recordItr = blk.getRecordIterator(HoodieRecordType.AVRO)
        try while ( {
          recordItr.hasNext
        }) {
          val ir = recordItr.next
          // Archived instants are saved as arvo encoded HoodieArchivedMetaEntry records. We need to get the
          // metadata record from the entry and convert it to json.
          val archiveEntryRecord = SpecificData.get.deepCopy(HoodieArchivedMetaEntry.SCHEMA$, ir).asInstanceOf[HoodieArchivedMetaEntry]
          val action = archiveEntryRecord.get("actionType").toString
          if (!actionSet.contains(action)) break() //todo: continue is not supported
          val metadata: GenericRecord = action match {
            case HoodieTimeline.CLEAN_ACTION =>
              archiveEntryRecord.getHoodieCleanMetadata

            case HoodieTimeline.COMMIT_ACTION =>
              archiveEntryRecord.getHoodieCommitMetadata

            case HoodieTimeline.DELTA_COMMIT_ACTION =>
              archiveEntryRecord.getHoodieCommitMetadata

            case HoodieTimeline.ROLLBACK_ACTION =>
              archiveEntryRecord.getHoodieRollbackMetadata

            case HoodieTimeline.SAVEPOINT_ACTION =>
              archiveEntryRecord.getHoodieSavePointMetadata

            case HoodieTimeline.COMPACTION_ACTION =>
              archiveEntryRecord.getHoodieCompactionMetadata

            case _ => logInfo("Unknown type of action " + action)
              null
          }
          val instantTime = archiveEntryRecord.get("commitTime").toString
          val outPath = localFolder + StoragePath.SEPARATOR + instantTime + "." + action
          if (metadata != null) writeToFile(storage, outPath, HoodieAvroUtils.avroToJson(metadata, true))
          if ( {
            copyCount += 1;
            copyCount
          } == limit) break //todo: break is not supported
        }
        finally if (recordItr != null) recordItr.close()
      }
      reader.close()
    }
    copyCount
  }

  @throws[Exception]
  private def copyNonArchivedInstants(metaClient: HoodieTableMetaClient, instants: util.List[HoodieInstant], limit: Int, localFolder: String): Int = {
    var copyCount = 0
    if (!instants.isEmpty) {
      val timeline = metaClient.getActiveTimeline
      val storage = HoodieStorageUtils.getStorage(metaClient.getBasePath, HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()))
      val instantFileNameGenerator = metaClient.getTimelineLayout.getInstantFileNameGenerator
      for (instant <- instants.asScala) {
        val localPath = localFolder + StoragePath.SEPARATOR + instantFileNameGenerator.getFileName(instant)
        val data: Array[Byte] = instant.getAction match {
          case HoodieTimeline.CLEAN_ACTION =>
            val metadata = timeline.readCleanMetadata(instant)
            HoodieAvroUtils.avroToJson(metadata, true)

          case HoodieTimeline.DELTA_COMMIT_ACTION =>
            // Already in json format
            timeline.getInstantDetails(instant).get

          case HoodieTimeline.COMMIT_ACTION =>
            // Already in json format
            timeline.getInstantDetails(instant).get

          case HoodieTimeline.COMPACTION_ACTION =>
            // Already in json format
            timeline.getInstantDetails(instant).get

          case HoodieTimeline.ROLLBACK_ACTION =>
            val metadata = timeline.readRollbackMetadata(instant)
            HoodieAvroUtils.avroToJson(metadata, true)

          case HoodieTimeline.SAVEPOINT_ACTION =>
            val metadata = timeline.readSavepointMetadata(instant)
            HoodieAvroUtils.avroToJson(metadata, true)

          case _ => null

        }
        if (data != null) {
          writeToFile(storage, localPath, data)
          copyCount = copyCount + 1
        }
      }
    }
    copyCount
  }

  @throws[Exception]
  private def writeToFile(storage: HoodieStorage, path: String, data: Array[Byte]): Unit = {
    val out = storage.create(new StoragePath(path))
    out.write(data)
    out.flush()
    out.close()
  }

  override def build = new ExportInstantsProcedure()
}

object ExportInstantsProcedure {
  val NAME = "export_instants"

  def builder: Supplier[ProcedureBuilder] = new Supplier[ProcedureBuilder] {
    override def get() = new ExportInstantsProcedure()
  }
}
