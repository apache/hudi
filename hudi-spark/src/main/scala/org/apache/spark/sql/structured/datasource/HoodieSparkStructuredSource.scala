package org.apache.spark.sql.structured.datasource

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.util
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}

/**
 * we need call internalCreateDataFrame()
 * sqlContext#private[sql] internalCreateDataFrame
 * so,we package as org.apache.spark.sql.structured.datasource.HoodieSparkStructuredSource
 *
 * @param sqlContext
 * @param metadataPath
 * @param schemaOpt
 * @param providerName
 * @param parameters
 */
class HoodieSparkStructuredSource (sqlContext: SQLContext,
                                   metadataPath: String,
                                   schemaOpt: Option[StructType],
                                   providerName: String,
                                   parameters: Map[String, String]) extends Source {

  override def schema: StructType = {
    schemaOpt.get
  }

  override def getOffset: scala.Option[HoodieStructuredSourceOffset] = {
    val path = parameters.get("path")
    val hoodieTableMetaClient = new HoodieTableMetaClient(sqlContext.sparkContext.hadoopConfiguration, path.get)
    val hoodieInstant: util.Option[HoodieInstant] = hoodieTableMetaClient.getCommitTimeline.lastInstant()
    var time: String = ""
    if (hoodieInstant.isPresent) {
      time = hoodieInstant.get().getTimestamp
    }
    val tableName = hoodieTableMetaClient.getTableConfig.getTableName
    val offsets = HoodieStructuredSourceOffset(Map[String, String] (tableName -> time))
    Option(offsets)
  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    //TODO
    /**
     * The next step
     * we will get hoodie data
     */
    sqlContext.internalCreateDataFrame(
      sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty"), schema,  true)
  }


  override def stop(): Unit = {
  }

  def offset2Map(offset: Offset): Map[String, String] = {
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    Serialization.read[Map[String, String]](offset.json())
  }
}
case class HoodieStructuredSourceOffset(offset: Map[String, String]) extends Offset {
  implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

  override def json(): String = Serialization.write(offset)
}


