package org.apache.spark.sql.structured.datasource

import org.apache.hudi.DataSourceReadOptions.{QUERY_TYPE_INCREMENTAL_OPT_VAL, QUERY_TYPE_OPT_KEY, QUERY_TYPE_SNAPSHOT_OPT_VAL}
import org.apache.hudi.IncrementalRelation
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.util
import org.apache.hudi.exception.HoodieException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame,SQLContext}
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}

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

    val path = parameters.get("path")
    if (parameters(QUERY_TYPE_OPT_KEY).equals(QUERY_TYPE_INCREMENTAL_OPT_VAL)) {
      val rdd = new IncrementalRelation(sqlContext, path.get, parameters, schema).buildScan()


      val value = rdd.map(i => InternalRow(
          schema.map(s=>{
//            UTF8String.fromString(i.getAs(s.name))
            i.getAs(s.name)
          })
        ))

      sqlContext.sparkSession.internalCreateDataFrame(value, schema, isStreaming = true).toDF()
    } else {
      throw new HoodieException("Invalid query type :" + parameters(QUERY_TYPE_OPT_KEY))
    }
//    sqlContext.sparkContext.hadoopConfiguration.setClass(
//      "mapreduce.input.pathFilter.class",
//      classOf[HoodieROTablePathFilter],
//      classOf[org.apache.hadoop.fs.PathFilter])
//    val newDataSource =
//    DataSource.apply(
//      sparkSession = sqlContext.sparkSession,
//      userSpecifiedSchema = Option(schema),
//      className = "parquet",
//      options = parameters)
//    Dataset.ofRows( sqlContext.sparkSession, LogicalRelation(newDataSource.resolveRelation(
//      checkFilesExist = false), isStreaming = true))





//    val path = parameters.get("path")
//    if (parameters(QUERY_TYPE_OPT_KEY).equals(QUERY_TYPE_SNAPSHOT_OPT_VAL)) {
//      sqlContext.sparkContext.hadoopConfiguration.setClass(
//        "mapreduce.input.pathFilter.class",
//        classOf[HoodieROTablePathFilter],
//        classOf[org.apache.hadoop.fs.PathFilter])
//      DataSource.apply(
//        sparkSession = sqlContext.sparkSession,
//        userSpecifiedSchema = Option(schema),
//        className = "parquet",
//        options = parameters)
//        .resolveRelation()
//    } else if (parameters(QUERY_TYPE_OPT_KEY).equals(QUERY_TYPE_INCREMENTAL_OPT_VAL)) {
//      new IncrementalRelation(sqlContext, path.get, parameters, schema)
//    } else {
//      throw new HoodieException("Invalid query type :" + parameters(QUERY_TYPE_OPT_KEY))
//    }

//    println("providerName============="+providerName)

//   val fakeSchema = StructType(StructField("b", IntegerType) :: Nil)
//    sqlContext.internalCreateDataFrame(
//      sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty"), fakeSchema,  true)
//    Dataset.ofRows(sqlContext.sparkSession, LogicalRelation(rdd, isStreaming = true))
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


