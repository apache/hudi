package org.apache.spark.sql.hudi

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

import scala.collection.immutable.HashMap

class HoodieOptionConfigTest {
  private val PRIMARY_KEY = "primaryKey"
  private val TYPE = "type"
  private val INDEX_TYPE = "index.type"
  private val INDEX_TYPE_BUCKET_NUMS = "hoodie.bucket.index.num.buckets"

  private val TRANSLATED_PRIMARY_KEY = "hoodie.datasource.write.recordkey.field"
  private val TRANSLATED_TYPE = "hoodie.datasource.write.table.type"
  private val TRANSLATED_INDEX_TYPE = "hoodie.index.type"


  @Test
  def testMapSqlOptionsToDataSourceWriteConfigs(): Unit = {
    val mapSqlOptions: Map[String, String] =  Map(
      PRIMARY_KEY -> "id",
      TYPE -> "cow",
      INDEX_TYPE -> "BUCKET",
      INDEX_TYPE_BUCKET_NUMS -> "4"
    )

    val options = HashMap(mapSqlOptions.toSeq: _*)
    val translatedOptions = HoodieOptionConfig.mapSqlOptionsToDataSourceWriteConfigs(options)
    compareOptions(mapSqlOptions, translatedOptions)
  }


  private def compareOptions(options: Map[String, String], translatedOptions: Map[String, String]): Unit = {
    assertEquals(options.size, translatedOptions.size)
    options.foreach(kv => {
      val k = kv._1
      val translatedK = confComparisonExpectedMap.getOrElse(k, k)

      assertTrue(translatedOptions.contains(translatedK))
    })
  }

  private val confComparisonExpectedMap: Map[String, String] = Map(
    PRIMARY_KEY -> TRANSLATED_PRIMARY_KEY,
    TYPE -> TRANSLATED_TYPE,
    INDEX_TYPE -> TRANSLATED_INDEX_TYPE,
    INDEX_TYPE_BUCKET_NUMS -> INDEX_TYPE_BUCKET_NUMS
  )

}
