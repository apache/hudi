package org.apache.hudi.functional

import org.apache.hudi.DataSourceWriteOptions
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Tag
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

@Tag("functional")
class TestRecordLevelIndexWithSQL extends RecordLevelIndexTestBase {
  val sqlTempTable = "tbl"

  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE"))
  def testRLIWithSQL(tableType: String): Unit = {
    var hudiOpts = commonOpts
    hudiOpts = hudiOpts + (DataSourceWriteOptions.TABLE_TYPE.key -> tableType)

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      validate = false)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      validate = false)

    createTempTable(hudiOpts)
    val reckey = mergedDfList.last.limit(1).collect()(0).getAs("_row_key").toString
    spark.sql("select * from " + sqlTempTable + " where '" + reckey + "' = _row_key").show(false)
  }

  private def createTempTable(hudiOpts: Map[String, String]): Unit = {
    val readDf = spark.read.format("hudi").options(hudiOpts).load(basePath)
    readDf.registerTempTable(sqlTempTable)
  }
}
