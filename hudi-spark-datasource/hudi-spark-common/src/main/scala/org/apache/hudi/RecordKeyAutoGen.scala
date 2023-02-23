package org.apache.hudi

import org.apache.hudi.common.config.HoodieConfig
import org.apache.hudi.common.model.{HoodiePayloadProps, HoodieRecord}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions.AutoRecordKeyGenExpression

import java.util.{Map => JMap}
import scala.collection.mutable
import scala.jdk.CollectionConverters.mapAsScalaMapConverter

object RecordKeyAutoGen {

  // TODO elaborate
  def tryRecordKeyAutoGen(df: DataFrame, commitInstant: String, config: HoodieConfig): DataFrame = {
    val shouldAutoGenRecordKeys = config.getBooleanOrDefault(HoodieTableConfig.AUTO_GEN_RECORD_KEYS)
    if (shouldAutoGenRecordKeys) {
      // TODO reorder to keep all meta-fields as first?
      df.withColumn(HoodieRecord.AUTOGEN_ROW_KEY, new Column(AutoRecordKeyGenExpression(commitInstant)))
    } else {
      df
    }
  }

  def handleAutoGenRecordKeysConfigJava(mergedParams: JMap[String, String]): Unit = {
    handleAutoGenRecordKeysConfig(mergedParams.asScala)
  }

  def handleAutoGenRecordKeysConfig(mergedParams: mutable.Map[String, String]): Unit = {
    val shouldAutoGenRecordKeys = mergedParams.getOrElse(HoodieTableConfig.AUTO_GEN_RECORD_KEYS.key,
      HoodieTableConfig.AUTO_GEN_RECORD_KEYS.defaultValue.toString).toBoolean
    if (shouldAutoGenRecordKeys) {
      // In case when keys will be auto-generated we have to override following configuration
      mergedParams ++=
        // TODO set payload disallowing merging
        Map(
          KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key -> HoodieRecord.AUTOGEN_ROW_KEY
        )

      // TODO operations (only insert, bulk-insert allowed)

      // TODO elaborate
      mergedParams --= Seq(
        HoodieWriteConfig.PRECOMBINE_FIELD_NAME.key,
        HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY
      )
    }
  }

}
