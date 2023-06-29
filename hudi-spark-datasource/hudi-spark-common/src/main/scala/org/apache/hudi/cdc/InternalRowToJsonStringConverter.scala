package org.apache.hudi.cdc

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hudi.HoodieTableSchema
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

class InternalRowToJsonStringConverter(originTableSchema: HoodieTableSchema) {

  private lazy val mapper: ObjectMapper = {
    val _mapper = new ObjectMapper
    _mapper.setSerializationInclusion(Include.NON_ABSENT)
    _mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    _mapper.registerModule(DefaultScalaModule)
    _mapper
  }

  def convertRowToJsonString(record: InternalRow): UTF8String = {
    val map = scala.collection.mutable.Map.empty[String, Any]
    originTableSchema.structTypeSchema.zipWithIndex.foreach {
      case (field, idx) =>
        if (field.dataType.isInstanceOf[StringType]) {
          map(field.name) = Option(record.getUTF8String(idx)).map(_.toString).orNull
        } else {
          map(field.name) = record.get(idx, field.dataType)
        }
    }
    UTF8String.fromString(mapper.writeValueAsString(map))
  }
}
