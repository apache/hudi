package org.apache.hudi

import org.apache.avro.Schema
import org.apache.hudi.AvroConversionUtils.convertAvroSchemaToStructType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TestHoodieRelations {

  @Test
  def testPruningSchema(): Unit = {
    val avroSchemaString = "{\"type\":\"record\",\"name\":\"record\"," +
      "\"fields\":[{\"name\":\"_hoodie_commit_time\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null}," +
      "{\"name\":\"_hoodie_commit_seqno\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null}," +
      "{\"name\":\"_hoodie_record_key\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null}," +
      "{\"name\":\"_hoodie_partition_path\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null}," +
      "{\"name\":\"_hoodie_file_name\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null}," +
      "{\"name\":\"uuid\",\"type\":\"string\"},{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null}," +
      "{\"name\":\"age\",\"type\":[\"null\",\"int\"],\"default\":null}," +
      "{\"name\":\"ts\",\"type\":[\"null\",{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}],\"default\":null}," +
      "{\"name\":\"partition\",\"type\":[\"null\",\"string\"],\"default\":null}]}"

    val tableAvroSchema = new Schema.Parser().parse(avroSchemaString)
    val tableStructSchema = convertAvroSchemaToStructType(tableAvroSchema)

    val (requiredAvroSchema, requiredStructSchema, _) =
      HoodieBaseRelation.pruneSchema(tableAvroSchema, Array("ts"))

    assertEquals(Seq(tableAvroSchema.getField("ts")), requiredAvroSchema.getFields)
    assertEquals(
      Seq(tableStructSchema.fields.apply(tableStructSchema.fieldIndex("ts"))),
      requiredStructSchema.fields
    )
  }


}
