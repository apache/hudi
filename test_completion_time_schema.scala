import org.apache.hudi.HoodieHadoopFsRelationFactory._
import org.apache.hudi.common.model.HoodieRecord
import org.apache.spark.sql.types._

// Test the schema extension logic
val baseSchema = StructType(Array(
  StructField("_hoodie_commit_time", StringType, nullable = true),
  StructField("_row_key", StringType, nullable = false),
  StructField("name", StringType, nullable = true)
))

println("Base schema fields:")
baseSchema.fieldNames.foreach(println)

// Test the extendSchemaForCompletionTime logic
val fields = baseSchema.fields.toBuffer

if (!fields.exists(_.name == HoodieRecord.COMMIT_TIME_METADATA_FIELD)) {
  val commitTimeField = StructField(HoodieRecord.COMMIT_TIME_METADATA_FIELD, StringType, nullable = true)
  fields += commitTimeField
}

if (!fields.exists(_.name == HoodieRecord.COMMIT_COMPLETION_TIME_METADATA_FIELD)) {
  val completionTimeField = StructField(HoodieRecord.COMMIT_COMPLETION_TIME_METADATA_FIELD, StringType, nullable = true)
  fields += completionTimeField
}

val extendedSchema = StructType(fields.toArray)

println("\nExtended schema fields:")
extendedSchema.fieldNames.foreach(println)

println(s"\nCOMMIT_COMPLETION_TIME_METADATA_FIELD = ${HoodieRecord.COMMIT_COMPLETION_TIME_METADATA_FIELD}")
