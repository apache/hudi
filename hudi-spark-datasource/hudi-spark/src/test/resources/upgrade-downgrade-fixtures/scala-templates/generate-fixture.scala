// Initialize Spark imports and session
import org.apache.spark.sql.SaveMode
import org.apache.hudi.DataSourceWriteOptions._
import spark.implicits._

val tableName = "${TABLE_NAME}"
val basePath = "${BASE_PATH}"

// Create simple test data with consistent schema
val testData = Seq(
  ("id1", "Alice", 1000L, "2023-01-01"),
  ("id2", "Bob", 2000L, "2023-01-01"),  
  ("id3", "Charlie", 3000L, "2023-01-02"),
  ("id4", "David", 4000L, "2023-01-02"),
  ("id5", "Eve", 5000L, "2023-01-03")
)

val df = testData.toDF("id", "name", "ts", "partition")

// Write initial batch (creates base files)
df.write.format("hudi").
  option(PRECOMBINE_FIELD.key, "ts").
  option(RECORDKEY_FIELD.key, "id").
  option(PARTITIONPATH_FIELD.key, "partition").
  option("hoodie.table.name", tableName).
  option("hoodie.datasource.write.table.type", "MERGE_ON_READ").
  option("hoodie.datasource.write.operation", "insert").
  mode(SaveMode.Overwrite).
  save(basePath)

println("Initial batch written")

// Write update batch (creates log files)
val updateData = Seq(
  ("id1", "Alice_Updated", 1001L, "2023-01-01"),
  ("id2", "Bob_Updated", 2001L, "2023-01-01"),
  ("id6", "Frank", 6000L, "2023-01-03")
)

val updateDf = updateData.toDF("id", "name", "ts", "partition")

updateDf.write.format("hudi").
  option(PRECOMBINE_FIELD.key, "ts").
  option(RECORDKEY_FIELD.key, "id").
  option(PARTITIONPATH_FIELD.key, "partition").
  option("hoodie.table.name", tableName).
  option("hoodie.datasource.write.table.type", "MERGE_ON_READ").
  option("hoodie.datasource.write.operation", "upsert").
  mode(SaveMode.Append).
  save(basePath)

println("Update batch written")

// Create one more insert to have multiple commits
val insertData = Seq(
  ("id7", "Grace", 7000L, "2023-01-04"),
  ("id8", "Henry", 8000L, "2023-01-04")
)

val insertDf = insertData.toDF("id", "name", "ts", "partition")

insertDf.write.format("hudi").
  option(PRECOMBINE_FIELD.key, "ts").
  option(RECORDKEY_FIELD.key, "id").
  option(PARTITIONPATH_FIELD.key, "partition").
  option("hoodie.table.name", tableName).
  option("hoodie.datasource.write.table.type", "MERGE_ON_READ").
  option("hoodie.datasource.write.operation", "insert").
  mode(SaveMode.Append).
  save(basePath)

println("Additional insert written")
println(s"Fixture ${FIXTURE_NAME} generated successfully!")

System.exit(0)