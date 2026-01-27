// ============================================================================
// SHELL 2 - Storage Lock Provider Test (S3 / GCS)
// ============================================================================

// CONFIGURATION - Must match Shell 1
val storageType = "gcs"  // Change to "gcs" for Google Cloud Storage
val bucketName = "lokesh-testing"  // CHANGE THIS to your bucket name
val basePath = "ljain"  // Optional prefix path

// Derived configuration
val tableName = "storage_concurrency_test"
val storagePrefix = if (storageType == "gcs") "gs" else "s3a"
val fullBasePath = s"${storagePrefix}://${bucketName}/${basePath}/${tableName}"
val parquetPath = s"${storagePrefix}://${bucketName}/${basePath}/${tableName}_parquet"

println(s"Using ${storageType.toUpperCase} storage")
println(s"Table path: ${fullBasePath}")

// IMPORTS
import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.common.model.HoodieRecord
import org.apache.spark.sql.functions.lit

// ============================================================================
// STEP 1: Try to update SAME partition (Should FAIL with write conflict)
// ============================================================================

println("Shell 2: Attempting to update San Francisco partition with driver-test2...")
println("This should FAIL because Shell 1 is updating the same partition")

val updatesSamePartition = (spark.read.parquet(parquetPath)
  .drop("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name")
  .where("partitionpath = 'americas/united_states/san_francisco'")
  .withColumn("driver", lit("driver-test2")))

try {
  updatesSamePartition.write.format("hudi")
    .options(getQuickstartWriteConfigs)
    .option(PRECOMBINE_FIELD_OPT_KEY, "ts")
    .option(RECORDKEY_FIELD_OPT_KEY, "uuid")
    .option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath")
    .option("hoodie.write.concurrency.mode", "optimistic_concurrency_control")
    .option("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.lock.StorageBasedLockProvider")
    //    .option("hoodie.write.lock.storage.validity.timeout.secs", "300")
    //    .option("hoodie.write.lock.storage.renew.interval.secs", "30")
    .option("hoodie.parquet.max.file.size", "2097100")
    .option(TABLE_NAME, tableName)
    .mode(Append)
    .save(fullBasePath)

  println("ERROR: Update succeeded when it should have failed!")
} catch {
  case e: org.apache.hudi.exception.HoodieWriteConflictException =>
    println("\n" + "=" * 80)
    println("SUCCESS! Got expected write conflict exception")
    println("=" * 80)
    println(s"Message: ${e.getMessage}")
  case e: Exception =>
    println(s"\nGot unexpected exception: ${e.getClass.getName}")
    println(s"Message: ${e.getMessage}")
}

// ============================================================================
// STEP 2: Update DIFFERENT partition (Should SUCCEED)
// ============================================================================

println("\n" + "=" * 80)
println("Shell 2: Now updating a DIFFERENT partition (Sao Paulo) with driver-test2...")
println("This should SUCCEED")
println("=" * 80)

val updatesDiffPartition = (spark.read.parquet(parquetPath)
  .drop("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name")
  .where("partitionpath = 'americas/brazil/sao_paulo'")
  .withColumn("driver", lit("driver-test2")))

updatesDiffPartition.write.format("hudi")
  .options(getQuickstartWriteConfigs)
  .option(PRECOMBINE_FIELD_OPT_KEY, "ts")
  .option(RECORDKEY_FIELD_OPT_KEY, "uuid")
  .option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath")
  .option("hoodie.write.concurrency.mode", "optimistic_concurrency_control")
  .option("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.lock.StorageBasedLockProvider")
  //  .option("hoodie.write.lock.storage.validity.timeout.secs", "300")
  //  .option("hoodie.write.lock.storage.renew.interval.secs", "30")
  .option("hoodie.parquet.max.file.size", "2097100")
  .option(TABLE_NAME, tableName)
  .mode(Append)
  .save(fullBasePath)

println("Shell 2: Update to different partition complete!")

// ============================================================================
// VERIFICATION
// ============================================================================

println("\n" + "=" * 80)
println("VERIFICATION: Checking final state")
println("=" * 80)

val finalData = spark.read.format("hudi").load(fullBasePath)

println("\nDriver distribution by partition:")
finalData.groupBy("partitionpath", "driver").count()
  .orderBy("partitionpath", "driver")
  .show(100, false)

println("\nExpected results:")
println("  ✓ San Francisco: all records should have driver='driver-test1'")
println("  ✓ Sao Paulo: all records should have driver='driver-test2'")
println("  ✓ Other partitions: original driver values")
