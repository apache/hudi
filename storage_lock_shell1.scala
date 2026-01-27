// ============================================================================
// SHELL 1 - Storage Lock Provider Test (S3 / GCS)
// ============================================================================

// CONFIGURATION - Choose your storage type
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
// STEP 1: Initial Ingestion (Run this FIRST in Shell 1 only)
// ============================================================================

println("Shell 1: Starting initial ingestion...")

val dataGen = new DataGenerator
val inserts = convertToStringList(dataGen.generateInserts(10000))
val df = spark.read.json(spark.sparkContext.parallelize(inserts, 20))

df.write.format("hudi")
  .options(getQuickstartWriteConfigs)
  .option(PRECOMBINE_FIELD_OPT_KEY, "ts")
  .option(RECORDKEY_FIELD_OPT_KEY, "uuid")
  .option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath")
  .option("hoodie.write.concurrency.mode", "optimistic_concurrency_control")
  .option("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.lock.StorageBasedLockProvider")
  .option("hoodie.write.lock.storage.renew.interval.secs", "30")
  .option("hoodie.cleaner.policy.failed.writes", "LAZY")
  .option("hoodie.parquet.max.file.size", "2097100")
  .option(TABLE_NAME, tableName)
  .mode(Overwrite)
  .save(fullBasePath)

println("Shell 1: Initial ingestion complete!")

// Export to parquet for easier reading in updates
spark.read.format("hudi").load(fullBasePath)
  .write.mode("overwrite").parquet(parquetPath)

println("Shell 1: Exported to parquet for updates")

// ============================================================================
// STEP 2: Update San Francisco partition (Run this SECOND)
// ============================================================================

println("\nShell 1: Updating San Francisco partition with driver-test1...")

val updates = (spark.read.parquet(parquetPath)
  .drop("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name")
  .where("partitionpath = 'americas/united_states/san_francisco'")
  .withColumn("driver", lit("driver-test1")))

updates.write.format("hudi")
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

println("Shell 1: Update complete!")
