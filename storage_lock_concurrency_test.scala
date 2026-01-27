// Storage-based Lock Provider Concurrency Test (S3 / GCS)
// This script demonstrates concurrent write operations using storage-based lock provider
//
// Prerequisites:
// 1. Cloud storage bucket configured and accessible (S3 or GCS)
// 2. Credentials configured:
//    - S3: AWS credentials (environment variables, ~/.aws/credentials, or IAM role)
//    - GCS: GCP credentials (GOOGLE_APPLICATION_CREDENTIALS or ADC)
// 3. Two spark-shell sessions to simulate concurrent writes
//
// Usage:
// Session 1: Run the initial ingestion and first update
// Session 2: Run concurrent updates on same/different partitions

// ============================================================================
// CONFIGURATION - Update these values for your environment
// ============================================================================

val storageType = "s3"  // Change to "gcs" for Google Cloud Storage
val bucketName = "your-bucket-name"  // CHANGE THIS to your bucket name
val basePath = "hudi"  // Optional prefix path

// Derived configuration
val tableName = "storage_concurrency_test"
val storagePrefix = if (storageType == "gcs") "gs" else "s3a"
val fullBasePath = s"${storagePrefix}://${bucketName}/${basePath}/${tableName}"
val parquetPath = s"${storagePrefix}://${bucketName}/${basePath}/${tableName}_parquet"

println(s"=" * 80)
println(s"Storage Type: ${storageType.toUpperCase}")
println(s"Bucket: ${bucketName}")
println(s"Table Path: ${fullBasePath}")
println(s"=" * 80)

// ============================================================================
// IMPORTS
// ============================================================================

import org.apache.hudi.QuickstartUtils._
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.common.model.HoodieRecord
import org.apache.spark.sql.functions.lit

// ============================================================================
// HELPER FUNCTION - Common write options for storage-based lock provider
// ============================================================================

def getStorageLockWriteConfigs(): Map[String, String] = {
  val baseConfigs = getQuickstartWriteConfigs.toMap

  baseConfigs ++ Map(
    // Basic table configuration
    PRECOMBINE_FIELD_OPT_KEY.key() -> "ts",
    RECORDKEY_FIELD_OPT_KEY.key() -> "uuid",
    PARTITIONPATH_FIELD_OPT_KEY.key() -> "partitionpath",
    TABLE_NAME.key() -> tableName,

    // Enable optimistic concurrency control
    "hoodie.write.concurrency.mode" -> "optimistic_concurrency_control",

    // Storage-based lock provider configuration
    "hoodie.write.lock.provider" -> "org.apache.hudi.client.transaction.lock.StorageBasedLockProvider",

    // Lock validity and renewal settings
    "hoodie.write.lock.storage.validity.timeout.secs" -> "300",  // 5 minutes
    "hoodie.write.lock.storage.renew.interval.secs" -> "30",     // Renew every 30 seconds

    // File size configuration for testing
    "hoodie.parquet.max.file.size" -> "2097100",

    // Cleaner policy for failed writes
    "hoodie.cleaner.policy.failed.writes" -> "LAZY"
  )
}

// ============================================================================
// STEP 1: INITIAL INGESTION (Run in ONE session first)
// ============================================================================

println("=" * 80)
println("STEP 1: Initial Data Generation and Ingestion")
println("=" * 80)

val dataGen = new DataGenerator

val inserts = convertToStringList(dataGen.generateInserts(10000))
val df = spark.read.json(spark.sparkContext.parallelize(inserts, 20))

println(s"Generated ${inserts.size()} records for initial ingestion")
println(s"Writing to: ${fullBasePath}")

df.write.format("hudi")
  .options(getStorageLockWriteConfigs())
  .mode(Overwrite)
  .save(fullBasePath)

println("Initial ingestion complete!")
println("=" * 80)

// Export to parquet for easier reading in updates
spark.read.format("hudi").load(fullBasePath)
  .write.mode("overwrite").parquet(parquetPath)

println("Exported to parquet for updates")

// ============================================================================
// STEP 2a: SHELL 1 - Update San Francisco partition with driver-test1
// ============================================================================

println("\n" + "=" * 80)
println("SHELL 1: Updating San Francisco partition with driver-test1")
println("=" * 80)

val updates1 = spark.read.parquet(parquetPath)
  .drop("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name")
  .where("partitionpath = 'americas/united_states/san_francisco'")
  .withColumn("driver", lit("driver-test1"))

println(s"Updating ${updates1.count()} records in San Francisco partition")

updates1.write.format("hudi")
  .options(getStorageLockWriteConfigs())
  .mode(Append)
  .save(fullBasePath)

println("Shell 1 update complete!")
println("=" * 80)

// ============================================================================
// STEP 2b: SHELL 2 - Concurrent update to SAME partition (should fail)
// ============================================================================

println("\n" + "=" * 80)
println("SHELL 2: Attempting concurrent update to SAME partition (San Francisco)")
println("This should FAIL with HoodieWriteConflictException")
println("=" * 80)

val updates2SamePartition = spark.read.parquet(parquetPath)
  .drop("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name")
  .where("partitionpath = 'americas/united_states/san_francisco'")
  .withColumn("driver", lit("driver-test2"))

println(s"Attempting to update ${updates2SamePartition.count()} records in San Francisco partition")

try {
  updates2SamePartition.write.format("hudi")
    .options(getStorageLockWriteConfigs())
    .mode(Append)
    .save(fullBasePath)

  println("ERROR: Update succeeded when it should have failed!")
} catch {
  case e: org.apache.hudi.exception.HoodieWriteConflictException =>
    println("SUCCESS: Got expected write conflict exception!")
    println(s"Exception message: ${e.getMessage}")
  case e: Exception =>
    println(s"Got exception: ${e.getClass.getName}")
    println(s"Message: ${e.getMessage}")
    throw e
}

println("=" * 80)

// ============================================================================
// STEP 3: SHELL 2 - Update DIFFERENT partition (should succeed)
// ============================================================================

println("\n" + "=" * 80)
println("SHELL 2: Updating DIFFERENT partition (Sao Paulo) with driver-test2")
println("This should SUCCEED since it's a different partition")
println("=" * 80)

val updates2DiffPartition = spark.read.parquet(parquetPath)
  .drop("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path", "_hoodie_file_name")
  .where("partitionpath = 'americas/brazil/sao_paulo'")
  .withColumn("driver", lit("driver-test2"))

println(s"Updating ${updates2DiffPartition.count()} records in Sao Paulo partition")

updates2DiffPartition.write.format("hudi")
  .options(getStorageLockWriteConfigs())
  .mode(Append)
  .save(fullBasePath)

println("Shell 2 update to different partition complete!")
println("=" * 80)

// ============================================================================
// VERIFICATION: Check the final state
// ============================================================================

println("\n" + "=" * 80)
println("VERIFICATION: Checking final state")
println("=" * 80)

val finalData = spark.read.format("hudi").load(fullBasePath)

println("\nDriver distribution by partition:")
finalData.groupBy("partitionpath", "driver").count().orderBy("partitionpath", "driver").show(100, false)

println("\nExpected results:")
println("  - San Francisco: all records should have driver='driver-test1'")
println("  - Sao Paulo: all records should have driver='driver-test2'")
println("  - Other partitions: original driver values")

println("=" * 80)

// ============================================================================
// ADDITIONAL NOTES
// ============================================================================

/*
CONCURRENCY TEST EXECUTION GUIDE:

1. Configure storage type at the top of this script:
   - Set storageType = "s3" for AWS S3
   - Set storageType = "gcs" for Google Cloud Storage

2. Start TWO spark-shell sessions:
   - For S3: spark-shell --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.15.0
   - For GCS: spark-shell --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.15.0,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.11

3. In ONE session, run STEP 1 (Initial Ingestion)

4. In Shell 1: Run STEP 2a to update San Francisco partition

5. WHILE Shell 1 is running (or immediately after), in Shell 2:
   - Run STEP 2b to attempt updating the SAME partition (should fail)
   - Then run STEP 3 to update a DIFFERENT partition (should succeed)

6. Run VERIFICATION in either shell to check the results

EXPECTED BEHAVIOR:
- Storage-based lock provider uses cloud storage conditional writes to ensure atomicity
- Lock files are stored in .hoodie/.locks/ directory
- Concurrent writes to the SAME partition should fail with HoodieWriteConflictException
- Concurrent writes to DIFFERENT partitions should succeed
- Lock validity timeout is 5 minutes (configurable)
- Lock is automatically renewed every 30 seconds while the write is in progress

TROUBLESHOOTING:
- If you get credential errors, configure cloud credentials properly
- If lock acquisition times out, check bucket permissions
- If both operations succeed, verify OCC is enabled and lock provider is correct
- Check the .hoodie/.locks/ directory in your bucket for lock files
*/
