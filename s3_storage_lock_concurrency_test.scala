// Storage-based Lock Provider Concurrency Test for S3
// This script demonstrates concurrent write operations using S3-based storage lock provider
//
// Prerequisites:
// 1. S3 bucket configured and accessible
// 2. AWS credentials configured (via environment variables, ~/.aws/credentials, or IAM role)
// 3. Two spark-shell sessions to simulate concurrent writes
//
// Usage:
// Session 1: Run the initial ingestion and first update
// Session 2: Run concurrent updates on same/different partitions

// ============================================================================
// CONFIGURATION - Update these values for your environment
// ============================================================================

val tableName = "s3_concurrency_test"
val s3BucketName = "your-bucket-name"  // CHANGE THIS to your S3 bucket
val s3BasePath = s"s3a://${s3BucketName}/hudi/${tableName}"
val s3ParquetPath = s"s3a://${s3BucketName}/hudi/${tableName}_parquet"

// Optional: Uncomment and configure if not using default AWS credentials
// val awsAccessKey = "YOUR_ACCESS_KEY"
// val awsSecretKey = "YOUR_SECRET_KEY"

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

    // Optional: Add AWS credentials if not using default credential provider chain
    // "hoodie.aws.access.key" -> awsAccessKey,
    // "hoodie.aws.secret.key" -> awsSecretKey
  )
}

// ============================================================================
// STEP 1: INITIAL INGESTION (Run in BOTH sessions first)
// ============================================================================

println("=" * 80)
println("STEP 1: Initial Data Generation and Ingestion")
println("=" * 80)

val dataGen = new DataGenerator

val inserts = convertToStringList(dataGen.generateInserts(10000))
val df = spark.read.json(spark.sparkContext.parallelize(inserts, 20))

println(s"Generated ${inserts.size()} records for initial ingestion")
println(s"Writing to: ${s3BasePath}")

df.write.format("hudi")
  .options(getStorageLockWriteConfigs())
  .mode(Overwrite)
  .save(s3BasePath)

println("Initial ingestion complete!")
println("=" * 80)

// ============================================================================
// STEP 2a: SHELL 1 - Update San Francisco partition with driver-test1
// ============================================================================

println("\n" + "=" * 80)
println("SHELL 1: Updating San Francisco partition with driver-test1")
println("=" * 80)

val updates1 = spark.read.parquet(s3ParquetPath)
  .drop("file_group")
  .where("partitionpath = 'americas/united_states/san_francisco'")
  .withColumn("driver", lit("driver-test1"))

println(s"Updating ${updates1.count()} records in San Francisco partition")

updates1.write.format("hudi")
  .options(getStorageLockWriteConfigs())
  .mode(Append)
  .save(s3BasePath)

println("Shell 1 update complete!")
println("=" * 80)

// ============================================================================
// STEP 2b: SHELL 2 - Concurrent update to SAME partition (should fail)
// ============================================================================

println("\n" + "=" * 80)
println("SHELL 2: Attempting concurrent update to SAME partition (San Francisco)")
println("This should FAIL with HoodieWriteConflictException")
println("=" * 80)

val updates2SamePartition = spark.read.parquet(s3ParquetPath)
  .drop("file_group")
  .where("partitionpath = 'americas/united_states/san_francisco'")
  .withColumn("driver", lit("driver-test2"))

println(s"Attempting to update ${updates2SamePartition.count()} records in San Francisco partition")

try {
  updates2SamePartition.write.format("hudi")
    .options(getStorageLockWriteConfigs())
    .mode(Append)
    .save(s3BasePath)

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

val updates2DiffPartition = spark.read.parquet(s3ParquetPath)
  .drop("file_group")
  .where("partitionpath = 'americas/brazil/sao_paulo'")
  .withColumn("driver", lit("driver-test2"))

println(s"Updating ${updates2DiffPartition.count()} records in Sao Paulo partition")

updates2DiffPartition.write.format("hudi")
  .options(getStorageLockWriteConfigs())
  .mode(Append)
  .save(s3BasePath)

println("Shell 2 update to different partition complete!")
println("=" * 80)

// ============================================================================
// VERIFICATION: Check the final state
// ============================================================================

println("\n" + "=" * 80)
println("VERIFICATION: Checking final state")
println("=" * 80)

val finalData = spark.read.format("hudi").load(s3BasePath)

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

1. Start TWO spark-shell sessions with Hudi dependencies

2. In BOTH sessions, run STEP 1 (Initial Ingestion) in one of them

3. Export the table to parquet for reading in updates:
   spark.read.format("hudi").load(s3BasePath).write.mode("overwrite").parquet(s3ParquetPath)

4. In Shell 1: Run STEP 2a to update San Francisco partition

5. WHILE Shell 1 is running (or immediately after), in Shell 2:
   - Run STEP 2b to attempt updating the SAME partition (should fail)
   - Then run STEP 3 to update a DIFFERENT partition (should succeed)

6. Run VERIFICATION in either shell to check the results

EXPECTED BEHAVIOR:
- Storage-based lock provider uses S3 conditional writes to ensure atomicity
- Lock files are stored in s3://bucket/hudi/s3_concurrency_test/.hoodie/.locks/
- Concurrent writes to the SAME partition should fail with HoodieWriteConflictException
- Concurrent writes to DIFFERENT partitions should succeed
- Lock validity timeout is 5 minutes (configurable)
- Lock is automatically renewed every 30 seconds while the write is in progress

TROUBLESHOOTING:
- If you get AWS credential errors, configure AWS credentials properly
- If lock acquisition times out, check S3 bucket permissions
- If both operations succeed, verify OCC is enabled and lock provider is correct
- Check the .hoodie/.locks/ directory in S3 for lock files
*/
