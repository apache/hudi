# Storage-Based Lock Provider Concurrency Test (S3 & GCS)

This test demonstrates concurrent write operations using Hudi's storage-based lock provider with S3 or GCS.

## Overview

The storage-based lock provider uses cloud storage conditional write capabilities to implement distributed locking without requiring external services like ZooKeeper or DynamoDB.

## Key Differences from ZooKeeper Lock Provider

| Feature | ZooKeeper Lock Provider | Storage-Based Lock Provider |
|---------|------------------------|----------------------------|
| External Service | Requires ZooKeeper cluster | No external service needed |
| Configuration | `hoodie.write.lock.zookeeper.*` | `hoodie.write.lock.storage.*` |
| Lock Provider Class | `ZookeeperBasedLockProvider` | `StorageBasedLockProvider` |
| Lock Storage | ZooKeeper znodes | Cloud storage objects in `.hoodie/.locks/` |
| Lock Mechanism | ZooKeeper ephemeral nodes | Conditional writes (ETags/preconditions) |

## Configuration

### Storage-Based Lock Provider

```scala
.option("hoodie.write.concurrency.mode", "optimistic_concurrency_control")
.option("hoodie.write.lock.provider", "org.apache.hudi.client.transaction.lock.StorageBasedLockProvider")
.option("hoodie.write.lock.storage.validity.timeout.secs", "300")  // 5 minutes
.option("hoodie.write.lock.storage.renew.interval.secs", "30")     // Renew every 30 seconds
```

## Prerequisites

### For S3:
1. **S3 Bucket**: Configured and accessible S3 bucket
2. **AWS Credentials**: Configure via one of:
   - Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
   - `~/.aws/credentials` file
   - IAM role (if running on EC2/EMR)
3. **Spark Shell**: Two spark-shell sessions with Hudi dependencies

### For GCS:
1. **GCS Bucket**: Configured and accessible GCS bucket
2. **GCP Credentials**: Configure via one of:
   - Service account key file: `GOOGLE_APPLICATION_CREDENTIALS` environment variable
   - Application Default Credentials (ADC)
   - Compute Engine service account (if running on GCE/Dataproc)
3. **Spark Shell**: Two spark-shell sessions with Hudi and GCS connector dependencies

## Test Execution

### Step 1: Update Configuration

Edit `storage_lock_shell1.scala` and `storage_lock_shell2.scala`:

**For S3:**
```scala
val storageType = "s3"  // Use S3
val bucketName = "your-bucket-name"
val basePath = "ljain"  // Optional prefix path
```

**For GCS:**
```scala
val storageType = "gcs"  // Use GCS
val bucketName = "your-bucket-name"
val basePath = "hudi"  // Optional prefix path
```

### Step 2: Start Two Spark Shells

**For S3:**
```bash
spark-shell --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.15.0
```

**For GCS:**
```bash
spark-shell --packages org.apache.hudi:hudi-spark3-bundle_2.12:0.15.0,com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.11
```

### Step 3: Run Initial Setup (Shell 1)

In Terminal 1 (Shell 1), load and run the initial ingestion:

```scala
:load storage_lock_shell1.scala
```

Wait for the initial ingestion to complete. This creates the table and exports data to parquet.

### Step 4: Test Concurrent Writes

#### Shell 1: Start Update
In Terminal 1, the script will start updating the San Francisco partition.

#### Shell 2: Concurrent Operations
In Terminal 2, immediately load and run:

```scala
:load storage_lock_shell2.scala
```

This will:
1. **Attempt to update SAME partition** (San Francisco) - Should FAIL with `HoodieWriteConflictException`
2. **Update DIFFERENT partition** (Sao Paulo) - Should SUCCEED

### Step 5: Verify Results

The verification section in Shell 2 will display the final state. Expected results:

- **San Francisco partition**: All records have `driver='driver-test1'` (from Shell 1)
- **Sao Paulo partition**: All records have `driver='driver-test2'` (from Shell 2)
- **Other partitions**: Original driver values unchanged

## What's Happening Behind the Scenes

1. **Lock Acquisition**: When a write starts, Hudi creates a lock file at:
   - S3: `s3://bucket/path/storage_concurrency_test/.hoodie/.locks/table_lock.json`
   - GCS: `gs://bucket/path/storage_concurrency_test/.hoodie/.locks/table_lock.json`

2. **Lock File Content**: The lock file contains:
   ```json
   {
     "ownerId": "writer-uuid",
     "validUntil": 1234567890000,
     "expired": false
   }
   ```

3. **Atomic Operations**:
   - S3: Uses ETags with conditional operations (`If-None-Match: "*"`, `If-Match: <etag>`)
   - GCS: Uses generation numbers and preconditions

4. **Heartbeat Renewal**: Every 30 seconds, the lock holder updates the `validUntil` timestamp

5. **Conflict Detection**: When commit validation happens, Hudi checks for overlapping file groups between concurrent writes

## Expected Output

### Shell 1 Output
```
Shell 1: Starting initial ingestion...
Shell 1: Initial ingestion complete!
Shell 1: Exported to parquet for updates
Shell 1: Updating San Francisco partition with driver-test1...
Shell 1: Update complete!
```

### Shell 2 Output
```
Shell 2: Attempting to update San Francisco partition with driver-test2...
This should FAIL because Shell 1 is updating the same partition

================================================================================
SUCCESS! Got expected write conflict exception
================================================================================
Message: java.util.ConcurrentModificationException: Cannot resolve conflicts for overlapping writes

================================================================================
Shell 2: Now updating a DIFFERENT partition (Sao Paulo) with driver-test2...
This should SUCCEED
================================================================================
Shell 2: Update to different partition complete!

VERIFICATION: Checking final state
Driver distribution by partition:
+------------------------------------------+------------+-----+
|partitionpath                             |driver      |count|
+------------------------------------------+------------+-----+
|americas/brazil/sao_paulo                 |driver-test2|1234 |
|americas/united_states/san_francisco      |driver-test1|5678 |
|...                                       |...         |...  |
+------------------------------------------+------------+-----+
```

## Troubleshooting

### S3 Credential Errors
```
Error: Unable to load credentials from any provider in the chain
```
**Solution**: Configure AWS credentials properly. Add to your Spark configuration:
```scala
spark.conf.set("fs.s3a.access.key", "YOUR_ACCESS_KEY")
spark.conf.set("fs.s3a.secret.key", "YOUR_SECRET_KEY")
```

### GCS Credential Errors
```
Error: Unable to load GCS credentials
```
**Solution**: Configure GCP credentials:
```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

Or in Spark:
```scala
spark.conf.set("fs.gs.auth.service.account.enable", "true")
spark.conf.set("fs.gs.auth.service.account.json.keyfile", "/path/to/key.json")
```

### Lock Acquisition Timeout
```
Error: Failed to acquire lock within timeout period
```
**Solution**:
- Check bucket permissions
- Increase lock validity timeout: `hoodie.write.lock.storage.validity.timeout.secs`
- Check for orphaned lock files in `.hoodie/.locks/`

### Both Operations Succeed
```
Both Shell 1 and Shell 2 updates succeeded (unexpected)
```
**Solution**:
- Verify OCC is enabled: `hoodie.write.concurrency.mode=optimistic_concurrency_control`
- Verify correct lock provider class is configured
- Check if both writers are updating truly overlapping file groups

### Access Denied
**S3:**
```
Error: Access Denied (Service: Amazon S3; Status Code: 403)
```
**Solution**: Ensure IAM role/credentials have `s3:PutObject`, `s3:GetObject`, `s3:DeleteObject` permissions

**GCS:**
```
Error: Access Denied (Service: GCS; Status Code: 403)
```
**Solution**: Ensure service account has `storage.objects.create`, `storage.objects.get`, `storage.objects.delete` permissions

## Files and Locations

- **Lock files**: `.hoodie/.locks/table_lock.json`
- **Audit files** (if enabled): `.hoodie/.locks/audit/`
- **Timeline metadata**: `.hoodie/`

## Additional Resources

- [Hudi Concurrency Control Documentation](https://hudi.apache.org/docs/concurrency_control)
- [Storage-Based Lock Provider Design](https://github.com/apache/hudi/blob/master/rfc/rfc-78/rfc-78.md)
- [HUDI-9782: Storage-based lock provider implementation](https://issues.apache.org/jira/browse/HUDI-9782)

## Notes

- The storage-based lock provider is simpler to deploy than ZooKeeper-based locking
- No additional infrastructure required beyond your storage system (S3, GCS, etc.)
- Lock files are automatically cleaned up by Hudi's cleaner service
- For production use, consider enabling audit logging for lock operations
