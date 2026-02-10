/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.action.clean;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.config.HoodieCompactionConfig.PARQUET_SMALL_FILE_LIMIT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestBlobCleaner extends HoodieClientTestBase {

  /**
   * Test cleaning with blob fields containing inline blobs, managed external references,
   * and unmanaged external references. Verifies that:
   * - Managed blob files from old commits are deleted
   * - Managed blob files from recent commits are retained
   * - Unmanaged blob files are never deleted
   * - Inline blobs don't cause issues
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  void testCleaningWithBlobFields(HoodieTableType tableType) throws Exception {
    // Create schema with blob fields
    HoodieSchema blobSchema = createBlobTestSchema();

    HoodieWriteConfig.Builder configBuilder = getConfigBuilder(blobSchema);
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      configBuilder = configBuilder.withCompactionConfig(HoodieCompactionConfig.newBuilder()
          .withInlineCompaction(true)
          .withMaxNumDeltaCommitsBeforeCompaction(2)
          .build());
    }
    HoodieWriteConfig config = configBuilder.build();

    HoodieTestUtils.init(storageConf, basePath, tableType);

    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      // Commit 1: Write initial records with blobs
      String commit1 = generateAndWriteBlobs(client, 10, blobSchema);

      // Commit 2: Update some records, add new ones
      String commit2 = generateAndWriteBlobs(client, 15, blobSchema);

      // Commit 3: More updates
      String commit3 = generateAndWriteBlobs(client, 12, blobSchema);

      // Commit 4: Final updates
      String commit4 = generateAndWriteBlobs(client, 8, blobSchema);

      // Trigger clean (should remove commit 1 and 2 files)
      HoodieCleanMetadata cleanMetadata = client.clean();

      // Verify blob file cleanup
      assertNotNull(cleanMetadata, "Clean metadata should not be null");
      verifyBlobFileCleanup(commit1, commit2, commit3, commit4);
    }
  }

  /**
   * Validate that blob files are cleaned correctly after clustering operation which can move references between file groups.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  void testCleaningAfterClustering(HoodieTableType tableType) throws Exception {
    // Create schema with blob fields
    HoodieSchema blobSchema = createBlobTestSchema();

    HoodieWriteConfig.Builder configBuilder = getConfigBuilder(blobSchema)
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withInlineClustering(false) // disable inline clustering to control timing
            .withAsyncClusteringMaxCommits(1)
            .build());
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      configBuilder = configBuilder.withCompactionConfig(HoodieCompactionConfig.newBuilder()
          .withInlineCompaction(false) // disable compaction, log files are compacted during clustering
          .build());
    }
    HoodieWriteConfig config = configBuilder.build();

    HoodieTestUtils.init(storageConf, basePath, tableType);

    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      // Commit 1: Write initial records with blobs
      String commit1 = generateAndWriteBlobs(client, 10, blobSchema);

      // Commit 2: Insert more records to create a new file group
      String commit2 = generateAndWriteBlobs(client, 15, blobSchema);

      // Commit 3: Write updates
      String commit3 = generateAndWriteBlobs(client, 12, blobSchema);

      // Trigger clustering
      Option<String> clusteringInstant = client.scheduleClustering(Option.empty());
      assertTrue(clusteringInstant.isPresent(), "Clustering should be scheduled");
      client.cluster(clusteringInstant.get());

      // Commit 4: Final updates
      String commit4 = generateAndWriteBlobs(client, 8, blobSchema);

      // Trigger clean (should remove commit 1 and 2 files)
      HoodieCleanMetadata cleanMetadata = client.clean();

      // Verify blob file cleanup
      assertNotNull(cleanMetadata, "Clean metadata should not be null");
      verifyBlobFileCleanup(commit1, commit2, commit3, commit4);
    }
  }

  /**
   * Create HoodieSchema with a single blob field for testing.
   * Different records will have different blob types (inline/managed/unmanaged).
   */
  private HoodieSchema createBlobTestSchema() {
    // Create blob field using HoodieSchema
    HoodieSchema.Blob blobSchema = HoodieSchema.createBlob();

    // Create record schema with single blob field
    return HoodieSchema.createRecord(
        "test_record",
        "org.apache.hudi.test",
        null,
        Arrays.asList(
            HoodieSchemaField.of("_row_key", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("partition_path", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("timestamp", HoodieSchema.create(HoodieSchemaType.LONG)),
            HoodieSchemaField.of("file_data", blobSchema)  // Single blob field for all types
        )
    );
  }

  private HoodieWriteConfig.Builder getConfigBuilder(HoodieSchema blobSchema) {
    return getConfigBuilder()
        .withProps(Collections.singletonMap(PARQUET_SMALL_FILE_LIMIT.key(), "0")) // Disable small file handling to introduce multiple file groups in the test
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(1)  // Keep only 1 commit as history so 2 commits are cleaned later
            .withAutoClean(false) // Manually trigger clean to control timing
            .build())
        .withSchema(blobSchema.toString());
  }

  private String generateAndWriteBlobs(SparkRDDWriteClient client, int numRecords, HoodieSchema blobSchema) throws IOException {
    String instant = client.startCommit();
    List<GenericRecord> records1 = generateBlobRecords(instant, numRecords, blobSchema);
    createManagedBlobFiles(instant, numRecords);
    writeRecordsWithBlobs(client, instant, records1);
    return instant;
  }

  /**
   * Generate test records with different blob types in a single blob column.
   * Uses i % 3 pattern to vary blob types across records:
   * - i % 3 == 0: inline blob
   * - i % 3 == 1: managed external reference
   * - i % 3 == 2: unmanaged external reference
   */
  private List<GenericRecord> generateBlobRecords(
      String commitTime, int numRecords, HoodieSchema schema) {
    List<GenericRecord> records = new ArrayList<>();

    for (int i = 0; i < numRecords; i++) {
      GenericRecord record = new GenericData.Record(schema.toAvroSchema());
      record.put("_row_key", "key_" + i);
      record.put("partition_path", "2020/01/01");
      record.put("timestamp", Long.parseLong(commitTime));

      // Vary blob types across records using i % 3 pattern
      GenericRecord blobValue;
      if (i % 3 == 0) {
        // Inline blob - data stored in record
        blobValue = createInlineBlob(schema, ("inline_data_" + commitTime + "_" + i).getBytes());
      } else if (i % 3 == 1) {
        // Managed external reference - should be cleaned when record is removed
        String managedPath = basePath + "/.hoodie/blobs/" + commitTime + "_" + i + ".bin";
        blobValue = createBlob(schema, managedPath, 0L, 1024L, true);
      } else {
        // Unmanaged external reference - should NEVER be cleaned
        String unmanagedPath = basePath + "/.hoodie/blobs/" + commitTime + "_" + i +  ".bin";
        blobValue = createBlob(schema, unmanagedPath, 0L, 2048L, false);
      }

      record.put("file_data", blobValue);
      records.add(record);
    }

    return records;
  }

  /**
   * Create an inline blob (data stored in record).
   */
  private GenericRecord createInlineBlob(HoodieSchema recordSchema, byte[] data) {
    HoodieSchema blobSchema = recordSchema.getField("file_data").get().getNonNullSchema();
    GenericRecord blob = new GenericData.Record(blobSchema.toAvroSchema());
    blob.put("storage_type", "inline");
    blob.put("data", java.nio.ByteBuffer.wrap(data));
    blob.put("reference", null);
    return blob;
  }

  /**
   * Create a blob
   */
  private GenericRecord createBlob(
      HoodieSchema recordSchema, String path, Long offset, Long length, boolean managed) {
    HoodieSchema blobSchema = recordSchema.getField("file_data").get().getNonNullSchema();
    HoodieSchema refSchema = blobSchema.getField("reference").get().getNonNullSchema();

    GenericRecord reference = new GenericData.Record(refSchema.toAvroSchema());
    reference.put("external_path", path);
    reference.put("offset", offset);
    reference.put("length", length);
    reference.put("managed", managed);

    GenericRecord blob = new GenericData.Record(blobSchema.toAvroSchema());
    blob.put("storage_type", "out_of_line");
    blob.put("data", null);
    blob.put("reference", reference);
    return blob;
  }

  /**
   * Write records with blob fields to the table.
   */
  private void writeRecordsWithBlobs(SparkRDDWriteClient client, String commitTime, List<GenericRecord> records) {
    // Convert GenericRecords to HoodieRecords
    List<HoodieRecord> hoodieRecords = records.stream()
        .map(gr -> {
          String key = gr.get("_row_key").toString();
          String partition = gr.get("partition_path").toString();
          HoodieKey hoodieKey = new HoodieKey(key, partition);
          return new HoodieAvroIndexedRecord(hoodieKey, gr);
        })
        .collect(Collectors.toList());

    // Write to table
    JavaRDD<HoodieRecord> recordsRDD = jsc.parallelize(hoodieRecords, 2);
    client.commit(commitTime, client.upsert(recordsRDD, commitTime));
  }

  /**
   * Create managed blob files that are referenced by managed blob fields.
   * Only creates files for records where i % 3 == 1 (managed blobs).
   */
  private void createManagedBlobFiles(String commitTime, int numRecords) throws IOException {
    StoragePath blobDir = new StoragePath(basePath, ".hoodie/blobs");
    if (!storage.exists(blobDir)) {
      storage.createDirectory(blobDir);
    }

    for (int i = 0; i < numRecords; i++) {
      // Only create files for managed blobs (i % 3 == 1)
      if (i % 3 != 0) {
        String blobFileName = commitTime + "_" + i + ".bin";
        StoragePath blobPath = new StoragePath(blobDir, blobFileName);

        // Write dummy data to blob file
        byte[] dummyData = new byte[1024];
        Arrays.fill(dummyData, (byte) i);
        try (OutputStream out = storage.create(blobPath)) {
          out.write(dummyData);
        }
      }
    }
  }

  /**
   * Verify that blob files are cleaned up correctly.
   * Only checks managed blob files (i % 3 == 1), as inline blobs (i % 3 == 0) have no external files
   * and unmanaged blobs (i % 3 == 2) are external files not managed by Hudi.
   */
  private void verifyBlobFileCleanup(String commit1, String commit2, String commit3, String commit4)
      throws IOException {
    StoragePath blobDir = new StoragePath(basePath, ".hoodie/blobs");
    // Check commit 1 managed blobs - should be DELETED (only i % 3 == 1)
    for (int i = 0; i < 10; i++) {
      if (i % 3 == 1) {
        StoragePath blobPath = getStoragePathForBlob(commit1, blobDir, i);
        assertFalse(storage.exists(blobPath), "Managed blob from old commit should be deleted: " + blobPath);
      } else if (i % 3 == 2) {
        // Unmanaged blobs should never be deleted
        StoragePath blobPath = getStoragePathForBlob(commit1, blobDir, i);
        assertTrue(storage.exists(blobPath), "Unmanaged blob should never be deleted: " + blobPath);
      }
    }

    // Check commit 2 managed blobs - should be DELETED (only i % 3 == 1)
    for (int i = 0; i < 15; i++) {
      if (i % 3 == 1) {
        StoragePath blobPath = getStoragePathForBlob(commit2, blobDir, i);
        // only records 0-12 are updated
        assertEquals(i >= 12, storage.exists(blobPath), "Managed and updated blob from commit2 should be removed: " + blobPath);
      } else if (i % 3 == 2) {
        // Unmanaged blobs should never be deleted
        StoragePath blobPath = getStoragePathForBlob(commit2, blobDir, i);
        assertTrue(storage.exists(blobPath), "Unmanaged blob should never be deleted: " + blobPath);
      }
    }

    // Check commit 3 managed blobs - should be RETAINED (only i % 3 == 1)
    for (int i = 0; i < 12; i++) {
      if (i % 3 == 1) {
        StoragePath blobPath = getStoragePathForBlob(commit3, blobDir, i);
        assertTrue(storage.exists(blobPath), "Managed blob from latest commit should be retained: " + blobPath);
      } else if (i % 3 == 2) {
        // Unmanaged blobs should never be deleted
        StoragePath blobPath = getStoragePathForBlob(commit3, blobDir, i);
        assertTrue(storage.exists(blobPath), "Unmanaged blob should never be deleted: " + blobPath);
      }
    }

    // Check commit 4 managed blobs - should be RETAINED (only i % 3 == 1)
    for (int i = 0; i < 8; i++) {
      if (i % 3 == 1) {
        StoragePath blobPath = getStoragePathForBlob(commit4, blobDir, i);
        assertTrue(storage.exists(blobPath), "Managed blob from latest commit should be retained: " + blobPath);
      } else if (i % 3 == 2) {
        // Unmanaged blobs should never be deleted
        StoragePath blobPath = getStoragePathForBlob(commit4, blobDir, i);
        assertTrue(storage.exists(blobPath), "Unmanaged blob should never be deleted: " + blobPath);
      }
    }
  }

  private static StoragePath getStoragePathForBlob(String commit2, StoragePath blobDir, int i) {
    return new StoragePath(blobDir, commit2 + "_" + i + ".bin");
  }
}
