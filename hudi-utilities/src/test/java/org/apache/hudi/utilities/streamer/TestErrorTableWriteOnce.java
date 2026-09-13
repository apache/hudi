/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression coverage for the error-table write landing exactly once under
 * {@code hoodie.errortable.write.unification.enabled=true}.
 *
 * <p>The error-table write-status RDD is only ever kept alive by a Spark cache: the bulk insert
 * persists the statuses under the instant's cache key, and the error table's commit releases that
 * cache (and, in the writer, the upstream error events too). Reading the RDD after the commit
 * therefore re-runs the bulk insert and lands every error record a second time under an instant
 * that is already complete.
 *
 * <p>Both tests drive a real {@link RddBackedErrorTableWriter} over a real Hudi table and assert on
 * the record keys the error table actually holds, not on counts alone. The second test pins the
 * broken ordering so the guard in {@link ErrorTableCommitter#collectAndCommit} cannot be quietly
 * removed: it is red on the fixed ordering and green only on the ordering that caused the
 * duplication.
 */
class TestErrorTableWriteOnce extends SparkClientFunctionalTestHarness {

  private static final int ERROR_RECORD_COUNT = 20;
  private static final String BASE_TABLE_INSTANT = "20260913120000000";

  /**
   * The ordering {@code StreamSync} uses: collect the statuses while the write is still cached,
   * then commit, then count off the collected list.
   */
  @Test
  void collectBeforeCommitWritesEachErrorRecordOnce() throws Exception {
    Fixture fixture = newFixture();
    try (RddBackedErrorTableWriter writer = fixture.writer) {
      JavaRDD<WriteStatus> writeStatusRDD = writer.upsert(BASE_TABLE_INSTANT, Option.empty());

      ErrorTableCommitter.ErrorTableCommitResult result = ErrorTableCommitter.collectAndCommit(
          writer, Option.of(writeStatusRDD), true, BASE_TABLE_INSTANT, Option.empty());
      assertTrue(result.isSuccess(), "error table commit should succeed");

      List<WriteStatus> statuses = result.getWriteStatuses().get();
      SuccessfulRecordCounter.Counts counts = SuccessfulRecordCounter.compute(
          new ArrayList<>(), Option.of(statuses), true);

      assertEquals(fixture.expectedKeys, readErrorTableKeys(fixture.errorTablePath),
          "the error table must hold each error record exactly once");
      assertEquals(ERROR_RECORD_COUNT, counts.getTotalRecords(),
          "counts must come from the collected statuses");
      assertEquals(0L, counts.getTotalErrorRecords());
    }
  }

  /**
   * The ordering that shipped before {@link ErrorTableCommitter#collectAndCommit}: commit first,
   * then aggregate over the same RDD. The commit has released both caches by then, so the
   * aggregate re-runs the bulk insert and the error table ends up with two rows per record. The
   * re-run allocates fresh file ids, so the duplicates land in new file groups stamped with the
   * already-completed instant and a snapshot read returns both.
   *
   * <p>If this test goes red, the release-and-recompute mechanism itself changed. Confirm that
   * before treating it as a regression in this test.
   */
  @Test
  void readingTheRddAfterTheCommitWritesEveryErrorRecordTwice() throws Exception {
    Fixture fixture = newFixture();
    try (RddBackedErrorTableWriter writer = fixture.writer) {
      JavaRDD<WriteStatus> writeStatusRDD = writer.upsert(BASE_TABLE_INSTANT, Option.empty());

      assertTrue(ErrorTableCommitter.commit(writer, Option.of(writeStatusRDD), true,
          BASE_TABLE_INSTANT, Option.empty()), "error table commit should succeed");
      long totalRecords = writeStatusRDD.aggregate(
          0L, (acc, ws) -> acc + ws.getTotalRecords(), Long::sum);

      assertEquals(ERROR_RECORD_COUNT, totalRecords,
          "the recomputed statuses still report one write's worth of records, which is why the "
              + "duplication was invisible to the counts");
      List<String> expectedTwice = fixture.expectedKeys.stream()
          .flatMap(key -> Stream.of(key, key))
          .sorted()
          .collect(Collectors.toList());
      assertEquals(expectedTwice, readErrorTableKeys(fixture.errorTablePath),
          "every error record landed twice under the committed instant");
    }
  }

  private List<String> readErrorTableKeys(String errorTablePath) {
    return spark().read().format("hudi").load(errorTablePath)
        .select(HoodieRecord.RECORD_KEY_METADATA_FIELD)
        .collectAsList().stream()
        .map(row -> row.getString(0))
        .sorted()
        .collect(Collectors.toList());
  }

  private Fixture newFixture() throws Exception {
    String errorTablePath = basePath() + "/error_table";
    HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName("error_table")
        .setPayloadClass(HoodieAvroPayload.class)
        .setRecordKeyFields("_row_key")
        .setPartitionFields("partition_path")
        .initTable(storageConf().newInstance(), errorTablePath);

    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(errorTablePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withFinalizeWriteParallelism(2)
        .withDeleteParallelism(2)
        // Both are defaults, pinned because the tests depend on them. Releasing resources on
        // commit is what drops the write-status cache and makes the recompute observable, and the
        // snapshot read has to list the table directly: served from the metadata table it would
        // only ever see the first write's files, hiding the duplicates as orphans.
        .withReleaseResourceEnabled(true)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .forTable("error_table")
        .build();

    RddBackedErrorTableWriter writer =
        new RddBackedErrorTableWriter(spark(), context(), writeConfig);

    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator(0L);
    List<HoodieRecord> errorRecords = dataGenerator.generateInserts(BASE_TABLE_INSTANT, ERROR_RECORD_COUNT);
    List<ErrorEvent<HoodieRecord>> errorEvents = errorRecords.stream()
        .map(record -> new ErrorEvent<>(record, ErrorEvent.ErrorReason.HUDI_WRITE_FAILURES))
        .collect(Collectors.toList());
    writer.addErrorEvents(jsc().parallelize(errorEvents, 2));

    List<String> expectedKeys = errorRecords.stream()
        .map(HoodieRecord::getRecordKey)
        .sorted()
        .collect(Collectors.toList());
    return new Fixture(errorTablePath, writer, expectedKeys);
  }

  private static final class Fixture {
    private final String errorTablePath;
    private final RddBackedErrorTableWriter writer;
    private final List<String> expectedKeys;

    private Fixture(String errorTablePath, RddBackedErrorTableWriter writer, List<String> expectedKeys) {
      this.errorTablePath = errorTablePath;
      this.writer = writer;
      this.expectedKeys = expectedKeys;
    }
  }
}
