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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.collection.Tuple3;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.streamer.ErrorEvent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieDeltaStreamerErrorTableWriteFlow extends TestHoodieDeltaStreamerSchemaEvolutionBase {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Bad records a source batch can carry, each built from valid generated records by one column mutation. A case
   * declares, per write operation, whether the streamer must route the record to the error table or write it to the
   * base table, and the reason it is routed there, so every case is exercised by the same streamer sync. Cover a new
   * failure mode by adding a case here rather than a new test, since each streamer sync is expensive.
   */
  enum BadRecordCase {
    // The original record key is kept in driver, so each quarantined record stays attributable.
    EMPTY_RECORD_KEY(df -> df.withColumn("driver", functions.col("_row_key")).withColumn("_row_key", functions.lit("")),
        operation -> true, ErrorEvent.ErrorReason.RECORD_CREATION),
    // Only an upsert combines records by their ordering value; the other operations write a null one through.
    NULL_ORDERING_VALUE(df -> df.withColumn("timestamp", functions.lit(null).cast(DataTypes.LongType)),
        operation -> operation == WriteOperationType.UPSERT, ErrorEvent.ErrorReason.RECORD_CREATION);

    private final UnaryOperator<Dataset<Row>> mutation;
    private final Predicate<WriteOperationType> routedToErrorTable;
    private final ErrorEvent.ErrorReason errorReason;

    BadRecordCase(UnaryOperator<Dataset<Row>> mutation, Predicate<WriteOperationType> routedToErrorTable,
                  ErrorEvent.ErrorReason errorReason) {
      this.mutation = mutation;
      this.routedToErrorTable = routedToErrorTable;
      this.errorReason = errorReason;
    }
  }

  protected void testBase(Tuple3<Integer, Integer, Integer> sourceGenInfo) throws Exception {
    int totalRecords = sourceGenInfo.f0;
    int errorRecords = sourceGenInfo.f1;
    int numFiles = sourceGenInfo.f2;
    boolean shouldCreateMultipleSourceFiles = numFiles > 1;
    // Expected outcome, kept up to date as each batch is built: the base table as record key to rider, and the records
    // routed to the error table as (record, reason), where a record is named by its key, or by driver when the key is empty.
    Map<String, String> expectedRiders = new HashMap<>();
    List<String> expectedErrorRecords = new ArrayList<>();

    PARQUET_SOURCE_ROOT = basePath + "parquetFilesDfs" + testNum++;

    List<Row> validRecords = Collections.emptyList();
    StructType sourceSchema = null;
    if (totalRecords > 0) {
      if (shouldCreateMultipleSourceFiles) {
        prepareParquetDFSMultiFiles(totalRecords - errorRecords, PARQUET_SOURCE_ROOT, numFiles);
      } else {
        prepareParquetDFSFiles(totalRecords - errorRecords, PARQUET_SOURCE_ROOT);
      }
      Dataset<Row> validRecordsDf = sparkSession.read().parquet(PARQUET_SOURCE_ROOT);
      sourceSchema = validRecordsDf.schema();
      validRecords = validRecordsDf.orderBy("_row_key").collectAsList();
      validRecords.forEach(row -> expectedRiders.put(row.getAs("_row_key"), row.getAs("rider")));

      // Add errorRecords records of each bad-record case to the source
      if (errorRecords > 0) {
        for (BadRecordCase badRecordCase : BadRecordCase.values()) {
          String errorDataSourceRoot = basePath + "parquetErrorFilesDfs" + testNum++;
          prepareParquetDFSFiles(errorRecords, errorDataSourceRoot);
          addBadRecords(badRecordCase, sparkSession.read().parquet(errorDataSourceRoot), expectedRiders, expectedErrorRecords);
        }
      }
    } else {
      fs.mkdirs(new Path(PARQUET_SOURCE_ROOT));
    }

    tableName = "test_parquet_table" + testNum;
    tableBasePath = basePath + tableName;
    HoodieDeltaStreamer.Config cfg = getDeltaStreamerConfig();
    cfg.operation = writeOperationType;
    this.deltaStreamer = new HoodieDeltaStreamer(cfg, jsc);
    this.deltaStreamer.sync();
    int expectedInstants = 1;
    int lastSyncErrorRecords = expectedErrorRecords.size();

    // An upsert also updates stored records, in a second sync: a valid update must be applied, every bad record in the
    // batch must reach the error table, and a null ordering value on a stored record must leave that record unchanged.
    // Without a guard at record creation, that null ordering value reaches the record merger. Every check runs after
    // the last sync, so a failure in the first one does not stop the second from reaching the merger.
    if (writeOperationType == WriteOperationType.UPSERT && errorRecords > 0) {
      assertTrue(validRecords.size() >= (BadRecordCase.values().length + 1) * errorRecords,
          "the second sync updates errorRecords stored records per bad-record case, plus as many valid updates");
      int firstSyncErrorRecords = expectedErrorRecords.size();
      List<Row> validUpdates = updatesOf(validRecords.subList(0, errorRecords), sourceSchema);
      addParquetData(validUpdates, sourceSchema);
      validUpdates.forEach(row -> expectedRiders.put(row.getAs("_row_key"), row.getAs("rider")));
      BadRecordCase[] badRecordCases = BadRecordCase.values();
      for (int i = 0; i < badRecordCases.length; i++) {
        List<Row> storedRecords = validRecords.subList((i + 1) * errorRecords, (i + 2) * errorRecords);
        addBadRecords(badRecordCases[i], sparkSession.createDataFrame(updatesOf(storedRecords, sourceSchema), sourceSchema),
            expectedRiders, expectedErrorRecords);
      }
      this.deltaStreamer = new HoodieDeltaStreamer(cfg, jsc);
      this.deltaStreamer.sync();
      expectedInstants++;
      lastSyncErrorRecords = expectedErrorRecords.size() - firstSyncErrorRecords;
    }

    // base table validation
    Dataset<Row> baseDf = sparkSession.read().format("hudi").load(tableBasePath);
    if (expectedRiders.isEmpty()) {
      assertEquals(0, baseDf.count());
    } else {
      Map<String, String> actualRiders = new HashMap<>();
      baseDf.select("_row_key", "rider").collectAsList().forEach(row -> actualRiders.put(row.getString(0), row.getString(1)));
      assertEquals(expectedRiders, actualRiders);
    }

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()))
        .setBasePath(tableBasePath).build();
    assertEquals(expectedInstants, metaClient.getActiveTimeline().getInstants().size());

    // error table validation
    if (withErrorTable) {
      Collections.sort(expectedErrorRecords);
      List<Object> receivedErrorEvents = new ArrayList<>();
      TestErrorTable.receivedErrorEvents.forEach(errorEvents -> receivedErrorEvents.addAll(errorEvents.collect()));
      assertEquals(expectedErrorRecords, errorRecordsOf(receivedErrorEvents));
      // What the error table writer committed, not only what it was handed
      List<Object> committed = new ArrayList<>();
      TestErrorTable.commited.values().forEach(errors -> errors.ifPresent(rdd -> committed.addAll(rdd.collect())));
      if (this.writeErrorTableInParallelWithBaseTable) {
        // The unified write commits write statuses, and each commit replaces the previous one
        assertEquals(lastSyncErrorRecords, committed.stream().mapToLong(status -> ((WriteStatus) status).getTotalRecords()).sum());
      } else {
        assertEquals(expectedErrorRecords, errorRecordsOf(committed));
      }
    }
  }

  private void addBadRecords(BadRecordCase badRecordCase, Dataset<Row> records, Map<String, String> expectedRiders,
                             List<String> expectedErrorRecords) {
    Dataset<Row> badRecords = badRecordCase.mutation.apply(records);
    List<Row> badRows = badRecords.select("_row_key", "rider", "driver").collectAsList();
    addParquetData(badRecords, false);
    if (badRecordCase.routedToErrorTable.test(writeOperationType)) {
      badRows.forEach(row -> expectedErrorRecords.add(errorRecord(row.getString(0), row.getString(2), badRecordCase.errorReason)));
    } else {
      badRows.forEach(row -> expectedRiders.put(row.getString(0), row.getString(1)));
    }
  }

  private void addParquetData(List<Row> rows, StructType schema) {
    addParquetData(sparkSession.createDataFrame(rows, schema), false);
  }

  /**
   * Updates of the given stored records: a new rider and a later ordering value, so each update wins the merge.
   */
  private List<Row> updatesOf(List<Row> storedRecords, StructType schema) {
    return sparkSession.createDataFrame(storedRecords, schema)
        .withColumn("rider", functions.concat(functions.col("rider"), functions.lit("-updated")))
        .withColumn("timestamp", functions.col("timestamp").plus(1))
        .collectAsList();
  }

  /**
   * The given error events as (record, reason), sorted. Error records are serialized as JSON, where a nullable field
   * may be wrapped in its union branch.
   */
  private static List<String> errorRecordsOf(List<Object> errorEvents) throws IOException {
    List<String> errorRecords = new ArrayList<>();
    for (Object errorEvent : errorEvents) {
      ErrorEvent<String> event = (ErrorEvent<String>) errorEvent;
      JsonNode record = OBJECT_MAPPER.readTree(event.getPayload());
      errorRecords.add(errorRecord(stringField(record, "_row_key"), stringField(record, "driver"), event.getReason()));
    }
    Collections.sort(errorRecords);
    return errorRecords;
  }

  private static String errorRecord(String recordKey, String driver, ErrorEvent.ErrorReason reason) {
    return (recordKey.isEmpty() ? driver : recordKey) + "," + reason;
  }

  private static String stringField(JsonNode record, String field) {
    JsonNode value = record.get(field);
    return value.isObject() ? value.get("string").asText() : value.asText();
  }

  protected static Stream<Arguments> testErrorTableWriteFlowArgs() {
    Stream.Builder<Arguments> b = Stream.builder();
    // totalRecords, numErrorRecords (bad records of each bad-record case, added to totalRecords - numErrorRecords valid ones),
    // numSourceFiles, WriteOperationType, shouldWriteErrorTableInUnionWithBaseTable

    // empty source, error table union enabled, INSERT
    b.add(Arguments.of(0, 0, 0, WriteOperationType.INSERT, true));
    // empty source, error table union disabled, INSERT
    b.add(Arguments.of(0, 0, 0, WriteOperationType.INSERT, false));
    // non-empty source, error table union enabled, INSERT
    b.add(Arguments.of(100, 5, 1, WriteOperationType.INSERT, true));
    // non-empty source, error table union disabled, INSERT
    b.add(Arguments.of(100, 5, 1, WriteOperationType.INSERT, false));
    // non-empty source, error table union enabled, UPSERT
    b.add(Arguments.of(100, 5, 1, WriteOperationType.UPSERT, true));
    // non-empty source, error table union disabled, UPSERT
    b.add(Arguments.of(100, 5, 1, WriteOperationType.UPSERT, false));
    // non-empty source, error table union enabled, BULK_INSERT
    b.add(Arguments.of(100, 5, 1, WriteOperationType.BULK_INSERT, true));
    // non-empty source, error table union disabled, BULK_INSERT
    b.add(Arguments.of(100, 5, 1, WriteOperationType.BULK_INSERT, false));
    return b.build();
  }

  @ParameterizedTest
  @MethodSource("testErrorTableWriteFlowArgs")
  void testErrorTableWriteFlow(
      int totalRecords,
      int numErrorRecords,
      int numSourceFiles,
      WriteOperationType wopType,
      boolean writeErrorTableInParallel) throws Exception {
    this.withErrorTable = true;
    this.writeErrorTableInParallelWithBaseTable = writeErrorTableInParallel;
    this.writeOperationType = wopType;
    this.useSchemaProvider = false;
    this.useTransformer = false;
    this.tableType = "COPY_ON_WRITE";
    this.shouldCluster = false;
    this.shouldCompact = false;
    this.rowWriterEnable = false;
    this.addFilegroups = false;
    this.multiLogFiles = false;
    this.dfsSourceLimitBytes = 100000000; // set source limit to 100mb
    testBase(Tuple3.of(totalRecords, numErrorRecords, numSourceFiles));
  }
}