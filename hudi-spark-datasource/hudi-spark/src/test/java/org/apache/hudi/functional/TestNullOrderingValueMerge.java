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

package org.apache.hudi.functional;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Commit-time merges preserve nullable ordering columns. Event-time records preserve null until
 * ingestion or a comparison requires a valid ordering value, at which point the operation fails.
 */
class TestNullOrderingValueMerge {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() {
    spark = SparkSession.builder()
        .appName("null-ordering-merge-1x")
        .master("local[2]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  private static final StructType SCHEMA = new StructType()
      .add("id", DataTypes.StringType, false)
      .add("part", DataTypes.StringType, false)
      .add("ts", DataTypes.LongType, true)
      .add("value", DataTypes.StringType, false);

  static Stream<Arguments> cases() {
    // record type x merge mode x null-case. Record type is forced via the record merger.
    List<Arguments> args = new ArrayList<>();
    for (String recordType : new String[] {"AVRO", "SPARK"}) {
      for (String mergeMode : new String[] {"EVENT_TIME_ORDERING", "COMMIT_TIME_ORDERING"}) {
        args.add(Arguments.of(recordType, mergeMode, "base-null", null, 100L));
        args.add(Arguments.of(recordType, mergeMode, "incoming-null", 100L, null));
        args.add(Arguments.of(recordType, mergeMode, "both-null", null, null));
      }
    }
    return args.stream();
  }

  @ParameterizedTest(name = "{0} / {1} / {2}")
  @MethodSource("cases")
  void nullOrderingValueMerge(String recordType, String mergeMode, String caseName, Long baseTs, Long incomingTs,
                              @TempDir Path tmp) {
    boolean expectWriteReject = "AVRO".equals(recordType)
        && "EVENT_TIME_ORDERING".equals(mergeMode)
        && incomingTs == null;
    boolean expectMergeFailure = "EVENT_TIME_ORDERING".equals(mergeMode);
    String path = tmp.resolve(recordType + "_" + mergeMode + "_" + caseName).toString();

    writeRow(path, recordType, mergeMode, "insert", SaveMode.Overwrite, baseTs, "base");

    if (expectWriteReject) {
      Throwable thrown = assertThrows(Exception.class,
          () -> writeRow(path, recordType, mergeMode, "upsert", SaveMode.Append, incomingTs, "incoming"));
      assertTrue(rootMessage(thrown).contains("has null value for record key"),
          "expected null-ordering write rejection, got: " + rootMessage(thrown));
      return;
    }
    if (expectMergeFailure) {
      Throwable thrown = assertThrows(Exception.class,
          () -> writeRow(path, recordType, mergeMode, "upsert", SaveMode.Append, incomingTs, "incoming"));
      assertTrue(rootCause(thrown) instanceof NullPointerException,
          "expected null-ordering comparison failure, got: " + rootCause(thrown));
      return;
    }

    writeRow(path, recordType, mergeMode, "upsert", SaveMode.Append, incomingTs, "incoming");
    List<Row> rows = spark.read().format("hudi").load(path)
        .select("id", "ts", "value").where("id = 'k1'").collectAsList();

    assertEquals(1, rows.size(), "expected exactly one record for key k1");
    Row row = rows.get(0);
    assertEquals("incoming", row.getAs("value"));
    int tsIdx = row.fieldIndex("ts");
    if (incomingTs == null) {
      assertTrue(row.isNullAt(tsIdx), "ts should remain NULL, the default sentinel must not be materialized");
    } else {
      assertEquals(incomingTs.longValue(), row.getLong(tsIdx));
    }
  }

  private static String rootMessage(Throwable t) {
    return rootCause(t).getMessage() == null ? "" : rootCause(t).getMessage();
  }

  private static Throwable rootCause(Throwable t) {
    while (t.getCause() != null && t.getCause() != t) {
      t = t.getCause();
    }
    return t;
  }

  private void writeRow(String path, String recordType, String mergeMode, String operation, SaveMode mode, Long ts, String value) {
    Dataset<Row> df = spark.createDataFrame(
        Arrays.asList(RowFactory.create("k1", "p1", ts, value)), SCHEMA);
    Map<String, String> opts = new HashMap<>();
    opts.put("hoodie.table.name", "null_ordering_t");
    opts.put("hoodie.datasource.write.recordkey.field", "id");
    opts.put("hoodie.datasource.write.partitionpath.field", "part");
    opts.put("hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.SimpleKeyGenerator");
    opts.put("hoodie.datasource.write.table.type", "COPY_ON_WRITE");
    opts.put("hoodie.datasource.write.hive_style_partitioning", "true");
    opts.put("hoodie.metadata.enable", "false");
    opts.put("hoodie.record.merge.mode", mergeMode);
    opts.put("hoodie.datasource.write.operation", operation);
    // Force the record type via the record merger: default (unset) -> AVRO; DefaultSparkRecordMerger -> SPARK.
    if ("SPARK".equals(recordType)) {
      opts.put("hoodie.write.record.merge.custom.implementation.classes", "org.apache.hudi.DefaultSparkRecordMerger");
    }
    // Commit-time ordering does not use a precombine field; setting one makes the writer infer
    // event-time ordering and then reject the explicit commit-time merge mode.
    if ("EVENT_TIME_ORDERING".equals(mergeMode)) {
      opts.put("hoodie.datasource.write.precombine.field", "ts");
    }
    df.write().format("hudi").options(opts).mode(mode).save(path);
  }
}
