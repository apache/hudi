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

package org.apache.hudi.functional;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * An incremental query filters on {@code _hoodie_commit_time}, and that filter is pushed into the
 * file reader. The reader evaluates a pushed predicate against the schema it was asked to read, so
 * if the query's projection does not include the filtered column the predicate cannot be satisfied
 * and every row is dropped -- silently, with no error.
 *
 * <p>That made {@code count()} and any projected read return zero rows on a CoW incremental query
 * while {@code collect()} returned the right ones: {@code collect()} happens to project every
 * column, including the filtered one, so it worked only incidentally.
 *
 * <p>Each case here asserts the same query returns the same rows regardless of what it projects.
 */
public class TestIncrementalQueryProjection extends SparkClientFunctionalTestHarness {

  private static StructType schema() {
    return DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField("record_key", DataTypes.StringType, true),
        DataTypes.createStructField("partition_path", DataTypes.StringType, true),
        DataTypes.createStructField("payload", DataTypes.StringType, true)
    }).asNullable();
  }

  private Map<String, String> writeOptions() {
    Map<String, String> opts = new HashMap<>();
    opts.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "record_key");
    opts.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition_path");
    opts.put(DataSourceWriteOptions.ORDERING_FIELDS().key(), "payload");
    opts.put(HoodieTableConfig.NAME.key(), "test_incr_projection");
    opts.put(DataSourceWriteOptions.TABLE_TYPE().key(), "COPY_ON_WRITE");
    opts.put(HoodieMetadataConfig.ENABLE.key(), "false");
    opts.put(DataSourceWriteOptions.OPERATION().key(),
        DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL());
    return opts;
  }

  private void write(List<Row> rows, SaveMode mode) {
    spark().createDataset(rows,
            SparkAdapterSupport$.MODULE$.sparkAdapter().getCatalystExpressionUtils().getEncoder(schema()))
        .write().format("hudi").options(writeOptions()).mode(mode).save(basePath());
  }

  /** Writes two commits and returns their instant times, oldest first. */
  private List<String> writeTwoCommits() {
    write(Arrays.asList(
        RowFactory.create("k1", "p1", "v1"),
        RowFactory.create("k2", "p1", "v2")), SaveMode.Overwrite);
    write(Arrays.asList(
        RowFactory.create("k3", "p1", "v3"),
        RowFactory.create("k4", "p1", "v4")), SaveMode.Append);

    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setBasePath(basePath()).setConf(storageConf()).build();
    List<String> instants = new ArrayList<>();
    metaClient.getActiveTimeline().filterCompletedInstants().getInstants()
        .forEach(i -> instants.add(i.requestedTime()));
    Collections.sort(instants);
    return instants;
  }

  private Dataset<Row> incrementalFrom(String startCommit) {
    return spark().read().format("hudi")
        .option(DataSourceReadOptions.QUERY_TYPE().key(),
            DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
        .option(DataSourceReadOptions.START_COMMIT().key(), startCommit)
        .load(basePath());
  }

  @Test
  public void incrementalCountMatchesCollect() {
    List<String> instants = writeTwoCommits();
    Dataset<Row> incremental = incrementalFrom(instants.get(1));

    // count() projects no columns at all, which is what made it return 0.
    assertEquals(2, incremental.collectAsList().size(), "collect must return the second commit's rows");
    assertEquals(2, incremental.count(), "count must agree with collect");
  }

  @Test
  public void incrementalProjectedReadMatchesCollect() {
    List<String> instants = writeTwoCommits();
    Dataset<Row> incremental = incrementalFrom(instants.get(1));

    // Projecting a subset that excludes _hoodie_commit_time is the general form of the bug;
    // count() is just its most visible instance.
    assertEquals(2, incremental.select("record_key").collectAsList().size(),
        "a projected read must return the same rows as an unprojected one");
    assertEquals(2, incremental.select("record_key").count());
    assertEquals(2, incremental.select("record_key", "payload").collectAsList().size());
  }

  @Test
  public void incrementalProjectedReadReturnsTheRightRows() {
    List<String> instants = writeTwoCommits();

    List<String> keys = new ArrayList<>();
    incrementalFrom(instants.get(1)).select("record_key").collectAsList()
        .forEach(r -> keys.add(r.getString(0)));
    Collections.sort(keys);

    // Not just the right count -- the right rows. A filter that silently matched everything would
    // pass a count-only assertion.
    assertEquals(Arrays.asList("k3", "k4"), keys);
  }

  @Test
  public void snapshotCountIsUnaffected() {
    writeTwoCommits();

    // A snapshot read supplies no required filters, so the filter-column augmentation must be a
    // strict no-op here -- otherwise every count() in Hudi would start reading an extra column.
    Dataset<Row> snapshot = spark().read().format("hudi").load(basePath());
    assertEquals(4, snapshot.count());
    assertEquals(4, snapshot.collectAsList().size());
    assertEquals(4, snapshot.select("record_key").count());
  }
}
