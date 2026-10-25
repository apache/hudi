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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.utilities.sources.SnapshotLoadQuerySplitter.CheckpointWithPredicates;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.hudi.DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL;
import static org.apache.hudi.DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the {@link QueryInfo} value object used by the cloud incremental sources.
 */
class TestQueryInfo {

  private static final String PREVIOUS_INSTANT = "20240101000000000";
  private static final String START_INSTANT = "20240101010000000";
  private static final String END_INSTANT = "20240101020000000";
  private static final String ORDER_COLUMN = "_hoodie_commit_time";
  private static final String KEY_COLUMN = "_hoodie_record_key";
  private static final String LIMIT_COLUMN = "s3.object.size";

  private static QueryInfo incrementalQueryInfo() {
    return new QueryInfo(QUERY_TYPE_INCREMENTAL_OPT_VAL(), PREVIOUS_INSTANT, START_INSTANT, END_INSTANT,
        ORDER_COLUMN, KEY_COLUMN, LIMIT_COLUMN);
  }

  @Test
  void incrementalQueryInfoHasNoPredicateFilter() {
    QueryInfo queryInfo = incrementalQueryInfo();

    assertEquals(QUERY_TYPE_INCREMENTAL_OPT_VAL(), queryInfo.getQueryType());
    assertEquals(PREVIOUS_INSTANT, queryInfo.getPreviousInstant());
    assertEquals(START_INSTANT, queryInfo.getStartInstant());
    assertEquals(END_INSTANT, queryInfo.getEndInstant());
    assertEquals(ORDER_COLUMN, queryInfo.getOrderColumn());
    assertEquals(KEY_COLUMN, queryInfo.getKeyColumn());
    assertEquals(LIMIT_COLUMN, queryInfo.getLimitColumn());
    assertEquals(Arrays.asList(ORDER_COLUMN, KEY_COLUMN), queryInfo.getOrderByColumns());

    // The 7-arg ctor defaults the predicate filter to the empty string, which reads back as absent.
    assertFalse(queryInfo.getPredicateFilter().isPresent());
    assertTrue(queryInfo.isIncremental());
    assertFalse(queryInfo.isSnapshot());
    assertFalse(queryInfo.areStartAndEndInstantsEqual());
  }

  @Test
  void snapshotQueryInfoExposesPredicateFilter() {
    QueryInfo queryInfo = new QueryInfo(QUERY_TYPE_SNAPSHOT_OPT_VAL(), PREVIOUS_INSTANT, START_INSTANT, START_INSTANT,
        "partition_path = 'a'", ORDER_COLUMN, KEY_COLUMN, LIMIT_COLUMN);

    assertTrue(queryInfo.isSnapshot());
    assertFalse(queryInfo.isIncremental());
    assertTrue(queryInfo.areStartAndEndInstantsEqual());
    assertEquals("partition_path = 'a'", queryInfo.getPredicateFilter().get());
  }

  @Test
  void withUpdatedEndInstantMovesTheEndInstantAndDropsThePredicateFilter() {
    QueryInfo queryInfo = new QueryInfo(QUERY_TYPE_INCREMENTAL_OPT_VAL(), PREVIOUS_INSTANT, START_INSTANT, END_INSTANT,
        "partition_path = 'a'", ORDER_COLUMN, KEY_COLUMN, LIMIT_COLUMN);
    assertTrue(queryInfo.getPredicateFilter().isPresent());

    QueryInfo updated = queryInfo.withUpdatedEndInstant("20240101030000000");

    assertEquals("20240101030000000", updated.getEndInstant());
    assertEquals(START_INSTANT, updated.getStartInstant());
    assertEquals(PREVIOUS_INSTANT, updated.getPreviousInstant());
    assertEquals(QUERY_TYPE_INCREMENTAL_OPT_VAL(), updated.getQueryType());
    // withUpdatedEndInstant routes through the 7-arg ctor, so any predicate filter is dropped
    assertFalse(updated.getPredicateFilter().isPresent());
  }

  @Test
  void withUpdatedCheckpointAppliesEndTimeAndPredicate() {
    CheckpointWithPredicates checkpoint = new CheckpointWithPredicates("20240101040000000", "partition_path > 'b'");
    assertEquals("20240101040000000", checkpoint.getEndCompletionTime());
    assertEquals("partition_path > 'b'", checkpoint.getPredicateFilter());

    QueryInfo updated = incrementalQueryInfo().withUpdatedCheckpoint(checkpoint);

    assertEquals("20240101040000000", updated.getEndInstant());
    assertEquals("partition_path > 'b'", updated.getPredicateFilter().get());
    assertEquals(START_INSTANT, updated.getStartInstant());
  }
}
