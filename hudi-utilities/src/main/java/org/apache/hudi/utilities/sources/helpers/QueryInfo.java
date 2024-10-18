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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.utilities.sources.SnapshotLoadQuerySplitter;

import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL;
import static org.apache.hudi.DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL;

/**
 * This class is used to prepare query information for s3 and gcs incr source.
 * Some of the information in this class is used for batching based on sourceLimit.
 * <p>
 * queryType: Incremental or Snapshot query on the hudi table
 * previousInstant: instant before startInstant.
 * startInstant: start instant for range query
 * endInstant: end instant for range query
 * predicateFilter: predicate filters on columns to prune partitions and files.
 * orderColumn: colum used for ordering results eg: _hoodie_record_key can be used.
 * keyColumn: column used for performing range query eg: _hoodie_commit_time > startInstant and _hoodie_commit_time <= endInstant
 * limitColumn: limits the numbers of rows returned by query
 * orderByColumns: (orderColumn, keyColumn)
 * </p>
 */
public class QueryInfo {
  private final String queryType;
  private final String previousInstant;
  private final String startInstant;
  private final String endInstant;
  private final String predicateFilter;
  private final String orderColumn;
  private final String keyColumn;
  private final String limitColumn;
  private final List<String> orderByColumns;

  public QueryInfo(
      String queryType, String previousInstant,
      String startInstant, String endInstant,
      String orderColumn, String keyColumn,
      String limitColumn) {
    this(
        queryType,
        previousInstant,
        startInstant,
        endInstant,
        StringUtils.EMPTY_STRING,
        orderColumn,
        keyColumn,
        limitColumn
    );
  }

  public QueryInfo(
      String queryType,
      String previousInstant,
      String startInstant,
      String endInstant,
      String predicateFilter,
      String orderColumn,
      String keyColumn,
      String limitColumn) {
    this.queryType = queryType;
    this.previousInstant = previousInstant;
    this.startInstant = startInstant;
    this.endInstant = endInstant;
    this.predicateFilter = predicateFilter;
    this.orderColumn = orderColumn;
    this.keyColumn = keyColumn;
    this.limitColumn = limitColumn;
    this.orderByColumns = Arrays.asList(orderColumn, keyColumn);
  }

  public boolean areStartAndEndInstantsEqual() {
    return getStartInstant().equals(getEndInstant());
  }

  public boolean isIncremental() {
    return QUERY_TYPE_INCREMENTAL_OPT_VAL().equals(queryType);
  }

  public boolean isSnapshot() {
    return QUERY_TYPE_SNAPSHOT_OPT_VAL().equals(queryType);
  }

  public String getQueryType() {
    return queryType;
  }

  public String getPreviousInstant() {
    return previousInstant;
  }

  public String getStartInstant() {
    return startInstant;
  }

  public String getEndInstant() {
    return endInstant;
  }

  public String getOrderColumn() {
    return orderColumn;
  }

  public String getKeyColumn() {
    return keyColumn;
  }

  public String getLimitColumn() {
    return limitColumn;
  }

  public List<String> getOrderByColumns() {
    return orderByColumns;
  }

  public Option<String> getPredicateFilter() {
    if (!StringUtils.isNullOrEmpty(predicateFilter)) {
      return Option.of(predicateFilter);
    }
    return Option.empty();
  }

  public QueryInfo withUpdatedEndInstant(String newEndInstant) {
    return new QueryInfo(
        this.queryType,
        this.previousInstant,
        this.startInstant,
        newEndInstant,
        this.orderColumn,
        this.keyColumn,
        this.limitColumn
    );
  }

  public QueryInfo withUpdatedCheckpoint(SnapshotLoadQuerySplitter.CheckpointWithPredicates checkpointWithPredicates) {
    return new QueryInfo(
        this.queryType,
        this.previousInstant,
        this.startInstant,
        checkpointWithPredicates.getEndCompletionTime(),
        checkpointWithPredicates.getPredicateFilter(),
        this.orderColumn,
        this.keyColumn,
        this.limitColumn
    );
  }

  @Override
  public String toString() {
    return ("Query information for Incremental Source "
        + "queryType: " + queryType
        + ", previousInstant: " + previousInstant
        + ", startInstant: " + startInstant
        + ", endInstant: " + endInstant
        + ", orderColumn: " + orderColumn
        + ", keyColumn: " + keyColumn
        + ", limitColumn: " + limitColumn
        + ", orderByColumns: " + orderByColumns);
  }
}