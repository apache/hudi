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

package org.apache.hudi.utilities;

import org.apache.hudi.common.util.StringUtils;

/**
 * SQL query builder. Current support for: SELECT, FROM, JOIN, ON, WHERE, ORDER BY, LIMIT clauses.
 */
public class SqlQueryBuilder {

  private final StringBuilder sqlBuilder;

  private SqlQueryBuilder(StringBuilder sqlBuilder) {
    this.sqlBuilder = sqlBuilder;
  }

  /**
   * Creates a SELECT query.
   *
   * @param columns The column names to select.
   * @return The new {@link SqlQueryBuilder} instance.
   */
  public static SqlQueryBuilder select(String... columns) {
    if (columns == null || columns.length == 0) {
      throw new IllegalArgumentException("No columns provided with SELECT statement. Please mention column names or '*' to select all columns.");
    }
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append("select ");
    sqlBuilder.append(String.join(", ", columns));
    return new SqlQueryBuilder(sqlBuilder);
  }

  /**
   * Appends a FROM clause to a query.
   *
   * @param tables The table names to select from.
   * @return The {@link SqlQueryBuilder} instance.
   */
  public SqlQueryBuilder from(String... tables) {
    if (tables == null || tables.length == 0) {
      throw new IllegalArgumentException("No table name provided with FROM clause. Please provide a table name to select from.");
    }
    sqlBuilder.append(" from ");
    sqlBuilder.append(String.join(", ", tables));
    return this;
  }

  /**
   * Appends a JOIN clause to a query.
   *
   * @param table The table to join with.
   * @return The {@link SqlQueryBuilder} instance.
   */
  public SqlQueryBuilder join(String table) {
    if (StringUtils.isNullOrEmpty(table)) {
      throw new IllegalArgumentException("No table name provided with JOIN clause. Please provide a table name to join with.");
    }
    sqlBuilder.append(" join ");
    sqlBuilder.append(table);
    return this;
  }

  /**
   * Appends an ON clause to a query.
   *
   * @param predicate The predicate to join on.
   * @return The {@link SqlQueryBuilder} instance.
   */
  public SqlQueryBuilder on(String predicate) {
    if (StringUtils.isNullOrEmpty(predicate)) {
      throw new IllegalArgumentException();
    }
    sqlBuilder.append(" on ");
    sqlBuilder.append(predicate);
    return this;
  }

  /**
   * Appends a WHERE clause to a query.
   *
   * @param predicate The predicate for WHERE clause.
   * @return The {@link SqlQueryBuilder} instance.
   */
  public SqlQueryBuilder where(String predicate) {
    if (StringUtils.isNullOrEmpty(predicate)) {
      throw new IllegalArgumentException("No predicate provided with WHERE clause. Please provide a predicate to filter records.");
    }
    sqlBuilder.append(" where ");
    sqlBuilder.append(predicate);
    return this;
  }

  /**
   * Appends an ORDER BY clause to a query. By default, records are ordered in ascending order by the given column.
   * To order in descending order use DESC after the column name, e.g. queryBuilder.orderBy("update_time desc").
   *
   * @param columns Column names to order by.
   * @return The {@link SqlQueryBuilder} instance.
   */
  public SqlQueryBuilder orderBy(String... columns) {
    if (columns == null || columns.length == 0) {
      throw new IllegalArgumentException("No columns provided with ORDER BY clause. Please provide a column name to order records.");
    }
    sqlBuilder.append(" order by ");
    sqlBuilder.append(String.join(", ", columns));
    return this;
  }

  /**
   * Appends a "limit" clause to a query.
   *
   * @param count The limit count.
   * @return The {@link SqlQueryBuilder} instance.
   */
  public SqlQueryBuilder limit(long count) {
    if (count < 0) {
      throw new IllegalArgumentException("Please provide a positive integer for the LIMIT clause.");
    }
    sqlBuilder.append(" limit ");
    sqlBuilder.append(count);
    return this;
  }

  @Override
  public String toString() {
    return sqlBuilder.toString();
  }
}
