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

package org.apache.hudi.utilities.transform;

import org.apache.hudi.common.config.TypedProperties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.UUID;

/**
 * A transformer that allows a sql-query template be used to transform the source before writing to Hudi data-set.
 * <p>
 * The query should reference the source as a table named "\<SRC\>"
 */
public class SqlQueryBasedTransformer implements Transformer {

  private static final Logger LOG = LogManager.getLogger(SqlQueryBasedTransformer.class);

  private static final String SRC_PATTERN = "<SRC>";
  private static final String TMP_TABLE = "HOODIE_SRC_TMP_TABLE_";

  /**
   * Configs supported.
   */
  static class Config {

    private static final String TRANSFORMER_SQL = "hoodie.deltastreamer.transformer.sql";

    /**
     * This is a flag indicating whether to replace the wildcard in SQL with all the column names.
     * <p>
     * This is very useful when the user wants to derive one or more columns from the existing columns, and the
     * existing columns are all needed, to avoid writing all the column names into SQL. it would be very useful when
     * we have dozens or hundreds of columns in our SQL.
     * <p>
     * For example:
     * let's say we have already "id","name","age","ts" these four columns in our Dataset, the "ts" is in millisecond
     * long format, we want to add a new column named dt in yyyyMMdd format as our partition column.
     * That means use
     * "select *, ROM_UNIXTIME(ts / 1000, 'yyyyMMdd') as dt from <SRC>"
     * to represent
     * "select id, name, age, ts, FROM_UNIXTIME(ts / 1000, 'yyyyMMdd') as dt from <SRC>"
     * <p>
     * Note: when using this feature, the wildcard to place should be put in the first place following "select".
     */
    private static final String TRANSFORMER_SQL_REPLACE_WILDCARD = "hoodie.deltastreamer.transformer.sql.replace.wildcard";
  }

  @Override
  public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset,
      TypedProperties properties) {
    String transformerSQL = properties.getString(Config.TRANSFORMER_SQL);
    if (null == transformerSQL) {
      throw new IllegalArgumentException("Missing configuration : (" + Config.TRANSFORMER_SQL + ")");
    }

    // tmp table name doesn't like dashes
    String tmpTable = TMP_TABLE.concat(UUID.randomUUID().toString().replace("-", "_"));
    LOG.info("Registering tmp table : " + tmpTable);
    rowDataset.registerTempTable(tmpTable);
    String sqlStr = transformerSQL.replaceAll(SRC_PATTERN, tmpTable);

    // replace wildcard if needed
    boolean replaceWildcard = properties.getBoolean(Config.TRANSFORMER_SQL_REPLACE_WILDCARD, false);
    if (replaceWildcard) {
      String columns = getColumns(rowDataset);
      sqlStr = sqlStr.replace("*", columns);
    }

    LOG.debug("SQL Query for transformation : (" + sqlStr + ")");
    return sparkSession.sql(sqlStr);
  }

  private String getColumns(Dataset<Row> rowDataset) {
    StringBuilder sb = new StringBuilder();
    for (String column : rowDataset.columns()) {
      sb.append(column).append(", ");
    }
    sb.delete(sb.length() - 1, sb.length());
    return sb.toString();
  }
}
