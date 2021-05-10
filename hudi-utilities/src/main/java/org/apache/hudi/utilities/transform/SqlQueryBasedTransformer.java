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
 *
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
    LOG.debug("SQL Query for transformation : (" + sqlStr + ")");
    return sparkSession.sql(sqlStr);
  }
}
