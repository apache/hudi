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
import org.apache.hudi.utilities.config.SqlTransformerConfig;
import org.apache.hudi.utilities.exception.HoodieTransformExecutionException;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;

/**
 * A transformer that allows a sql-query template be used to transform the source before writing to Hudi data-set.
 *
 * The query should reference the source as a table named "\<SRC\>"
 */
public class SqlQueryBasedTransformer implements Transformer {

  private static final Logger LOG = LoggerFactory.getLogger(SqlQueryBasedTransformer.class);

  private static final String SRC_PATTERN = "<SRC>";
  private static final String TMP_TABLE = "HOODIE_SRC_TMP_TABLE_";

  @Override
  public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset,
      TypedProperties properties) {
    String transformerSQL = getStringWithAltKeys(properties, SqlTransformerConfig.TRANSFORMER_SQL);

    try {
      // tmp table name doesn't like dashes
      String tmpTable = TMP_TABLE.concat(UUID.randomUUID().toString().replace("-", "_"));
      LOG.info("Registering tmp table: {}", tmpTable);
      rowDataset.createOrReplaceTempView(tmpTable);
      String sqlStr = transformerSQL.replaceAll(SRC_PATTERN, tmpTable);
      LOG.debug("SQL Query for transformation: {}", sqlStr);
      Dataset<Row> transformed = sparkSession.sql(sqlStr);
      sparkSession.catalog().dropTempView(tmpTable);
      return transformed;
    } catch (Exception e) {
      throw new HoodieTransformExecutionException("Failed to apply sql query based transformer", e);
    }
  }
}
