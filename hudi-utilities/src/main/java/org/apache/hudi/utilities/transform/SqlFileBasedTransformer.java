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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Scanner;
import java.util.UUID;

/**
 * A transformer that allows a sql template file be used to transform the source before writing to
 * Hudi data-set.
 *
 * <p>The query should reference the source as a table named "\<SRC\>"
 *
 * <p>The final sql statement result is used as the write payload.
 *
 * <p>The SQL file is configured with this hoodie property:
 * hoodie.deltastreamer.transformer.sql.file
 *
 * <p>Example Spark SQL Query:
 *
 * <p>CACHE TABLE tmp_personal_trips AS
 * SELECT * FROM <SRC> WHERE trip_type='personal_trips';
 * <p>
 * SELECT * FROM tmp_personal_trips;
 */
public class SqlFileBasedTransformer implements Transformer {

  private static final Logger LOG = LogManager.getLogger(SqlFileBasedTransformer.class);

  private static final String SRC_PATTERN = "<SRC>";
  private static final String TMP_TABLE = "HOODIE_SRC_TMP_TABLE_";

  @Override
  public Dataset<Row> apply(
      final JavaSparkContext jsc,
      final SparkSession sparkSession,
      final Dataset<Row> rowDataset,
      final TypedProperties props) {

    final String sqlFile = props.getString(Config.TRANSFORMER_SQL_FILE);
    if (null == sqlFile) {
      throw new IllegalArgumentException(
          "Missing required configuration : (" + Config.TRANSFORMER_SQL_FILE + ")");
    }

    final FileSystem fs = FSUtils.getFs(sqlFile, jsc.hadoopConfiguration(), true);
    // tmp table name doesn't like dashes
    final String tmpTable = TMP_TABLE.concat(UUID.randomUUID().toString().replace("-", "_"));
    LOG.info("Registering tmp table : " + tmpTable);
    rowDataset.createOrReplaceTempView(tmpTable);

    try (final Scanner scanner = new Scanner(fs.open(new Path(sqlFile)), "UTF-8")) {
      Dataset<Row> rows = null;
      // each sql statement is separated with semicolon hence set that as delimiter.
      scanner.useDelimiter(";");
      LOG.info("SQL Query for transformation : ");
      while (scanner.hasNext()) {
        String sqlStr = scanner.next();
        sqlStr = sqlStr.replaceAll(SRC_PATTERN, tmpTable).trim();
        if (!sqlStr.isEmpty()) {
          LOG.info(sqlStr);
          // overwrite the same dataset object until the last statement then return.
          rows = sparkSession.sql(sqlStr);
        }
      }
      return rows;
    } catch (final IOException ioe) {
      throw new HoodieIOException("Error reading transformer SQL file.", ioe);
    } finally {
      sparkSession.catalog().dropTempView(tmpTable);
    }
  }

  /** Configs supported. */
  private static class Config {

    private static final String TRANSFORMER_SQL_FILE = "hoodie.deltastreamer.transformer.sql.file";
  }
}
