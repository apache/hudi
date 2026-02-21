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

package org.apache.hudi;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class HoodieSparkSQLUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieSparkSQLUtils.class);

  /**
   * Returns basepath of Hudi table given the table name.
   *
   * @param jsc java spark context
   * @param fullTableName this is full table name including database.
   */
  public static String getBasePathFromTableName(JavaSparkContext jsc, String fullTableName) {
    SparkSession sparkSession = SparkSession.builder().enableHiveSupport().sparkContext(jsc.sc()).getOrCreate();
    return getBasePathFromTableName(sparkSession, fullTableName);
  }

  /**
   * Returns basepath of Hudi table given the table name.
   *
   * @param sparkSession spark session.
   * @param fullTableName this is full table name including database.
   */
  public static String getBasePathFromTableName(SparkSession sparkSession, String fullTableName) {
    try {
      return sparkSession.sql(String.format("desc formatted %s", fullTableName))
          .collectAsList()
          .stream()
          .filter(row -> "Location".equals(row.getString(0)))
          .map(v -> v.getString(1))
          .findFirst()
          .get();
    } catch (Exception e) {
      throw new HoodieException(String.format("Failed to fetch basepath for: %s", fullTableName), e);
    }
  }

  public static List<Pair<String, String>> loadHoodiePathsFromHive(SparkSession sparkSession, String database,
                                                                    boolean filterHudiDatasets) {
    return loadHoodiePathsFromHive(sparkSession, database, filterHudiDatasets, "");
  }

  /**
   * Get valid hoodie base path by checking hive registered tables.
   *
   * @param sparkSession sparkSession
   * @param database hive database to check
   * @param filterHudiDatasets decides whether to consider only hudi datasets.
   * @return List of a pair of (tablename, basepath)
   */
  public static List<Pair<String, String>> loadHoodiePathsFromHive(SparkSession sparkSession, String database,
                                                                    boolean filterHudiDatasets, String tableNamePrefix) {
    Dataset<Row> tables = sparkSession.sql("SHOW TABLES FROM " + database);
    List<Row> tableRows = !StringUtils.isNullOrEmpty(tableNamePrefix)
        ? tables.filter(tables.col("tableName").startsWith(tableNamePrefix)).collectAsList()
        : tables.collectAsList();
    List<Pair<String, String>> validHoodiePaths = new ArrayList<>();
    tableRows.forEach(tableRow -> {
      String fullTableName = null;
      try {
        fullTableName = tableRow.getString(0) + "." + tableRow.getString(1);
        LOG.info("Table name {}", fullTableName);
        List<Row> rows = sparkSession.sql(String.format("desc formatted %s", fullTableName))
            .collectAsList()
            .stream()
            .filter(row -> "Location".equals(row.getString(0)) || "InputFormat".equals(row.getString(0)))
            .collect(Collectors.toList());
        boolean skipDataset = false;
        if (filterHudiDatasets) {
          skipDataset = rows.stream().noneMatch(row -> "InputFormat".equals(row.getString(0))
              && row.getString(1).startsWith("Hoodie"));
        }
        if (!skipDataset) {
          String basepath = rows.stream().filter(row -> "Location".equals(row.getString(0)))
              .map(v -> v.getString(1))
              .findFirst()
              .get();
          validHoodiePaths.add(Pair.of(fullTableName, basepath));
        }
      } catch (Exception e) {
        LOG.error("Exception in fetching basepath for table {}", fullTableName, e);
      }
    });
    return validHoodiePaths;
  }
}
