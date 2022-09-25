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

package org.apache.hudi.cli.utils;

import org.apache.hudi.exception.HoodieException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.stream.Collectors;

public class SparkTempViewProvider implements TempViewProvider {

  private static final Logger LOG = LogManager.getLogger(SparkTempViewProvider.class);

  private JavaSparkContext jsc;
  private SQLContext sqlContext;

  public SparkTempViewProvider(String appName) {
    try {
      SparkConf sparkConf = new SparkConf().setAppName(appName)
              .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local[8]");
      jsc = new JavaSparkContext(sparkConf);
      sqlContext = new SQLContext(jsc);
    } catch (Throwable ex) {
      // log full stack trace and rethrow. Without this its difficult to debug failures, if any
      LOG.warn("unable to initialize spark context ", ex);
      throw new HoodieException(ex);
    }
  }

  public SparkTempViewProvider(JavaSparkContext jsc, SQLContext sqlContext) {
    this.jsc = jsc;
    this.sqlContext = sqlContext;
  }

  @Override
  public void createOrReplace(String tableName, List<String> headers, List<List<Comparable>> rows) {
    try {
      if (headers.isEmpty() || rows.isEmpty()) {
        return;
      }

      if (rows.stream().filter(row -> row.size() != headers.size()).count() > 0) {
        throw new HoodieException("Invalid row, does not match headers " + headers.size() + " " + rows.size());
      }

      // replace all whitespaces in headers to make it easy to write sql queries
      List<String> headersNoSpaces = headers.stream().map(title -> title.replaceAll("\\s+",""))
              .collect(Collectors.toList());

      // generate schema for table
      StructType structType = new StructType();
      for (int i = 0; i < headersNoSpaces.size(); i++) {
        // try guessing data type from column data.
        DataType headerDataType = getDataType(rows.get(0).get(i));
        structType = structType.add(DataTypes.createStructField(headersNoSpaces.get(i), headerDataType, true));
      }
      List<Row> records = rows.stream().map(row -> RowFactory.create(row.toArray(new Comparable[row.size()])))
              .collect(Collectors.toList());
      Dataset<Row> dataset = this.sqlContext.createDataFrame(records, structType);
      dataset.createOrReplaceTempView(tableName);
      System.out.println("Wrote table view: " + tableName);
    } catch (Throwable ex) {
      // log full stack trace and rethrow. Without this its difficult to debug failures, if any
      LOG.warn("unable to write ", ex);
      throw new HoodieException(ex);
    }
  }

  @Override
  public void runQuery(String sqlText) {
    try {
      this.sqlContext.sql(sqlText).show(Integer.MAX_VALUE, false);
    } catch (Throwable ex) {
      // log full stack trace and rethrow. Without this its difficult to debug failures, if any
      LOG.warn("unable to read ", ex);
      throw new HoodieException(ex);
    }
  }

  @Override
  public void showAllViews() {
    try {
      sqlContext.sql("SHOW TABLES").show(Integer.MAX_VALUE, false);
    } catch (Throwable ex) {
      // log full stack trace and rethrow. Without this its difficult to debug failures, if any
      LOG.warn("unable to get all views ", ex);
      throw new HoodieException(ex);
    }
  }

  @Override
  public void deleteTable(String tableName) {
    try {
      sqlContext.sql("DROP TABLE IF EXISTS " + tableName);
    } catch (Throwable ex) {
      // log full stack trace and rethrow. Without this its difficult to debug failures, if any
      LOG.warn("unable to initialize spark context ", ex);
      throw new HoodieException(ex);
    }
  }

  @Override
  public void close() {
    if (sqlContext != null) {
      sqlContext.sparkSession().stop();
    }
  }

  private DataType getDataType(Comparable comparable) {
    if (comparable instanceof Integer) {
      return DataTypes.IntegerType;
    }

    if (comparable instanceof Double) {
      return DataTypes.DoubleType;
    }

    if (comparable instanceof Long) {
      return DataTypes.LongType;
    }

    if (comparable instanceof Boolean) {
      return DataTypes.BooleanType;
    }

    // TODO add additional types when needed. default to string
    return DataTypes.StringType;
  }
}
