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
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.UUID;

/**
 * Transformer that can flatten nested objects. It currently doesn't unnest arrays.
 */
public class FlatteningTransformer implements Transformer {

  private static final String TMP_TABLE = "HUDI_SRC_TMP_TABLE_";
  private static final Logger LOG = LogManager.getLogger(FlatteningTransformer.class);

  /**
   * Configs supported.
   */
  @Override
  public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset,
      TypedProperties properties) {

    // tmp table name doesn't like dashes
    String tmpTable = TMP_TABLE.concat(UUID.randomUUID().toString().replace("-", "_"));
    LOG.info("Registering tmp table : " + tmpTable);
    rowDataset.createOrReplaceTempView(tmpTable);
    Dataset<Row> transformed = sparkSession.sql("select " + flattenSchema(rowDataset.schema(), null) + " from " + tmpTable);
    sparkSession.catalog().dropTempView(tmpTable);
    return transformed;
  }

  public String flattenSchema(StructType schema, String prefix) {
    final StringBuilder selectSQLQuery = new StringBuilder();

    for (StructField field : schema.fields()) {
      final String fieldName = field.name();

      // it is also possible to expand arrays by using Spark "expand" function.
      // As it can increase data size significantly we later pass additional property with a
      // list of arrays to expand.
      final String colName = prefix == null ? fieldName : (prefix + "." + fieldName);
      if (field.dataType().getClass().equals(StructType.class)) {
        selectSQLQuery.append(flattenSchema((StructType) field.dataType(), colName));
      } else {
        selectSQLQuery.append(colName);
        selectSQLQuery.append(" as ");
        selectSQLQuery.append(colName.replace(".", "_"));
      }

      selectSQLQuery.append(",");
    }

    if (selectSQLQuery.length() > 0) {
      selectSQLQuery.deleteCharAt(selectSQLQuery.length() - 1);
    }

    return selectSQLQuery.toString();
  }
}
