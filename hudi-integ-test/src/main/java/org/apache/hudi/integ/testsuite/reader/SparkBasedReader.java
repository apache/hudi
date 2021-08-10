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

package org.apache.hudi.integ.testsuite.reader;

import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.schema.RowBasedSchemaProvider;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import scala.collection.JavaConverters;


/**
 * Helper class to read avro and/or parquet files and generate a RDD of {@link GenericRecord}.
 */
public class SparkBasedReader {

  public static final String SPARK_AVRO_FORMAT = "avro";
  public static final String SPARK_PARQUET_FORMAT = "com.databricks.spark.parquet";
  private static final String AVRO_SCHEMA_OPTION_KEY = "avroSchema";

  // Spark anyways globs the path and gets all the paths in memory so take the List<filePaths> as an argument.
  // https://github.com/apache/spark/.../org/apache/spark/sql/execution/datasources/DataSource.scala#L251
  public static JavaRDD<GenericRecord> readAvro(SparkSession sparkSession, String schemaStr, List<String> listOfPaths,
      Option<String> structName, Option<String> nameSpace) {

    Dataset<Row> dataSet = sparkSession.read()
        .format(SPARK_AVRO_FORMAT)
        .option(AVRO_SCHEMA_OPTION_KEY, schemaStr)
        .load(JavaConverters.asScalaIteratorConverter(listOfPaths.iterator()).asScala().toSeq());

    return HoodieSparkUtils
        .createRdd(dataSet.toDF(), structName.orElse(RowBasedSchemaProvider.HOODIE_RECORD_STRUCT_NAME),
            nameSpace.orElse(RowBasedSchemaProvider.HOODIE_RECORD_NAMESPACE), false, Option.empty())
        .toJavaRDD();
  }

  public static JavaRDD<GenericRecord> readParquet(SparkSession sparkSession, List<String>
      listOfPaths, Option<String> structName, Option<String> nameSpace) {

    Dataset<Row> dataSet = sparkSession.read()
        .parquet((JavaConverters.asScalaIteratorConverter(listOfPaths.iterator()).asScala().toSeq()));

    return HoodieSparkUtils
        .createRdd(dataSet.toDF(), structName.orElse(RowBasedSchemaProvider.HOODIE_RECORD_STRUCT_NAME),
            RowBasedSchemaProvider.HOODIE_RECORD_NAMESPACE, false, Option.empty())
        .toJavaRDD();
  }

  public static JavaRDD<GenericRecord> readOrc(SparkSession sparkSession, List<String>
      listOfPaths, Option<String> structName, Option<String> nameSpace) {

    Dataset<Row> dataSet = sparkSession.read()
        .orc((JavaConverters.asScalaIteratorConverter(listOfPaths.iterator()).asScala().toSeq()));

    return HoodieSparkUtils.createRdd(dataSet.toDF(),
        structName.orElse(RowBasedSchemaProvider.HOODIE_RECORD_STRUCT_NAME),
        RowBasedSchemaProvider.HOODIE_RECORD_NAMESPACE,
        false, Option.empty()
    ).toJavaRDD();
  }

}
