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

import static org.apache.spark.sql.functions.callUDF;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.KeyGenerator;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

/**
 * Helper class to assist in preparing {@link Dataset<Row>}s for bulk insert with datasource implementation.
 */
public class HoodieDatasetBulkInsertHelper {

  private static final Logger LOG = LogManager.getLogger(HoodieDatasetBulkInsertHelper.class);

  private static final String RECORD_KEY_UDF_FN = "hudi_recordkey_gen_function";
  private static final String PARTITION_PATH_UDF_FN = "hudi_partition_gen_function";

  /**
   * Prepares input hoodie spark dataset for bulk insert. It does the following steps.
   *  1. Uses KeyGenerator to generate hoodie record keys and partition path.
   *  2. Add hoodie columns to input spark dataset.
   *  3. Reorders input dataset columns so that hoodie columns appear in the beginning.
   *  4. Sorts input dataset by hoodie partition path and record key
   *
   * @param sqlContext SQL Context
   * @param config  Hoodie Write Config
   * @param rows    Spark Input dataset
   * @return hoodie dataset which is ready for bulk insert.
   */
  public static Dataset<Row> prepareHoodieDatasetForBulkInsert(SQLContext sqlContext,
      HoodieWriteConfig config, Dataset<Row> rows, String structName, String recordNamespace) {
    List<Column> originalFields =
        Arrays.stream(rows.schema().fields()).map(f -> new Column(f.name())).collect(Collectors.toList());

    TypedProperties properties = new TypedProperties();
    properties.putAll(config.getProps());
    String keyGeneratorClass = properties.getString(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY());
    KeyGenerator keyGenerator = (KeyGenerator) ReflectionUtils.loadClass(keyGeneratorClass, properties);
    StructType structTypeForUDF = rows.schema();

    sqlContext.udf().register(RECORD_KEY_UDF_FN, (UDF1<Row, String>) keyGenerator::getRecordKey, DataTypes.StringType);
    sqlContext.udf().register(PARTITION_PATH_UDF_FN, (UDF1<Row, String>) keyGenerator::getPartitionPath, DataTypes.StringType);

    final Dataset<Row> rowDatasetWithRecordKeys = rows.withColumn(HoodieRecord.RECORD_KEY_METADATA_FIELD,
        callUDF(RECORD_KEY_UDF_FN, org.apache.spark.sql.functions.struct(
            JavaConverters.collectionAsScalaIterableConverter(originalFields).asScala().toSeq())));

    final Dataset<Row> rowDatasetWithRecordKeysAndPartitionPath =
        rowDatasetWithRecordKeys.withColumn(HoodieRecord.PARTITION_PATH_METADATA_FIELD,
            callUDF(PARTITION_PATH_UDF_FN,
                org.apache.spark.sql.functions.struct(
                    JavaConverters.collectionAsScalaIterableConverter(originalFields).asScala().toSeq())));

    // Add other empty hoodie fields which will be populated before writing to parquet.
    Dataset<Row> rowDatasetWithHoodieColumns =
        rowDatasetWithRecordKeysAndPartitionPath.withColumn(HoodieRecord.COMMIT_TIME_METADATA_FIELD,
            functions.lit("").cast(DataTypes.StringType))
            .withColumn(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD,
                functions.lit("").cast(DataTypes.StringType))
            .withColumn(HoodieRecord.FILENAME_METADATA_FIELD,
                functions.lit("").cast(DataTypes.StringType));
    List<Column> orderedFields = Stream.concat(HoodieRecord.HOODIE_META_COLUMNS.stream().map(Column::new),
        originalFields.stream()).collect(Collectors.toList());
    Dataset<Row> colOrderedDataset = rowDatasetWithHoodieColumns.select(
        JavaConverters.collectionAsScalaIterableConverter(orderedFields).asScala().toSeq());

    return colOrderedDataset
        .sort(functions.col(HoodieRecord.PARTITION_PATH_METADATA_FIELD), functions.col(HoodieRecord.RECORD_KEY_METADATA_FIELD))
        .coalesce(config.getBulkInsertShuffleParallelism());
  }
}
