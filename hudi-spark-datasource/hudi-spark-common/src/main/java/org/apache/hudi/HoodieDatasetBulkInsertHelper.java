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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.BuiltinKeyGenerator;
import org.apache.hudi.keygen.ComplexKeyGenerator;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.BulkInsertPartitioner;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.collection.JavaConverters;

import static org.apache.spark.sql.functions.callUDF;

/**
 * Helper class to assist in preparing {@link Dataset<Row>}s for bulk insert with datasource implementation.
 */
public class HoodieDatasetBulkInsertHelper {

  private static final Logger LOG = LogManager.getLogger(HoodieDatasetBulkInsertHelper.class);

  private static final String RECORD_KEY_UDF_FN = "hudi_recordkey_gen_function_";
  private static final String PARTITION_PATH_UDF_FN = "hudi_partition_gen_function_";

  /**
   * Prepares input hoodie spark dataset for bulk insert. It does the following steps.
   * 1. Uses KeyGenerator to generate hoodie record keys and partition path.
   * 2. Add hoodie columns to input spark dataset.
   * 3. Reorders input dataset columns so that hoodie columns appear in the beginning.
   * 4. Sorts input dataset by hoodie partition path and record key
   *
   * @param sqlContext SQL Context
   * @param config     Hoodie Write Config
   * @param rows       Spark Input dataset
   * @return hoodie dataset which is ready for bulk insert.
   */
  public static Dataset<Row> prepareHoodieDatasetForBulkInsert(SQLContext sqlContext,
                                                               HoodieWriteConfig config, Dataset<Row> rows, String structName, String recordNamespace,
                                                               BulkInsertPartitioner<Dataset<Row>> bulkInsertPartitionerRows,
                                                               boolean isGlobalIndex, boolean dropPartitionColumns) {
    List<Column> originalFields =
        Arrays.stream(rows.schema().fields()).map(f -> new Column(f.name())).collect(Collectors.toList());

    TypedProperties properties = new TypedProperties();
    properties.putAll(config.getProps());
    String keyGeneratorClass = properties.getString(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME().key());
    String recordKeyFields = properties.getString(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key());
    String partitionPathFields = properties.containsKey(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key())
        ? properties.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key()) : "";
    BuiltinKeyGenerator keyGenerator = (BuiltinKeyGenerator) ReflectionUtils.loadClass(keyGeneratorClass, properties);

    Dataset<Row> rowDatasetWithRecordKeysAndPartitionPath;
    if (keyGeneratorClass.equals(NonpartitionedKeyGenerator.class.getName())) {
      // for non partitioned, set partition path to empty.
      rowDatasetWithRecordKeysAndPartitionPath = rows.withColumn(HoodieRecord.RECORD_KEY_METADATA_FIELD, functions.col(recordKeyFields))
          .withColumn(HoodieRecord.PARTITION_PATH_METADATA_FIELD, functions.lit("").cast(DataTypes.StringType));
    } else if (keyGeneratorClass.equals(SimpleKeyGenerator.class.getName())
        || (keyGeneratorClass.equals(ComplexKeyGenerator.class.getName()) && !recordKeyFields.contains(",") && !partitionPathFields.contains(",")
        && (!partitionPathFields.contains("timestamp")))) { // incase of ComplexKeyGen, check partition path type.
      // simple fields for both record key and partition path: can directly use withColumn
      String partitionPathField = keyGeneratorClass.equals(SimpleKeyGenerator.class.getName()) ? partitionPathFields :
          partitionPathFields.substring(partitionPathFields.indexOf(":") + 1);
      rowDatasetWithRecordKeysAndPartitionPath = rows.withColumn(HoodieRecord.RECORD_KEY_METADATA_FIELD, functions.col(recordKeyFields).cast(DataTypes.StringType))
          .withColumn(HoodieRecord.PARTITION_PATH_METADATA_FIELD, functions.col(partitionPathField).cast(DataTypes.StringType));
    } else {
      // use udf
      String tableName = properties.getString(HoodieWriteConfig.TBL_NAME.key());
      String recordKeyUdfFn = RECORD_KEY_UDF_FN + tableName;
      String partitionPathUdfFn = PARTITION_PATH_UDF_FN + tableName;
      sqlContext.udf().register(recordKeyUdfFn, (UDF1<Row, String>) keyGenerator::getRecordKey, DataTypes.StringType);
      sqlContext.udf().register(partitionPathUdfFn, (UDF1<Row, String>) keyGenerator::getPartitionPath, DataTypes.StringType);

      final Dataset<Row> rowDatasetWithRecordKeys = rows.withColumn(HoodieRecord.RECORD_KEY_METADATA_FIELD,
          callUDF(recordKeyUdfFn, org.apache.spark.sql.functions.struct(
              JavaConverters.collectionAsScalaIterableConverter(originalFields).asScala().toSeq())));
      rowDatasetWithRecordKeysAndPartitionPath =
          rowDatasetWithRecordKeys.withColumn(HoodieRecord.PARTITION_PATH_METADATA_FIELD,
              callUDF(partitionPathUdfFn,
                  org.apache.spark.sql.functions.struct(
                      JavaConverters.collectionAsScalaIterableConverter(originalFields).asScala().toSeq())));
    }

    // Add other empty hoodie fields which will be populated before writing to parquet.
    Dataset<Row> rowDatasetWithHoodieColumns =
        rowDatasetWithRecordKeysAndPartitionPath.withColumn(HoodieRecord.COMMIT_TIME_METADATA_FIELD,
                functions.lit("").cast(DataTypes.StringType))
            .withColumn(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD,
                functions.lit("").cast(DataTypes.StringType))
            .withColumn(HoodieRecord.FILENAME_METADATA_FIELD,
                functions.lit("").cast(DataTypes.StringType));

    Dataset<Row> processedDf = rowDatasetWithHoodieColumns;
    if (dropPartitionColumns) {
      String partitionColumns = String.join(",", keyGenerator.getPartitionPathFields());
      for (String partitionField : keyGenerator.getPartitionPathFields()) {
        originalFields.remove(new Column(partitionField));
      }
      processedDf = rowDatasetWithHoodieColumns.drop(partitionColumns);
    }
    Dataset<Row> dedupedDf = processedDf;
    if (config.shouldCombineBeforeInsert()) {
      dedupedDf = SparkRowWriteHelper.newInstance().deduplicateRows(processedDf, config.getPreCombineField(), isGlobalIndex);
    }

    List<Column> orderedFields = Stream.concat(HoodieRecord.HOODIE_META_COLUMNS.stream().map(Column::new),
        originalFields.stream()).collect(Collectors.toList());
    Dataset<Row> colOrderedDataset = dedupedDf.select(
        JavaConverters.collectionAsScalaIterableConverter(orderedFields).asScala().toSeq());

    return bulkInsertPartitionerRows.repartitionRecords(colOrderedDataset, config.getBulkInsertShuffleParallelism());
  }

  /**
   * Add empty meta fields and reorder such that meta fields are at the beginning.
   *
   * @param rows
   * @return
   */
  public static Dataset<Row> prepareHoodieDatasetForBulkInsertWithoutMetaFields(Dataset<Row> rows,
                                                                                HoodieWriteConfig writeConfig,
                                                                                BulkInsertPartitioner<Dataset<Row>> bulkInsertPartitionerRows) {
    // add empty meta cols.
    Dataset<Row> rowsWithMetaCols = rows
        .withColumn(HoodieRecord.COMMIT_TIME_METADATA_FIELD,
            functions.lit("").cast(DataTypes.StringType))
        .withColumn(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD,
            functions.lit("").cast(DataTypes.StringType))
        .withColumn(HoodieRecord.RECORD_KEY_METADATA_FIELD,
            functions.lit("").cast(DataTypes.StringType))
        .withColumn(HoodieRecord.PARTITION_PATH_METADATA_FIELD,
            functions.lit("").cast(DataTypes.StringType))
        .withColumn(HoodieRecord.FILENAME_METADATA_FIELD,
            functions.lit("").cast(DataTypes.StringType));

    List<Column> originalFields =
        Arrays.stream(rowsWithMetaCols.schema().fields())
            .filter(field -> !HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION.contains(field.name()))
            .map(f -> new Column(f.name())).collect(Collectors.toList());

    List<Column> metaFields =
        Arrays.stream(rowsWithMetaCols.schema().fields())
            .filter(field -> HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION.contains(field.name()))
            .map(f -> new Column(f.name())).collect(Collectors.toList());

    // reorder such that all meta columns are at the beginning followed by original columns
    List<Column> allCols = new ArrayList<>();
    allCols.addAll(metaFields);
    allCols.addAll(originalFields);

    return bulkInsertPartitionerRows.repartitionRecords(rowsWithMetaCols.select(
        JavaConverters.collectionAsScalaIterableConverter(allCols).asScala().toSeq()), writeConfig.getBulkInsertShuffleParallelism());
  }

}
