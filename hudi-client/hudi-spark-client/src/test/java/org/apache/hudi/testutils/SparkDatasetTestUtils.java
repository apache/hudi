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

package org.apache.hudi.testutils;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import scala.collection.JavaConversions;
import scala.collection.JavaConverters;

import static org.apache.hudi.common.testutils.FileSystemTestUtils.RANDOM;

/**
 * Dataset test utils.
 */
public class SparkDatasetTestUtils {

  public static final StructType STRUCT_TYPE = new StructType(new StructField[] {
      new StructField(HoodieRecord.COMMIT_TIME_METADATA_FIELD, DataTypes.StringType, false, Metadata.empty()),
      new StructField(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, DataTypes.StringType, false, Metadata.empty()),
      new StructField(HoodieRecord.RECORD_KEY_METADATA_FIELD, DataTypes.StringType, false, Metadata.empty()),
      new StructField(HoodieRecord.PARTITION_PATH_METADATA_FIELD, DataTypes.StringType, false, Metadata.empty()),
      new StructField(HoodieRecord.FILENAME_METADATA_FIELD, DataTypes.StringType, false, Metadata.empty()),
      new StructField("randomInt", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("randomLong", DataTypes.LongType, false, Metadata.empty())});

  public static final StructType ERROR_STRUCT_TYPE = new StructType(new StructField[] {
      new StructField(HoodieRecord.COMMIT_TIME_METADATA_FIELD, DataTypes.StringType, false, Metadata.empty()),
      new StructField(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, DataTypes.LongType, false, Metadata.empty()),
      new StructField(HoodieRecord.RECORD_KEY_METADATA_FIELD, DataTypes.StringType, false, Metadata.empty()),
      new StructField(HoodieRecord.PARTITION_PATH_METADATA_FIELD, DataTypes.StringType, false, Metadata.empty()),
      new StructField(HoodieRecord.FILENAME_METADATA_FIELD, DataTypes.StringType, false, Metadata.empty()),
      new StructField("randomInt", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("randomStr", DataTypes.StringType, false, Metadata.empty())});

  public static final ExpressionEncoder ENCODER = getEncoder(STRUCT_TYPE);
  public static final ExpressionEncoder ERROR_ENCODER = getEncoder(ERROR_STRUCT_TYPE);

  /**
   * Generate Encode for the passed in {@link StructType}.
   *
   * @param schema instance of {@link StructType} for which encoder is requested.
   * @return the encoder thus generated.
   */
  private static ExpressionEncoder getEncoder(StructType schema) {
    List<Attribute> attributes = JavaConversions.asJavaCollection(schema.toAttributes()).stream()
        .map(Attribute::toAttribute).collect(Collectors.toList());
    return RowEncoder.apply(schema)
        .resolveAndBind(JavaConverters.asScalaBufferConverter(attributes).asScala().toSeq(),
            SimpleAnalyzer$.MODULE$);
  }

  /**
   * Generate random Rows.
   *
   * @param count total number of Rows to be generated.
   * @param partitionPath partition path to be set
   * @return the Dataset<Row>s thus generated.
   */
  public static Dataset<Row> getRandomRows(SQLContext sqlContext, int count, String partitionPath, boolean isError) {
    List<Row> records = new ArrayList<>();
    for (long recordNum = 0; recordNum < count; recordNum++) {
      records.add(getRandomValue(partitionPath, isError));
    }
    return sqlContext.createDataFrame(records, isError ? ERROR_STRUCT_TYPE : STRUCT_TYPE);
  }

  /**
   * Generate random Row.
   *
   * @param partitionPath partition path to be set in the Row.
   * @return the Row thus generated.
   */
  public static Row getRandomValue(String partitionPath, boolean isError) {
    // order commit time, seq no, record key, partition path, file name
    Object[] values = new Object[7];
    values[0] = ""; //commit time
    if (!isError) {
      values[1] = ""; // commit seq no
    } else {
      values[1] = RANDOM.nextLong();
    }
    values[2] = UUID.randomUUID().toString();
    values[3] = partitionPath;
    values[4] = ""; // filename
    values[5] = RANDOM.nextInt();
    if (!isError) {
      values[6] = RANDOM.nextLong();
    } else {
      values[6] = UUID.randomUUID().toString();
    }
    return new GenericRow(values);
  }

  /**
   * Convert Dataset<Row>s to List of {@link InternalRow}s.
   *
   * @param rows Dataset<Row>s to be converted
   * @return the List of {@link InternalRow}s thus converted.
   */
  public static List<InternalRow> toInternalRows(Dataset<Row> rows, ExpressionEncoder encoder) {
    List<InternalRow> toReturn = new ArrayList<>();
    List<Row> rowList = rows.collectAsList();
    for (Row row : rowList) {
      toReturn.add(encoder.toRow(row).copy());
    }
    return toReturn;
  }

  public static InternalRow getInternalRowWithError(String partitionPath) {
    // order commit time, seq no, record key, partition path, file name
    String recordKey = UUID.randomUUID().toString();
    Object[] values = new Object[7];
    values[0] = "";
    values[1] = "";
    values[2] = recordKey;
    values[3] = partitionPath;
    values[4] = "";
    values[5] = RANDOM.nextInt();
    values[6] = RANDOM.nextBoolean();
    return new GenericInternalRow(values);
  }

  public static HoodieWriteConfig.Builder getConfigBuilder(String basePath) {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withDeleteParallelism(2)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().compactionSmallFileSize(1024 * 1024).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(1024 * 1024).parquetMaxFileSize(1024 * 1024).build())
        .forTable("test-trip-table")
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withBulkInsertParallelism(2);
  }

}
