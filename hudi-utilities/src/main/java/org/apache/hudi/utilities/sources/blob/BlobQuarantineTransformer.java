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

package org.apache.hudi.utilities.sources.blob;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.collection.MappingIterator;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.keygen.SparkKeyGeneratorInterface;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.utilities.streamer.BaseErrorTableWriter;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The blob source's required transformer: quarantines the rows whose object was gone before its bytes could be read (a row with
 * no blob, {@link BlobRowBuilder#buildRow}) and passes every other row as built. It writes nothing: a file above the inline
 * threshold is an unmanaged reference to the object where it is, so no file is copied under the table.
 * A gone row carries its identity in one _corrupt_record column for ordinary transformer error routing; without an error table the
 * round fails before its instant starts, and its checkpoint is unchanged.
 * Output is persisted and materialized before returning, so each object's status call and inline read run once per round,
 * whether the error table splits the output or not. The next round releases the previous output after the preceding write has
 * consumed it. A round that ends before the transformer (an unchanged checkpoint) does not release it: the last output stays
 * cached until the next round that reaches the transformer, or until this transformer closes, so at most one round's output is held.
 */
public class BlobQuarantineTransformer implements Transformer {

  private static final String CORRUPT_RECORD = BaseErrorTableWriter.ERROR_TABLE_CURRUPT_RECORD_COL_NAME;
  private transient Dataset<Row> previousOutput;

  /** Releases the last successful round when the owning streamer stops or rebuilds. Safe to call repeatedly. */
  @Override
  public void close() {
    if (previousOutput != null) {
      previousOutput.unpersist(false);
      previousOutput = null;
    }
  }

  @Override
  public StructType transformedSchema(JavaSparkContext jsc, SparkSession spark, StructType schema, TypedProperties props) {
    return errorTableEnabled(props) ? withErrorColumn(schema) : dataSchema(schema);
  }

  @Override
  public Dataset<Row> apply(JavaSparkContext jsc, SparkSession spark, Dataset<Row> rows, TypedProperties props) {
    close();
    StructType inputSchema = rows.schema();
    String storageLevel = ConfigUtils.getStringWithAltKeys(props, HoodieWriteConfig.TAGGED_RECORD_STORAGE_LEVEL_VALUE, true);
    StructType outputSchema = withErrorColumn(inputSchema);
    int corruptIndex = outputSchema.fieldIndex(CORRUPT_RECORD);
    int blobIndex = inputSchema.fieldIndex(BlobTableSchema.BLOB);
    JavaRDD<Row> checked = rows.toJavaRDD().mapPartitions(partition -> {
      SparkKeyGeneratorInterface keyGenerator = (SparkKeyGeneratorInterface) HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);
      return new MappingIterator<>(partition, row -> {
        Object[] values = new Object[outputSchema.size()];
        for (int i = 0; i < row.size(); i++) {
          values[i] = row.get(i);
        }
        if (values[corruptIndex] == null && row.isNullAt(blobIndex)) {
          values[corruptIndex] = goneIdentity(row, inputSchema, keyGenerator);
        }
        return RowFactory.create(values);
      });
    });
    Dataset<Row> output = spark.createDataFrame(checked, outputSchema).persist(StorageLevel.fromString(storageLevel));
    // released on every failure path: a failed task surfaces from count() as Spark's checked SparkException
    boolean materialized = false;
    try {
      output.count();
      if (!errorTableEnabled(props)) {
        List<Row> failures = output.filter(functions.col(CORRUPT_RECORD).isNotNull())
            .select(CORRUPT_RECORD).limit(1).collectAsList();
        if (!failures.isEmpty()) {
          throw new HoodieException("Blob source row failed without an error table: " + failures.get(0).getString(0));
        }
      }
      materialized = true;
    } finally {
      if (!materialized) {
        output.unpersist(false);
      }
    }
    previousOutput = output;
    return errorTableEnabled(props) ? output : output.drop(CORRUPT_RECORD);
  }

  /**
   * The error-table identity of a row whose object was confirmed gone when the row was built: its record key and partition, from
   * the job's own key generator, and the object it names.
   */
  private static String goneIdentity(Row row, StructType schema, SparkKeyGeneratorInterface keyGenerator) {
    HoodieKey key = new HoodieKey(keyGenerator.getRecordKey(row), keyGenerator.getPartitionPath(row));
    Map<String, String> identity = new LinkedHashMap<>();
    identity.put("record_key", key.getRecordKey());
    identity.put("partition_path", key.getPartitionPath());
    identity.put("error", BlobTableSchema.GONE_SINCE_ANNOUNCED + ": no bytes were read when the row was prepared: source_path_sha "
        + row.getString(schema.fieldIndex(BlobTableSchema.SOURCE_PATH_SHA)) + " "
        + row.getString(schema.fieldIndex(BlobTableSchema.SOURCE_FILE_PATH)) + " "
        + row.getLong(schema.fieldIndex(BlobTableSchema.SOURCE_FILE_SIZE)));
    return JsonUtils.toString(identity);
  }

  private static boolean errorTableEnabled(TypedProperties props) {
    return ConfigUtils.getBooleanWithAltKeys(props, HoodieErrorTableConfig.ERROR_TABLE_ENABLED);
  }

  private static StructType dataSchema(StructType schema) {
    return new StructType(Arrays.stream(schema.fields())
        .filter(field -> !CORRUPT_RECORD.equals(field.name())).toArray(StructField[]::new));
  }

  private static StructType withErrorColumn(StructType schema) {
    if (!Arrays.asList(schema.fieldNames()).contains(CORRUPT_RECORD)) {
      return schema.add(DataTypes.createStructField(CORRUPT_RECORD, DataTypes.StringType, true));
    }
    return new StructType(Arrays.stream(schema.fields())
        .map(field -> CORRUPT_RECORD.equals(field.name())
            ? new StructField(field.name(), DataTypes.StringType, true, field.metadata()) : field)
        .toArray(StructField[]::new));
  }
}
