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

package org.apache.hudi.bootstrap;

import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.client.bootstrap.FullRecordBootstrapDataProvider;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.SparkKeyGeneratorInterface;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.List;

public abstract class SparkFullBootstrapDataProviderBase extends FullRecordBootstrapDataProvider<JavaRDD<HoodieRecord>> {

  private final transient SparkSession sparkSession;

  public SparkFullBootstrapDataProviderBase(TypedProperties props,
                                            HoodieSparkEngineContext context) {
    super(props, context);
    this.sparkSession = SparkSession.builder().config(context.getJavaSparkContext().getConf()).getOrCreate();
  }

  @Override
  public JavaRDD<HoodieRecord> generateInputRecords(String tableName, String sourceBasePath,
                                                    List<Pair<String, List<HoodieFileStatus>>> partitionPathsWithFiles, HoodieWriteConfig config) {
    String[] filePaths = partitionPathsWithFiles.stream().map(Pair::getValue)
        .flatMap(f -> f.stream().map(fs -> HadoopFSUtils.toPath(fs.getPath()).toString()))
        .toArray(String[]::new);

    // NOTE: "basePath" option is required for spark to discover the partition column
    // More details at https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery
    HoodieRecordType recordType =  config.getRecordMerger().getRecordType();
    Dataset inputDataset = sparkSession.read().format(getFormat()).option("basePath", sourceBasePath).load(filePaths);
    KeyGenerator keyGenerator = HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);
    String orderingFieldsStr = ConfigUtils.getOrderingFieldsStrDuringWrite(props);
    String structName = tableName + "_record";
    String namespace = "hoodie." + tableName;
    if (recordType == HoodieRecordType.AVRO) {
      RDD<GenericRecord> genericRecords = HoodieSparkUtils.createRdd(inputDataset, structName, namespace, false,
          Option.empty());
      return genericRecords.toJavaRDD().map(gr -> {
        String orderingVal = HoodieAvroUtils.getNestedFieldValAsString(
            gr, orderingFieldsStr, false, props.getBoolean(
                KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
                Boolean.parseBoolean(KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue())));
        return HoodieRecordUtils.createHoodieRecord(gr, orderingVal, keyGenerator.getKey(gr), ConfigUtils.getPayloadClass(props));
      });
    } else if (recordType == HoodieRecordType.SPARK) {
      SparkKeyGeneratorInterface sparkKeyGenerator = (SparkKeyGeneratorInterface) keyGenerator;
      StructType structType = inputDataset.schema();
      return inputDataset.queryExecution().toRdd().toJavaRDD().map(internalRow -> {
        String recordKey = sparkKeyGenerator.getRecordKey(internalRow, structType).toString();
        String partitionPath = sparkKeyGenerator.getPartitionPath(internalRow, structType).toString();
        HoodieKey key = new HoodieKey(recordKey, partitionPath);
        return new HoodieSparkRecord(key, internalRow, structType, false);
      });
    } else {
      throw new UnsupportedOperationException(recordType.name());
    }
  }

  protected abstract String getFormat();
}
