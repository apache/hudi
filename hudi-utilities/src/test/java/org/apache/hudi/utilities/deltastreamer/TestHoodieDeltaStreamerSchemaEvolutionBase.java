/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.AvroKafkaSource;
import org.apache.hudi.utilities.sources.ParquetDFSSource;
import org.apache.hudi.utilities.streamer.HoodieStreamer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.apache.hudi.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_NAMESPACE;
import static org.apache.hudi.utilities.schema.RowBasedSchemaProvider.HOODIE_RECORD_STRUCT_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Add test cases for out of the box schema evolution for deltastreamer:
 * https://hudi.apache.org/docs/schema_evolution#out-of-the-box-schema-evolution
 */
public class TestHoodieDeltaStreamerSchemaEvolutionBase extends HoodieDeltaStreamerTestBase {

  protected static Set<String> createdTopicNames = new HashSet<>();

  protected String tableType;
  protected String tableBasePath;
  protected Boolean shouldCluster;
  protected Boolean shouldCompact;
  protected Boolean rowWriterEnable;
  protected Boolean addFilegroups;
  protected Boolean multiLogFiles;
  protected Boolean useSchemaProvider;
  protected Boolean hasTransformer;
  protected String sourceSchemaFile;
  protected String targetSchemaFile;
  protected boolean useKafkaSource;
  protected boolean useTransformer;
  protected boolean userProvidedSchema;

  @BeforeAll
  public static void initKafka() {
    defaultSchemaProviderClassName = TestSchemaProvider.class.getName();
  }

  @BeforeEach
  public void setupTest() {
    super.setupTest();
    useSchemaProvider = false;
    hasTransformer = false;
    sourceSchemaFile = "";
    targetSchemaFile = "";
    topicName = "topic" + testNum;
  }

  @AfterEach
  public void teardown() throws Exception {
    super.teardown();
    TestSchemaProvider.resetTargetSchema();
  }

  @AfterAll
  static void teardownAll() {
    defaultSchemaProviderClassName = FilebasedSchemaProvider.class.getName();
    HoodieDeltaStreamerTestBase.cleanupKafkaTestUtils();
  }

  protected HoodieStreamer deltaStreamer;

  protected HoodieDeltaStreamer.Config getDeltaStreamerConfig() throws IOException {
    return getDeltaStreamerConfig(true);
  }

  protected HoodieDeltaStreamer.Config getDeltaStreamerConfig(boolean nullForDeletedCols) throws IOException {
    String[] transformerClasses = useTransformer ? new String[] {TestHoodieDeltaStreamer.TestIdentityTransformer.class.getName()}
        : new String[0];
    return getDeltaStreamerConfig(transformerClasses, nullForDeletedCols);
  }

  protected HoodieDeltaStreamer.Config getDeltaStreamerConfig(String[] transformerClasses, boolean nullForDeletedCols) throws IOException {
    return getDeltaStreamerConfig(transformerClasses, nullForDeletedCols, new TypedProperties());
  }

  protected HoodieDeltaStreamer.Config getDeltaStreamerConfig(String[] transformerClasses, boolean nullForDeletedCols,
                                                              TypedProperties extraProps) throws IOException {
    extraProps.setProperty("hoodie.datasource.write.table.type", tableType);
    extraProps.setProperty("hoodie.datasource.write.row.writer.enable", rowWriterEnable.toString());
    extraProps.setProperty(DataSourceWriteOptions.SET_NULL_FOR_MISSING_COLUMNS().key(), Boolean.toString(nullForDeletedCols));

    //we set to 0 so that we create new base files on insert instead of adding inserts to existing filegroups via small file handling
    extraProps.setProperty("hoodie.parquet.small.file.limit", "0");

    //We only want compaction/clustering to kick in after the final commit. This is because after compaction/clustering we have base files again
    //and adding to base files is already covered by the tests. This is important especially for mor, because we want to see how compaction/clustering
    //behaves when schema evolution is happening in the log files
    int maxCommits = 2;
    if (addFilegroups) {
      maxCommits++;
    }
    if (multiLogFiles) {
      maxCommits++;
    }

    extraProps.setProperty(HoodieCompactionConfig.INLINE_COMPACT.key(), shouldCompact.toString());
    if (shouldCompact) {
      extraProps.setProperty(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), Integer.toString(maxCommits));
    }

    if (shouldCluster) {
      extraProps.setProperty(HoodieClusteringConfig.INLINE_CLUSTERING.key(), "true");
      extraProps.setProperty(HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), Integer.toString(maxCommits));
      extraProps.setProperty(HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS.key(), "_row_key");
    }

    List<String> transformerClassNames = new ArrayList<>();
    Collections.addAll(transformerClassNames, transformerClasses);

    HoodieDeltaStreamer.Config cfg;
    if (useKafkaSource) {
      prepareAvroKafkaDFSSource(PROPS_FILENAME_TEST_AVRO_KAFKA, null, topicName,"partition_path", extraProps);
      cfg = TestHoodieDeltaStreamer.TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT, AvroKafkaSource.class.getName(),
          transformerClassNames, PROPS_FILENAME_TEST_AVRO_KAFKA, false,  useSchemaProvider, 100000, false, null, tableType, "timestamp", null);
    } else {
      prepareParquetDFSSource(false, hasTransformer, sourceSchemaFile, targetSchemaFile, PROPS_FILENAME_TEST_PARQUET,
          PARQUET_SOURCE_ROOT, false, "partition_path", "", extraProps);
      cfg = TestHoodieDeltaStreamer.TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT, ParquetDFSSource.class.getName(),
          transformerClassNames, PROPS_FILENAME_TEST_PARQUET, false,
          useSchemaProvider, 100000, false, null, tableType, "timestamp", null);
    }
    cfg.forceDisableCompaction = !shouldCompact;
    return cfg;
  }

  protected void addData(Dataset<Row> df, Boolean isFirst) {
    if (useSchemaProvider) {
      TestSchemaProvider.sourceSchema = AvroConversionUtils.convertStructTypeToAvroSchema(df.schema(), HOODIE_RECORD_STRUCT_NAME, HOODIE_RECORD_NAMESPACE);
    }
    if (useKafkaSource) {
      addKafkaData(df, isFirst);
    } else {
      addParquetData(df, isFirst);
    }
  }

  protected void addParquetData(Dataset<Row> df, Boolean isFirst) {
    df.write().format("parquet").mode(isFirst ? SaveMode.Overwrite : SaveMode.Append).save(PARQUET_SOURCE_ROOT);
  }

  protected void addKafkaData(Dataset<Row> df, Boolean isFirst) {
    if (isFirst && !createdTopicNames.contains(topicName)) {
      testUtils.createTopic(topicName);
      createdTopicNames.add(topicName);
    }
    List<GenericRecord> records = HoodieSparkUtils.createRdd(df, HOODIE_RECORD_STRUCT_NAME, HOODIE_RECORD_NAMESPACE, false, Option.empty()).toJavaRDD().collect();
    try (Producer<String, byte[]> producer = new KafkaProducer<>(getProducerProperties())) {
      for (GenericRecord record : records) {
        producer.send(new ProducerRecord<>(topicName, 0, "key", HoodieAvroUtils.avroToBytes(record)));
      }
    }
  }

  protected Properties getProducerProperties() {
    Properties props = new Properties();
    props.put("bootstrap.servers", testUtils.brokerAddress());
    props.put("value.serializer", ByteArraySerializer.class.getName());
    props.put("value.deserializer", ByteArraySerializer.class.getName());
    // Key serializer is required.
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("auto.register.schemas", "false");
    // wait for all in-sync replicas to ack sends
    props.put("acks", "all");
    return props;
  }

  /**
   * see how many files are read from in the latest commit. This verification is for making sure the test scenarios
   * are setup as expected, rather than testing schema evolution functionality
   */
  protected void assertFileNumber(int expected, boolean isCow) {
    if (isCow) {
      assertBaseFileOnlyNumber(expected);
    } else {
      //we can't differentiate between _hoodie_file_name for log files, so we use commit time as the differentiator between them
      assertEquals(expected, sparkSession.read().format("hudi").load(tableBasePath).select("_hoodie_commit_time", "_hoodie_file_name").distinct().count());
    }
  }

  /**
   * Base files might have multiple different commit times in the same file. To ensure this is only used when there are only base files
   * there is a check that every file ends with .parquet, as log files don't in _hoodie_file_name
   */
  protected void assertBaseFileOnlyNumber(int expected) {
    Dataset<Row> df = sparkSession.read().format("hudi").load(tableBasePath).select("_hoodie_file_name");
    df.createOrReplaceTempView("assertFileNumberPostCompactCluster");
    assertEquals(df.count(), sparkSession.sql("select * from assertFileNumberPostCompactCluster where _hoodie_file_name like '%.parquet'").count());
    assertEquals(expected, df.distinct().count());
  }

  protected void assertRecordCount(int expected) {
    sqlContext.clearCache();
    long recordCount = sqlContext.read().format("org.apache.hudi").load(tableBasePath).count();
    assertEquals(expected, recordCount);
  }

  protected StructType createFareStruct(DataType amountType) {
    return createFareStruct(amountType, false);
  }

  protected StructType createFareStruct(DataType amountType, Boolean dropCols) {
    if (dropCols) {
      return DataTypes.createStructType(new StructField[]{new StructField("amount", amountType, true, Metadata.empty())});
    }
    return DataTypes.createStructType(new StructField[]{new StructField("amount", amountType, true, Metadata.empty()),
        new StructField("currency", DataTypes.StringType, true, Metadata.empty())});
  }

  public static class TestSchemaProvider extends SchemaProvider {

    public static Schema sourceSchema;
    public static Schema targetSchema = null;

    public TestSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
      super(props, jssc);
    }

    @Override
    public Schema getSourceSchema() {
      return sourceSchema;
    }

    @Override
    public Schema getTargetSchema() {
      return targetSchema != null ? targetSchema : sourceSchema;
    }

    public static void setTargetSchema(Schema targetSchema) {
      TestSchemaProvider.targetSchema = targetSchema;
    }

    public static void resetTargetSchema() {
      TestSchemaProvider.targetSchema = null;
    }
  }
}
