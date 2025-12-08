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
import org.apache.hudi.TestHoodieSparkUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.AvroKafkaSource;
import org.apache.hudi.utilities.sources.ParquetDFSSource;
import org.apache.hudi.utilities.streamer.BaseErrorTableWriter;
import org.apache.hudi.utilities.streamer.HoodieStreamer;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
  protected String tableName;
  protected Boolean shouldCluster;
  protected Boolean shouldCompact;
  protected Boolean rowWriterEnable;
  protected Boolean addFilegroups;
  protected Boolean multiLogFiles;
  protected Boolean useSchemaProvider;
  protected Boolean hasTransformer;
  protected Boolean useParquetLogBlock;
  protected String sourceSchemaFile;
  protected String targetSchemaFile;
  protected boolean useKafkaSource;
  protected boolean withErrorTable;
  protected boolean useTransformer;
  protected boolean userProvidedSchema;
  protected boolean writeErrorTableInParallelWithBaseTable;
  protected int dfsSourceLimitBytes = 100000;
  protected WriteOperationType writeOperationType = WriteOperationType.UPSERT;

  @BeforeAll
  public static void initKafka() {
    defaultSchemaProviderClassName = TestSchemaProvider.class.getName();
  }

  @Override
  @BeforeEach
  public void setupTest() {
    super.setupTest();
    TestErrorTable.commited = new HashMap<>();
    TestErrorTable.errorEvents = new ArrayList<>();
    useSchemaProvider = false;
    hasTransformer = false;
    withErrorTable = false;
    useParquetLogBlock = false;
    sourceSchemaFile = "";
    targetSchemaFile = "";
    topicName = "topic" + testNum;
    sparkSession.conf().set("spark.sql.parquet.enableNestedColumnVectorizedReader", "false");
  }

  @AfterEach
  public void teardown() throws Exception {
    super.teardown();
    TestSchemaProvider.resetTargetSchema();
  }

  @AfterAll
  static void teardownAll() {
    defaultSchemaProviderClassName = FilebasedSchemaProvider.class.getName();
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
    extraProps.setProperty(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key(), useParquetLogBlock ? "parquet" : "avro");

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

    if (withErrorTable) {
      extraProps.setProperty(HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key(), "true");
      extraProps.setProperty(HoodieErrorTableConfig.ERROR_ENABLE_VALIDATE_TARGET_SCHEMA.key(), "true");
      extraProps.setProperty(HoodieErrorTableConfig.ERROR_ENABLE_VALIDATE_RECORD_CREATION.key(), "true");
      extraProps.setProperty(HoodieErrorTableConfig.ERROR_TARGET_TABLE.key(), tableName + "ERROR");
      extraProps.setProperty(HoodieErrorTableConfig.ERROR_TABLE_BASE_PATH.key(), basePath + tableName + "ERROR");
      extraProps.setProperty(HoodieErrorTableConfig.ERROR_TABLE_WRITE_CLASS.key(), TestErrorTable.class.getName());
      extraProps.setProperty("hoodie.base.path", tableBasePath);
      if (writeErrorTableInParallelWithBaseTable) {
        extraProps.setProperty(HoodieErrorTableConfig.ENABLE_ERROR_TABLE_WRITE_UNIFICATION.key(), "true");
      }
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
          PARQUET_SOURCE_ROOT, false, "partition_path", "", extraProps, false, false);
      cfg = TestHoodieDeltaStreamer.TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT, ParquetDFSSource.class.getName(),
          transformerClassNames, PROPS_FILENAME_TEST_PARQUET, false,
          useSchemaProvider, dfsSourceLimitBytes, false, null, tableType, "timestamp", null);
    }
    cfg.forceDisableCompaction = !shouldCompact;
    return cfg;
  }

  protected void addData(Dataset<Row> df, Boolean isFirst) {
    if (useSchemaProvider) {
      TestSchemaProvider.sourceSchema = HoodieSchema.fromAvroSchema(
          AvroConversionUtils.convertStructTypeToAvroSchema(df.schema(), HOODIE_RECORD_STRUCT_NAME, HOODIE_RECORD_NAMESPACE));
      if (withErrorTable && isFirst) {
        TestSchemaProvider.setTargetSchema(HoodieSchema.fromAvroSchema(
            AvroConversionUtils.convertStructTypeToAvroSchema(TestHoodieSparkUtils.getSchemaColumnNotNullable(df.schema(), "_row_key"),"idk", "idk")));
      }
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

    public static HoodieSchema sourceSchema;
    public static HoodieSchema targetSchema = null;

    public TestSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
      super(props, jssc);
    }

    @Override
    public HoodieSchema getSourceHoodieSchema() {
      return sourceSchema;
    }

    @Override
    public HoodieSchema getTargetHoodieSchema() {
      return targetSchema != null ? targetSchema : sourceSchema;
    }

    public static void setTargetSchema(HoodieSchema targetSchema) {
      TestSchemaProvider.targetSchema = targetSchema;
    }

    public static void resetTargetSchema() {
      TestSchemaProvider.targetSchema = null;
    }
  }

  public static class TestErrorTableV1 extends TestErrorTable {
    public TestErrorTableV1(HoodieStreamer.Config cfg,
                            SparkSession sparkSession,
                            TypedProperties props,
                            HoodieSparkEngineContext hoodieSparkContext,
                            FileSystem fs,
                            Option<HoodieIngestionMetrics> metrics) {
      super(cfg, sparkSession, props, hoodieSparkContext, fs);
    }
  }

  public static class TestErrorTable extends BaseErrorTableWriter {

    public static List<JavaRDD> errorEvents = new ArrayList<>();
    public static Map<String,Option<JavaRDD>> commited = new HashMap<>();
    // This instant time is only used for separate upsert and commit calls
    // to maintain the instant time for the error table
    private Option<String> errorTableInstantTime = Option.empty();

    public TestErrorTable(HoodieStreamer.Config cfg, SparkSession sparkSession, TypedProperties props, HoodieSparkEngineContext hoodieSparkContext,
                          FileSystem fileSystem) {
      super(cfg, sparkSession, props, hoodieSparkContext, fileSystem);
    }

    @Override
    public void addErrorEvents(JavaRDD errorEvent) {
      errorEvents.add(errorEvent);
    }

    @Override
    public boolean commit(JavaRDD writeStatuses) {
      if (writeStatuses == null) {
        throw new IllegalArgumentException("writeStatuses cannot be null");
      }
      if (this.errorTableInstantTime.isEmpty()) {
        return false;
      }
      commited.clear();
      commited.put(errorTableInstantTime.get(), Option.of(writeStatuses));
      return true;
    }

    @Override
    public JavaRDD<WriteStatus> upsert(String baseTableInstantTime, Option commitedInstantTime) {
      if (errorEvents.size() > 0) {
        if (errorTableInstantTime.isPresent()) {
          throw new IllegalStateException("Error table instant time should be empty before calling upsert");
        }
        errorTableInstantTime = Option.of(InProcessTimeGenerator.createNewInstantTime());
        JavaRDD errorsCombined = errorEvents.get(0);
        for (int i = 1; i < errorEvents.size(); i++) {
          errorsCombined = errorsCombined.union(errorEvents.get(i));
        }
        JavaRDD writeStatus = errorsCombined.mapPartitions(partition -> {
          Iterator itr = (Iterator) partition;
          long count = 0;
          while (itr.hasNext()) {
            itr.next();
            count++;
          }
          List<WriteStatus> result = new ArrayList<>();
          if (count > 0) {
            WriteStatus status = new WriteStatus();
            status.setTotalRecords(count);
            result.add(status);
          }
          return result.iterator();
        });
        errorEvents = new ArrayList<>();
        return writeStatus;
      }
      return null;
    }

    @Override
    public boolean upsertAndCommit(String baseTableInstantTime, Option commitedInstantTime) {
      if (errorEvents.size() > 0) {
        JavaRDD errorsCombined = errorEvents.get(0);
        for (int i = 1; i < errorEvents.size(); i++) {
          errorsCombined = errorsCombined.union(errorEvents.get(i));
        }
        commited.put(baseTableInstantTime, Option.of(errorsCombined));
        errorEvents = new ArrayList<>();

      } else {
        commited.put(baseTableInstantTime, Option.empty());
      }
      return true;
    }

    @Override
    public Option<JavaRDD<HoodieAvroIndexedRecord>> getErrorEvents(String baseTableInstantTime, Option commitedInstantTime) {
      return Option.empty();
    }
  }

}
