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

package org.apache.hudi.utilities.sources.debezium;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.config.HoodieSchemaProviderConfig;
import org.apache.hudi.utilities.config.KafkaSourceConfig;
import org.apache.hudi.utilities.deser.KafkaAvroSchemaDeserializer;
import org.apache.hudi.utilities.exception.HoodieReadFromSourceException;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.schema.SchemaRegistryProvider;
import org.apache.hudi.utilities.sources.RowSource;
import org.apache.hudi.utilities.sources.helpers.AvroConvertor;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.CheckpointUtils;
import org.apache.hudi.utilities.sources.helpers.KafkaSourceUtil;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.utilities.config.KafkaSourceConfig.KAFKA_AVRO_VALUE_DESERIALIZER_CLASS;

/**
 * Base class for Debezium streaming source which expects change events as Kafka Avro records.
 * Obtains the schema from the confluent schema-registry.
 */
public abstract class DebeziumSource extends RowSource {

  private static final Logger LOG = LoggerFactory.getLogger(DebeziumSource.class);
  // these are native kafka's config. do not change the config names.
  private static final String NATIVE_KAFKA_KEY_DESERIALIZER_PROP = "key.deserializer";
  private static final String NATIVE_KAFKA_VALUE_DESERIALIZER_PROP = "value.deserializer";
  private static final String OVERRIDE_CHECKPOINT_STRING = "hoodie.debezium.override.initial.checkpoint.key";
  private static final String CONNECT_NAME_KEY = "connect.name";
  private static final String DATE_CONNECT_NAME = "custom.debezium.DateString";

  private final KafkaOffsetGen offsetGen;
  private final HoodieIngestionMetrics metrics;
  private final SchemaProvider schemaProvider;
  private final String deserializerClassName;

  public DebeziumSource(TypedProperties props, JavaSparkContext sparkContext,
                        SparkSession sparkSession,
                        SchemaProvider schemaProvider,
                        HoodieIngestionMetrics metrics) {
    super(props, sparkContext, sparkSession, schemaProvider);

    props.put(NATIVE_KAFKA_KEY_DESERIALIZER_PROP, StringDeserializer.class.getName());
    deserializerClassName = getStringWithAltKeys(props, KAFKA_AVRO_VALUE_DESERIALIZER_CLASS, true);

    try {
      props.put(NATIVE_KAFKA_VALUE_DESERIALIZER_PROP, Class.forName(deserializerClassName).getName());
    } catch (ClassNotFoundException e) {
      String error = "Could not load custom avro kafka deserializer: " + deserializerClassName;
      LOG.error(error);
      throw new HoodieReadFromSourceException(error, e);
    }

    if (schemaProvider == null) {
      this.schemaProvider = new SchemaRegistryProvider(props, sparkContext);
    } else {
      this.schemaProvider = schemaProvider;
    }

    if (deserializerClassName.equals(KafkaAvroSchemaDeserializer.class.getName())) {
      KafkaSourceUtil.configureSchemaDeserializer(this.schemaProvider, props);
    }

    offsetGen = new KafkaOffsetGen(props);
    this.metrics = metrics;
  }

  @Override
  protected Pair<Option<Dataset<Row>>, Checkpoint> fetchNextBatch(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
    String overrideCheckpointStr = props.getString(OVERRIDE_CHECKPOINT_STRING, "");

    OffsetRange[] offsetRanges = offsetGen.getNextOffsetRanges(lastCheckpoint, sourceLimit, metrics);
    long totalNewMsgs = CheckpointUtils.totalNewMessages(offsetRanges);
    LOG.info("About to read " + totalNewMsgs + " from Kafka for topic :" + offsetGen.getTopicName());

    try {
      String schemaStr = schemaProvider.getSourceHoodieSchema().getAvroSchema().toString();
      Dataset<Row> dataset = toDataset(offsetRanges, offsetGen, schemaStr);
      LOG.info("Spark schema of Kafka Payload for topic {}:\n{}", offsetGen.getTopicName(), dataset.schema().treeString());
      LOG.info("New checkpoint string: {}", CheckpointUtils.offsetsToStr(offsetRanges));
      return Pair.of(Option.of(dataset),
              new StreamerCheckpointV2(overrideCheckpointStr.isEmpty() ? CheckpointUtils.offsetsToStr(offsetRanges) : overrideCheckpointStr));
    } catch (Exception e) {
      LOG.error("Fatal error reading and parsing incoming debezium event", e);
      throw new HoodieReadFromSourceException("Fatal error reading and parsing incoming debezium event", e);
    }
  }

  /**
   * Debezium Kafka Payload has a nested structure, flatten it specific to the Database type.
   * @param rawKafkaData Dataset of the Debezium CDC event from the kafka
   * @return A flattened dataset.
   */
  protected abstract Dataset<Row> processDataset(Dataset<Row> rawKafkaData);

  /**
   * Converts a Kafka Topic offset into a Spark dataset.
   *
   * @param offsetRanges Offset ranges
   * @param offsetGen    KafkaOffsetGen
   * @return Spark dataset
   */
  private Dataset<Row> toDataset(OffsetRange[] offsetRanges, KafkaOffsetGen offsetGen, String schemaStr) {
    AvroConvertor convertor = new AvroConvertor(schemaStr);
    Dataset<Row> kafkaData;
    if (deserializerClassName.equals(StringDeserializer.class.getName())) {
      kafkaData = AvroConversionUtils.createDataFrame(
          KafkaUtils.<String, String>createRDD(sparkContext, offsetGen.getKafkaParams(), offsetRanges, LocationStrategies.PreferConsistent())
              .map(obj -> convertor.fromJson(obj.value()))
              .rdd(), schemaStr, sparkSession);
    } else {
      kafkaData = AvroConversionUtils.createDataFrame(
          KafkaUtils.createRDD(sparkContext, offsetGen.getKafkaParams(), offsetRanges, LocationStrategies.PreferConsistent())
              .map(obj -> (GenericRecord) obj.value())
              .rdd(), schemaStr, sparkSession);
    }

    // Flatten debezium payload, specific to each DB type (postgres/ mysql/ etc..)
    Dataset<Row> debeziumDataset = processDataset(kafkaData);

    // Some required transformations to ensure debezium data types are converted to spark supported types.
    return convertArrayColumnsToString(convertColumnToNullable(sparkSession,
        convertDateColumns(debeziumDataset, new Schema.Parser().parse(schemaStr))));
  }

  /**
   * Converts string formatted date columns into Spark date columns.
   *
   * @param dataset Spark dataset
   * @param schema  Avro schema from Debezium
   * @return Converted dataset
   */
  public static Dataset<Row> convertDateColumns(Dataset<Row> dataset, Schema schema) {
    if (schema.getField("before") != null) {
      List<String> dateFields = schema.getField("before")
          .schema()
          .getTypes()
          .get(1)
          .getFields()
          .stream()
          .filter(field -> {
            if (field.schema().getType() == Type.UNION) {
              return field.schema().getTypes().stream().anyMatch(
                  schemaInUnion -> DATE_CONNECT_NAME.equals(schemaInUnion.getProp(CONNECT_NAME_KEY))
              );
            } else {
              return DATE_CONNECT_NAME.equals(field.schema().getProp(CONNECT_NAME_KEY));
            }
          }).map(Field::name).collect(Collectors.toList());

      LOG.info("Date fields: {}", dateFields);

      for (String dateCol : dateFields) {
        dataset = dataset.withColumn(dateCol, functions.col(dateCol).cast(DataTypes.DateType));
      }
    }

    return dataset;
  }

  /**
   * Utility function for converting columns to nullable. This is useful when required to make a column nullable to match a nullable column from Debezium change
   * events.
   *
   * @param sparkSession SparkSession object
   * @param dataset      Dataframe to modify
   * @return Modified dataframe
   */
  private static Dataset<Row> convertColumnToNullable(SparkSession sparkSession, Dataset<Row> dataset) {
    List<String> columns = Arrays.asList(dataset.columns());
    StructField[] modifiedStructFields = Arrays.stream(dataset.schema().fields()).map(field -> columns
        .contains(field.name()) ? new StructField(field.name(), field.dataType(), true, field.metadata()) : field)
        .toArray(StructField[]::new);

    return sparkSession.createDataFrame(dataset.rdd(), new StructType(modifiedStructFields));
  }

  /**
   * Converts Array types to String types because not all Debezium array columns are supported to be converted
   * to Spark array columns.
   *
   * @param dataset Dataframe to modify
   * @return Modified dataframe
   */
  private static Dataset<Row> convertArrayColumnsToString(Dataset<Row> dataset) {
    List<String> arrayColumns = Arrays.stream(dataset.schema().fields())
        .filter(field -> field.dataType().typeName().toLowerCase().startsWith("array"))
        .map(StructField::name)
        .collect(Collectors.toList());

    for (String colName : arrayColumns) {
      dataset = dataset.withColumn(colName, functions.col(colName).cast(DataTypes.StringType));
    }

    return dataset;
  }

  @Override
  public void onCommit(String lastCkptStr) {
    if (getBooleanWithAltKeys(this.props, KafkaSourceConfig.ENABLE_KAFKA_COMMIT_OFFSET)) {
      offsetGen.commitOffsetToKafka(lastCkptStr);
    }
  }
}

