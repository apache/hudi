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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.schema.SchemaRegistryProvider;
import org.apache.hudi.utilities.sources.RowSource;
import org.apache.hudi.utilities.sources.helpers.AvroConvertor;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.CheckpointUtils;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
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

/**
 * Base class for Debezium streaming source which expects change events as Kafka Avro records.
 * Obtains the schema from the confluent schema-registry.
 */
public abstract class DebeziumSource extends RowSource {

  private static final Logger LOG = LogManager.getLogger(DebeziumSource.class);
  // these are native kafka's config. do not change the config names.
  private static final String NATIVE_KAFKA_KEY_DESERIALIZER_PROP = "key.deserializer";
  private static final String NATIVE_KAFKA_VALUE_DESERIALIZER_PROP = "value.deserializer";
  private static final String OVERRIDE_CHECKPOINT_STRING = "hoodie.debezium.override.initial.checkpoint.key";
  private static final String CONNECT_NAME_KEY = "connect.name";
  private static final String DATE_CONNECT_NAME = "custom.debezium.DateString";

  private final KafkaOffsetGen offsetGen;
  private final HoodieDeltaStreamerMetrics metrics;
  private final SchemaRegistryProvider schemaRegistryProvider;
  private final String deserializerClassName;

  public DebeziumSource(TypedProperties props, JavaSparkContext sparkContext,
                        SparkSession sparkSession,
                        SchemaProvider schemaProvider,
                        HoodieDeltaStreamerMetrics metrics) {
    super(props, sparkContext, sparkSession, schemaProvider);

    props.put(NATIVE_KAFKA_KEY_DESERIALIZER_PROP, StringDeserializer.class);
    deserializerClassName = props.getString(DataSourceWriteOptions.KAFKA_AVRO_VALUE_DESERIALIZER_CLASS().key(),
        DataSourceWriteOptions.KAFKA_AVRO_VALUE_DESERIALIZER_CLASS().defaultValue());

    try {
      props.put(NATIVE_KAFKA_VALUE_DESERIALIZER_PROP, Class.forName(deserializerClassName));
    } catch (ClassNotFoundException e) {
      String error = "Could not load custom avro kafka deserializer: " + deserializerClassName;
      LOG.error(error);
      throw new HoodieException(error, e);
    }

    // Currently, debezium source requires Confluent/Kafka schema-registry to fetch the latest schema.
    if (schemaProvider == null || !(schemaProvider instanceof SchemaRegistryProvider)) {
      schemaRegistryProvider = new SchemaRegistryProvider(props, sparkContext);
    } else {
      schemaRegistryProvider = (SchemaRegistryProvider) schemaProvider;
    }

    offsetGen = new KafkaOffsetGen(props);
    this.metrics = metrics;
  }

  @Override
  protected Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr, long sourceLimit) {
    String overrideCheckpointStr = props.getString(OVERRIDE_CHECKPOINT_STRING, "");

    OffsetRange[] offsetRanges = offsetGen.getNextOffsetRanges(lastCkptStr, sourceLimit, metrics);
    long totalNewMsgs = CheckpointUtils.totalNewMessages(offsetRanges);
    LOG.info("About to read " + totalNewMsgs + " from Kafka for topic :" + offsetGen.getTopicName());

    if (totalNewMsgs == 0) {
      // If there are no new messages, use empty dataframe with no schema. This is because the schema from schema registry can only be considered
      // up to date if a change event has occurred.
      return Pair.of(Option.of(sparkSession.emptyDataFrame()), overrideCheckpointStr.isEmpty() ? CheckpointUtils.offsetsToStr(offsetRanges) : overrideCheckpointStr);
    } else {
      try {
        String schemaStr = schemaRegistryProvider.fetchSchemaFromRegistry(props.getString(SchemaRegistryProvider.Config.SRC_SCHEMA_REGISTRY_URL_PROP));
        Dataset<Row> dataset = toDataset(offsetRanges, offsetGen, schemaStr);
        LOG.info(String.format("Spark schema of Kafka Payload for topic %s:\n%s", offsetGen.getTopicName(), dataset.schema().treeString()));
        LOG.info(String.format("New checkpoint string: %s", CheckpointUtils.offsetsToStr(offsetRanges)));
        return Pair.of(Option.of(dataset), overrideCheckpointStr.isEmpty() ? CheckpointUtils.offsetsToStr(offsetRanges) : overrideCheckpointStr);
      } catch (IOException exc) {
        LOG.error("Fatal error reading and parsing incoming debezium event", exc);
        throw new HoodieException("Fatal error reading and parsing incoming debezium event", exc);
      }
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

      LOG.info("Date fields: " + dateFields.toString());

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
}

