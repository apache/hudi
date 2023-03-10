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

package org.apache.hudi.utilities.config;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import javax.annotation.concurrent.Immutable;

/**
 * Delta Streamer related config.
 */
@Immutable
@ConfigClassProperty(name = "DeltaStreamer Configs",
    groupName = ConfigGroups.Names.DELTA_STREAMER,
    description = "Configurations that control DeltaStreamer.")
public class HoodieDeltaStreamerConfig extends HoodieConfig {

  private static final String DELTA_STREAMER_CONFIG_PREFIX = "hoodie.deltastreamer.";
  public static final String INGESTION_PREFIX = DELTA_STREAMER_CONFIG_PREFIX + "ingestion.";

  public static final ConfigProperty<String> CHECKPOINT_RESET_KEY = ConfigProperty
      .key("deltastreamer.checkpoint.reset_key")
      .noDefaultValue()
      .withDocumentation("");

  public static final ConfigProperty<String> CHECKPOINT_PROVIDER_PATH_PROP = ConfigProperty
      .key(DELTA_STREAMER_CONFIG_PREFIX + "checkpoint.provider.path")
      .noDefaultValue()
      .withDocumentation("When enabled, the archival table service is invoked immediately after each commit,"
          + " to archive commits if we cross a maximum value of commits."
          + " It's recommended to enable this, to ensure number of active commits is bounded.");

  public static final ConfigProperty<String> KAFKA_TOPIC_PROP = ConfigProperty
      .key(DELTA_STREAMER_CONFIG_PREFIX + "source.kafka.topic")
      .noDefaultValue()
      .withDocumentation("");

  public static final ConfigProperty<String> KAFKA_APPEND_OFFSETS = ConfigProperty
      .key(DELTA_STREAMER_CONFIG_PREFIX + "source.kafka.append.offsets")
      .defaultValue("false")
      .withDocumentation("When enabled, appends kafka offset info like source offset(_hoodie_kafka_source_offset), "
          + "partition (_hoodie_kafka_source_partition) and timestamp (_hoodie_kafka_source_timestamp) to the records. "
          + "By default its disabled and no kafka offsets are added");

  public static final ConfigProperty<Boolean> SANITIZE_SCHEMA_FIELD_NAMES = ConfigProperty
      .key(DELTA_STREAMER_CONFIG_PREFIX + "source.sanitize.invalid.schema.field.names")
      .defaultValue(false)
      .withDocumentation("Sanitizes names of invalid schema fields both in the data read from source and also in the schema "
          + "Replaces invalid characters with hoodie.deltastreamer.source.sanitize.invalid.char.mask. Invalid characters are by "
          + "goes by avro naming convention (https://avro.apache.org/docs/current/spec.html#names).");

  public static final ConfigProperty<String> SCHEMA_FIELD_NAME_INVALID_CHAR_MASK = ConfigProperty
      .key(DELTA_STREAMER_CONFIG_PREFIX + "source.sanitize.invalid.char.mask")
      .defaultValue("__")
      .withDocumentation("Defines the character sequence that replaces invalid characters in schema field names if "
          + "hoodie.deltastreamer.source.sanitize.invalid.schema.field.names is enabled.");

  public static final ConfigProperty<String> MUTLI_WRITER_SOURCE_CHECKPOINT_ID = ConfigProperty
      .key(DELTA_STREAMER_CONFIG_PREFIX + "multiwriter.source.checkpoint.id")
      .noDefaultValue()
      .withDocumentation("Unique Id to be used for multiwriter deltastreamer scenario. This is the "
          + "scenario when multiple deltastreamers are used to write to the same target table. If you are just using "
          + "a single deltastreamer for a table then you do not need to set this config.");

  public static final ConfigProperty<String> TABLES_TO_BE_INGESTED_PROP = ConfigProperty
      .key(INGESTION_PREFIX + "tablesToBeIngested")
      .noDefaultValue()
      .withDocumentation("Comma separated names of tables to be ingested in the format <database>.<table>, for example db1.table1,db1.table2");

  public static final ConfigProperty<String> TARGET_BASE_PATH_PROP = ConfigProperty
      .key(INGESTION_PREFIX + "targetBasePath")
      .noDefaultValue()
      .withDocumentation("The path to which a particular table is ingested. The config is specific to HoodieMultiTableDeltaStreamer"
          + " and overrides path determined using option `--base-path-prefix` for a table");

  /**
   * Delta Streamer Schema Provider related config.
   */
  @Immutable
  @ConfigClassProperty(name = "DeltaStreamer Schema Provider Configs",
      groupName = ConfigGroups.Names.DELTA_STREAMER,
      description = "Configurations related to source and target schema for DeltaStreamer.")
  public static class HoodieDeltaStreamerSchemaProviderConfig extends HoodieConfig {

    private static final String DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX = DELTA_STREAMER_CONFIG_PREFIX + "schemaprovider.";

    public static final ConfigProperty<String> SCHEMA_POST_PROCESSOR_PROP = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "schema_post_processor")
        .noDefaultValue()
        .withDocumentation("");

    public static final ConfigProperty<String> SRC_SCHEMA_REGISTRY_URL_PROP = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "registry.url")
        .noDefaultValue()
        .withDocumentation("The schema of the source you are reading from e.g. https://foo:bar@schemaregistry.org");

    public static final ConfigProperty<String> TARGET_SCHEMA_REGISTRY_URL_PROP = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "registry.targetUrl")
        .noDefaultValue()
        .withDocumentation("The schema of the target you are writing to e.g. https://foo:bar@schemaregistry.org");

    public static final ConfigProperty<String> SCHEMA_CONVERTER_PROP = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "registry.schemaconverter")
        .noDefaultValue()
        .withDocumentation("");

    public static final ConfigProperty<Boolean> SPARK_AVRO_POST_PROCESSOR_PROP_ENABLE = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "spark_avro_post_processor.enable")
        .defaultValue(false)
        .withDocumentation("");

    public static final ConfigProperty<String> DELETE_COLUMN_POST_PROCESSOR_COLUMN_PROP = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "schema_post_processor.delete.columns")
        .noDefaultValue()
        .withDocumentation("");

    public static final ConfigProperty<String> SCHEMA_POST_PROCESSOR_ADD_COLUMN_NAME_PROP = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "schema_post_processor.add.column.name")
        .noDefaultValue()
        .withDocumentation("New column's name");

    public static final ConfigProperty<String> SCHEMA_POST_PROCESSOR_ADD_COLUMN_TYPE_PROP = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "schema_post_processor.add.column.type")
        .noDefaultValue()
        .withDocumentation("New column's type");

    public static final ConfigProperty<Boolean> SCHEMA_POST_PROCESSOR_ADD_COLUMN_NULLABLE_PROP = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "schema_post_processor.add.column.nullable")
        .defaultValue(true)
        .withDocumentation("New column's nullable");

    public static final ConfigProperty<String> SCHEMA_POST_PROCESSOR_ADD_COLUMN_DEFAULT_PROP = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "schema_post_processor.add.column.default")
        .noDefaultValue()
        .withDocumentation("New column's default value");

    public static final ConfigProperty<String> SCHEMA_POST_PROCESSOR_ADD_COLUMN_DOC_PROP = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "schema_post_processor.add.column.doc")
        .noDefaultValue()
        .withDocumentation("Docs about new column");

    public static final ConfigProperty<String> SCHEMA_REGISTRY_BASE_URL_PROP = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "registry.baseUrl")
        .noDefaultValue()
        .withDocumentation("");

    public static final ConfigProperty<String> SCHEMA_REGISTRY_URL_SUFFIX_PROP = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "registry.urlSuffix")
        .noDefaultValue()
        .withDocumentation("");

    public static final ConfigProperty<String> SCHEMA_REGISTRY_SOURCE_URL_SUFFIX = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "registry.sourceUrlSuffix")
        .noDefaultValue()
        .withDocumentation("");

    public static final ConfigProperty<String> SCHEMA_REGISTRY_TARGET_URL_SUFFIX = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "registry.targetUrlSuffix")
        .noDefaultValue()
        .withDocumentation("");

    public static final ConfigProperty<String> SOURCE_SCHEMA_FILE_PROP = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.file")
        .noDefaultValue()
        .withDocumentation("The schema of the source you are reading from");

    public static final ConfigProperty<String> TARGET_SCHEMA_FILE_PROP = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "target.schema.file")
        .noDefaultValue()
        .withDocumentation("The schema of the target you are writing to");

    public static final ConfigProperty<String> SOURCE_SCHEMA_DATABASE_PROP = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.hive.database")
        .noDefaultValue()
        .withDocumentation("Hive database from where source schema can be fetched");

    public static final ConfigProperty<String> SOURCE_SCHEMA_TABLE_PROP = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.hive.table")
        .noDefaultValue()
        .withDocumentation("Hive table from where source schema can be fetched");

    public static final ConfigProperty<String> TARGET_SCHEMA_DATABASE_PROP = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "target.schema.hive.database")
        .noDefaultValue()
        .withDocumentation("Hive database from where target schema can be fetched");

    public static final ConfigProperty<String> TARGET_SCHEMA_TABLE_PROP = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "target.schema.hive.table")
        .noDefaultValue()
        .withDocumentation("Hive table from where target schema can be fetched");

    public static final ConfigProperty<String> SOURCE_SCHEMA_JDBC_CONNECTION_URL = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.jdbc.connection.url")
        .noDefaultValue()
        .withDocumentation("The JDBC URL to connect to. The source-specific connection properties may be specified in the URL."
            + " e.g., jdbc:postgresql://localhost/test?user=fred&password=secret");

    public static final ConfigProperty<String> SOURCE_SCHEMA_JDBC_DRIVER_TYPE = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.jdbc.driver.type")
        .noDefaultValue()
        .withDocumentation("The class name of the JDBC driver to use to connect to this URL. e.g. org.h2.Driver");

    public static final ConfigProperty<String> SOURCE_SCHEMA_JDBC_USERNAME = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.jdbc.username")
        .noDefaultValue()
        .withDocumentation("Username for the connection e.g. fred");

    public static final ConfigProperty<String> SOURCE_SCHEMA_JDBC_PASSWORD = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.jdbc.password")
        .noDefaultValue()
        .withDocumentation("Password for the connection e.g. secret");

    public static final ConfigProperty<String> SOURCE_SCHEMA_JDBC_DBTABLE = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.jdbc.dbtable")
        .noDefaultValue()
        .withDocumentation("The table with the schema to reference e.g. test_database.test1_table or test1_table");

    public static final ConfigProperty<String> SOURCE_SCHEMA_JDBC_TIMEOUT = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.jdbc.timeout")
        .noDefaultValue()
        .withDocumentation("The number of seconds the driver will wait for a Statement object to execute. Zero means there is no limit. "
            + "In the write path, this option depends on how JDBC drivers implement the API setQueryTimeout, e.g., the h2 JDBC driver "
            + "checks the timeout of each query instead of an entire JDBC batch. It defaults to 0.");

    public static final ConfigProperty<String> SOURCE_SCHEMA_JDBC_NULLABLE = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.jdbc.nullable")
        .noDefaultValue()
        .withDocumentation("If true, all the columns are nullable.");

    private static final ConfigProperty<String> PROTO_SCHEMA_PROVIDER_PREFIX = ConfigProperty
        .key(DELTA_STREAMER_SCHEMAPROVIDER_CONFIG_PREFIX + "proto")
        .noDefaultValue()
        .withDocumentation("");

    public static final ConfigProperty<String> PROTO_SCHEMA_CLASS_NAME = ConfigProperty
        .key(PROTO_SCHEMA_PROVIDER_PREFIX + ".class.name")
        .noDefaultValue()
        .sinceVersion("0.13.0")
        .withDocumentation("The Protobuf Message class used as the source for the schema.");

    public static final ConfigProperty<Boolean> PROTO_SCHEMA_WRAPPED_PRIMITIVES_AS_RECORDS = ConfigProperty
        .key(PROTO_SCHEMA_PROVIDER_PREFIX + ".flatten.wrappers")
        .defaultValue(false)
        .sinceVersion("0.13.0")
        .withDocumentation("When set to true wrapped primitives like Int64Value are translated to a record with a single 'value' field. By default, the value is false and the wrapped primitives are "
            + "treated as a nullable value");

    public static final ConfigProperty<Boolean> PROTO_SCHEMA_TIMESTAMPS_AS_RECORDS = ConfigProperty
        .key(PROTO_SCHEMA_PROVIDER_PREFIX + ".timestamps.as.records")
        .defaultValue(false)
        .sinceVersion("0.13.0")
        .withDocumentation("When set to true Timestamp fields are translated to a record with a seconds and nanos field. By default, the value is false and the timestamp is converted to a long with "
            + "the timestamp-micros logical type");

    public static final ConfigProperty<Integer> PROTO_SCHEMA_MAX_RECURSION_DEPTH = ConfigProperty
        .key(PROTO_SCHEMA_PROVIDER_PREFIX + ".max.recursion.depth")
        .defaultValue(5)
        .sinceVersion("0.13.0")
        .withDocumentation("The max depth to unravel the Proto schema when translating into an Avro schema. Setting this depth allows the user to convert a schema that is recursive in proto into "
            + "something that can be represented in their lake format like Parquet. After a given class has been seen N times within a single branch, the schema provider will create a record with a "
            + "byte array to hold the remaining proto data and a string to hold the message descriptor's name for context.");
  }
}
