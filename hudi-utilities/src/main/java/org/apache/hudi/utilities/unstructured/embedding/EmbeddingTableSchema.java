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

package org.apache.hudi.utilities.unstructured.embedding;

import org.apache.hudi.HoodieSchemaConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.utilities.config.CloudSourceConfig;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * The embedding table schema. One writer writes whole rows: an upsert of a key replaces the row.
 */
public final class EmbeddingTableSchema {

  public static final String SOURCE_PATH_SHA = "source_path_sha";
  public static final String SOURCE_MODIFICATION_TIME = "source_modification_time";
  public static final String SOURCE_FILE_SIZE = "source_file_size";
  public static final String CHUNK_ID = "chunk_id";
  public static final String MODALITY = "modality";
  /** The source file's path, as the source row's {@code source_file_path}. */
  public static final String SOURCE_FILE_PATH = "source_file_path";
  /** The source file's name, as the source row's {@code source_file_name}. */
  public static final String SOURCE_FILE_NAME = "source_file_name";
  /** The source file's extension, as the source row's {@code source_file_extension}. */
  public static final String SOURCE_FILE_EXTENSION = "source_file_extension";
  /** The prepared text a model embeds for a text row (a PDF chunk). */
  public static final String PREPARED_TEXT_CHUNK = "prepared_text_chunk";
  public static final String CHUNK_TOKEN_COUNT = "chunk_token_count";
  public static final String AUX = "aux";
  public static final int MAX_ERROR_CHARS = 1024;

  /**
   * {@link #MODALITY} values: the TARGET type of a row's model input, never the source file's
   * type. Text rows: a PDF's chunks.
   */
  public static final String MODALITY_TEXT = "text";
  /** The modality an error record names for a file no preprocessor is mapped to; never a row's modality. */
  public static final String MODALITY_UNKNOWN = "unknown";

  /** The source version, modality and ordered chunk identify one embedding row. */
  public static final List<String> KEY_COLUMNS = Collections.unmodifiableList(Arrays.asList(SOURCE_PATH_SHA,
      SOURCE_MODIFICATION_TIME, SOURCE_FILE_SIZE, MODALITY, CHUNK_ID));
  /** Fixed columns before model vectors and user partition columns, in {@link #build} order. */
  public static final List<String> BASE_COLUMNS = Collections.unmodifiableList(Arrays.asList(SOURCE_PATH_SHA,
      SOURCE_MODIFICATION_TIME, SOURCE_FILE_SIZE, MODALITY, CHUNK_ID, SOURCE_FILE_PATH, SOURCE_FILE_NAME,
      SOURCE_FILE_EXTENSION, PREPARED_TEXT_CHUNK, CHUNK_TOKEN_COUNT, AUX));
  /** Number of fixed columns, up to {@link #AUX}. */
  public static final int BASE_COLUMN_COUNT = BASE_COLUMNS.size();
  /** Columns each declared model owns: its VECTOR column. */
  public static final int COLUMNS_PER_MODEL = 1;

  private EmbeddingTableSchema() {
  }

  /**
   * The table's columns: the {@link #BASE_COLUMNS}, one VECTOR column per declared model ({@link EmbeddingTableConfig#models}),
   * then the user partition columns. With no model declared, the prep_only shape, the table has no VECTOR column.
   *
   * @throws HoodieException if two columns would share one name (see {@link EmbeddingTableConfig#models})
   */
  public static StructType build(EmbeddingTableConfig config) {
    List<StructField> fields = new ArrayList<>();
    fields.add(DataTypes.createStructField(SOURCE_PATH_SHA, DataTypes.StringType, false));
    fields.add(DataTypes.createStructField(SOURCE_MODIFICATION_TIME, DataTypes.LongType, false));
    fields.add(DataTypes.createStructField(SOURCE_FILE_SIZE, DataTypes.LongType, false));
    fields.add(DataTypes.createStructField(MODALITY, DataTypes.StringType, false));
    fields.add(DataTypes.createStructField(CHUNK_ID, DataTypes.LongType, false));
    fields.add(DataTypes.createStructField(SOURCE_FILE_PATH, DataTypes.StringType, false));
    fields.add(DataTypes.createStructField(SOURCE_FILE_NAME, DataTypes.StringType, false));
    fields.add(DataTypes.createStructField(SOURCE_FILE_EXTENSION, DataTypes.StringType, false));
    fields.add(DataTypes.createStructField(PREPARED_TEXT_CHUNK, DataTypes.StringType, true));
    fields.add(DataTypes.createStructField(CHUNK_TOKEN_COUNT, DataTypes.IntegerType, true));
    fields.add(DataTypes.createStructField(AUX, DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true));
    for (EmbeddingModelSpec model : config.models()) {
      fields.add(vectorField(model));
    }
    for (String column : userPartitionColumns(config.getProps())) {
      fields.add(DataTypes.createStructField(column, DataTypes.StringType, true));
    }
    return DataTypes.createStructType(fields);
  }

  /** Stock source path columns carried to the embedding rows; fixed column names are reserved ignoring case. */
  public static List<String> userPartitionColumns(TypedProperties props) {
    if (!ConfigUtils.containsConfigProperty(props, CloudSourceConfig.PATH_BASED_PARTITION_FIELDS)) {
      return Collections.emptyList();
    }
    List<String> columns = new ArrayList<>();
    for (String configured : ConfigUtils.getStringWithAltKeys(props, CloudSourceConfig.PATH_BASED_PARTITION_FIELDS).split(",")) {
      String column = configured.trim();
      if (!column.isEmpty() && BASE_COLUMNS.stream().noneMatch(fixed -> fixed.equalsIgnoreCase(column))) {
        columns.add(column);
      }
    }
    return columns;
  }

  /** The VECTOR column of {@code model}: named by {@code model.<alias>.column}, sized by its dimensions. */
  public static StructField vectorField(EmbeddingModelSpec model) {
    Metadata metadata = new MetadataBuilder()
        .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(" + model.getDimension() + ")")
        .build();
    return new StructField(model.getColumn(), new ArrayType(DataTypes.FloatType, false), true, metadata);
  }

  /**
   * {@code error} cut to at most {@link #MAX_ERROR_CHARS} chars on a code-point boundary: when the cut would keep only
   * the high surrogate of a pair, the pair is dropped whole (one char shorter), so a stored error is never malformed UTF-16.
   *
   * @return {@code error} itself when it is null or short enough
   */
  public static String truncateError(String error) {
    if (error == null || error.length() <= MAX_ERROR_CHARS) {
      return error;
    }
    int end = Character.isHighSurrogate(error.charAt(MAX_ERROR_CHARS - 1)) ? MAX_ERROR_CHARS - 1 : MAX_ERROR_CHARS;
    return error.substring(0, end);
  }

  /**
   * The table's latest committed schema, including meta fields; empty for a missing table or one without a completed commit.
   */
  public static Option<StructType> writerReadSchema(String tablePath, StorageConfiguration<?> conf) {
    if (!EmbeddingTableChecks.tableExists(tablePath, conf)) {
      return Option.empty();
    }
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(conf.newInstance()).setBasePath(tablePath).build();
    if (metaClient.getCommitsTimeline().filterCompletedInstants().empty()) {
      return Option.empty();
    }
    try {
      return Option.of(HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(
          new TableSchemaResolver(metaClient).getTableSchema()));
    } catch (Exception e) {
      throw new HoodieException("Failed reading embedding table schema at " + tablePath, e);
    }
  }
}
