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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.unstructured.embedding.text.Utf16;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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
  /**
   * The model input of an image row, a nullable Hudi BLOB: an image file's own bytes, as the source row carried them (inline, or
   * an unmanaged reference to the object), or a derived input (a video frame) stored inline. Null on text rows. Written in both
   * modes: a prep_only table has no vector column but keeps its prepared images.
   */
  public static final String PREPARED_IMAGE = "prepared_image";
  /** Aux key of a row carrying {@link #PREPARED_IMAGE}: the stored input's width in pixels. */
  public static final String AUX_INPUT_WIDTH = "input_width";
  /** Aux key of a row carrying {@link #PREPARED_IMAGE}: the stored input's height in pixels. */
  public static final String AUX_INPUT_HEIGHT = "input_height";
  /** Legacy raw-chunk logic hint; row assembly replaces it with {@link #AUX_PREP_LOGIC_VERSION}. */
  public static final String AUX_INPUT_LOGIC = "input_logic";
  /** The table-pinned preparation logic version on every prepared row, including text. */
  public static final String AUX_PREP_LOGIC_VERSION = "prep_logic_version";
  /** Aux key of a video frame row: the frame's dense kept-frame index, which is also its chunk_id. */
  public static final String AUX_FRAME_INDEX = "frame_index";
  public static final int MAX_ERROR_CHARS = 1024;

  /**
   * {@link #MODALITY} values: the TARGET type of a row's model input, never the source file's
   * type. Text rows: a PDF's chunks; image rows: an image file's row.
   */
  public static final String MODALITY_TEXT = "text";
  /** Image rows: an image file's row and a video's frame rows (a frame is an image to the model). */
  public static final String MODALITY_IMAGE = "image";
  /** The modality an error record names for a file no preprocessor is mapped to; never a row's modality. */
  public static final String MODALITY_UNKNOWN = "unknown";

  /**
   * The source version, modality and ordered chunk identify one embedding row. Keeping the source version in the key is a
   * decision, as for the blob table ({@link org.apache.hudi.utilities.sources.blob.BlobTableSchema#KEY_COLUMNS}); moving to an
   * identity-only key needs a new {@code hoodie.table.unstructured.schema.version} and a rebuilt table.
   */
  public static final List<String> KEY_COLUMNS = Collections.unmodifiableList(Arrays.asList(SOURCE_PATH_SHA,
      SOURCE_MODIFICATION_TIME, SOURCE_FILE_SIZE, MODALITY, CHUNK_ID));
  /** Fixed columns before model vectors and user partition columns, in {@link #build} order. */
  public static final List<String> BASE_COLUMNS = Collections.unmodifiableList(Arrays.asList(SOURCE_PATH_SHA,
      SOURCE_MODIFICATION_TIME, SOURCE_FILE_SIZE, MODALITY, CHUNK_ID, SOURCE_FILE_PATH, SOURCE_FILE_NAME,
      SOURCE_FILE_EXTENSION, PREPARED_TEXT_CHUNK, CHUNK_TOKEN_COUNT, AUX, PREPARED_IMAGE));
  /** Number of fixed columns, up to {@link #PREPARED_IMAGE}. */
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
    fields.add(preparedImageField());
    for (EmbeddingModelSpec model : config.models()) {
      fields.add(vectorField(model));
    }
    for (String column : userPartitionColumns(config.getProps())) {
      fields.add(DataTypes.createStructField(column, DataTypes.StringType, true));
    }
    return DataTypes.createStructType(fields);
  }

  /**
   * The schema of the rows a round of {@code config} writes on the table whose latest writer schema is {@code table} (empty for a
   * new table): {@link #outputSchema} with the active model under {@code prep_and_compute}, none under {@code prep_only}.
   */
  public static StructType roundSchema(EmbeddingTableConfig config, Option<StructType> table) {
    return outputSchema(config, table, config.computes() ? Option.of(config.activeModel()) : Option.empty());
  }

  /**
   * The schema of the rows the round writes: the latest writer schema of an existing table ({@code table}) without its meta
   * fields, every column beyond the base ones nullable (prep writes null in a column it does not make), with a missing nullable
   * prepared-image BLOB appended before validation and the active model's VECTOR column added under {@code prep_and_compute} when the table does not have it yet; else the configuration's
   * ({@link EmbeddingTableSchema#build}).
   */
  public static StructType outputSchema(EmbeddingTableConfig config, Option<StructType> table, Option<EmbeddingModelSpec> model) {
    if (!table.isPresent()) {
      return EmbeddingTableSchema.build(config);
    }
    StructType existing = table.get();
    // Preserve nullable-image evolution for tables created before image preparation.
    if (Arrays.stream(existing.fields()).noneMatch(field -> field.name().equalsIgnoreCase(PREPARED_IMAGE))) {
      existing = existing.add(preparedImageField());
    }
    EmbeddingTableSchema.checkBaseColumns(existing);
    // every column of the table is in the round's rows, so the write keeps the table's schema; one beyond the base columns (a path
    // partition, a model's VECTOR, a column a customer's transformer added) is nullable, since prep writes null in a column it
    // does not make. A customer's transformer that adds its column again in the round wins over that null (UnstructuredSchemaGuard)
    List<StructField> fields = Arrays.stream(existing.fields())
        .filter(field -> !HoodieRecord.HOODIE_META_COLUMNS.contains(field.name()))
        .map(field -> EmbeddingTableSchema.BASE_COLUMNS.contains(field.name()) || field.nullable() ? field
            : new StructField(field.name(), field.dataType(), true, field.metadata()))
        .collect(Collectors.toList());
    if (model.isPresent()) {
      StructField vector = EmbeddingTableSchema.vectorField(model.get());
      if (fields.stream().noneMatch(field -> field.name().equals(vector.name()))) {
        fields.add(vector);
      }
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

  /** The nullable {@link #PREPARED_IMAGE} column: a Hudi BLOB ({@code BlobType}, Hudi type metadata {@code BLOB}). */
  public static StructField preparedImageField() {
    Metadata metadata = new MetadataBuilder()
        .putString(HoodieSchema.TYPE_METADATA_FIELD, HoodieSchemaType.BLOB.name())
        .build();
    return new StructField(PREPARED_IMAGE, BlobType.dataType(), true, metadata);
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
    int end = Utf16.floor(error, MAX_ERROR_CHARS, true);
    return error.substring(0, end);
  }

  /** Refuses a schema predating any base column before the round can silently drop its value. */
  public static void checkBaseColumns(StructType schema) {
    List<String> found = Arrays.asList(schema.fieldNames());
    for (String column : BASE_COLUMNS) {
      if (!found.contains(column)) {
        throw new IllegalArgumentException("Embedding table is missing base column " + column
            + "; create a new embedding table with schema version 1. Nothing was written");
      }
    }
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
