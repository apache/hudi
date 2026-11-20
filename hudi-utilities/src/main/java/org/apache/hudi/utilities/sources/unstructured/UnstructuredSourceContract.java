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

package org.apache.hudi.utilities.sources.unstructured;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.ComplexKeyGenerator;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.utilities.sources.SourceContract;
import org.apache.hudi.utilities.sources.UnstructuredDataSource;
import org.apache.hudi.utilities.sources.blob.BlobPhysicalPins;
import org.apache.hudi.utilities.sources.blob.BlobSourceSetup;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.input.EmbeddingInputConfig;
import org.apache.hudi.utilities.unstructured.embedding.prep.PrepLogicVersions;
import org.apache.hudi.utilities.unstructured.embedding.prep.PreprocessorRegistry;
import org.apache.hudi.utilities.unstructured.embedding.streamer.EmbeddingColumnInfoStore;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * What the job of {@link UnstructuredDataSource} requires, all of it read from its mode ({@link UnstructuredMode}): the settings
 * it runs with, the mode's transformer in every round with the other mode's refused, the schema guard last in the chain, and the
 * check of the chain and the schemas against the table's contract.
 */
public class UnstructuredSourceContract implements SourceContract {

  /** The key generators of both jobs' tables: the complex one (partitioned) and the non-partitioned one. */
  private static final List<String> KEY_GENERATORS = Arrays.asList(ComplexKeyGenerator.class.getName(),
      NonpartitionedKeyGenerator.class.getName());

  private final UnstructuredMode mode;
  private final UnstructuredTableProperties tablePins;
  private TypedProperties preparedProperties;
  private String checkedTablePath;
  private Properties checkedTableProperties;

  /**
   * The contract of the job of {@code props}.
   *
   * @throws IllegalArgumentException when {@code hoodie.unstructured.source.mode} is unset or names no mode
   */
  public UnstructuredSourceContract(TypedProperties props) {
    this.mode = UnstructuredMode.from(props);
    String sourceName = UnstructuredSchemaContract.sourceName(mode);
    if (mode == UnstructuredMode.EMBEDDING) {
      this.tablePins = embeddingTablePins(sourceName, props);
    } else {
      Map<String, String> physical = BlobPhysicalPins.of(props);
      this.tablePins = UnstructuredTableVersion.tablePropertiesBuilder(sourceName, mode.configValue())
          .requireTableProperties(physical, (name, basePath, table) -> BlobPhysicalPins.check(name, basePath, table, physical))
          .build();
    }
  }

  /**
   * The embedding table's pins for the job of {@code props}, the one list that the streamer's contract and a direct transformer
   * run ({@link org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableChecks}) both write and check: the layout pins
   * of every unstructured table, the column-info layout version, the input store's inline threshold and the prep contract
   * ({@link PrepLogicVersions}). The threshold is written at creation by every job, and checked only for a job whose
   * preprocessors store inputs ({@link #storesInputs}): it decides no byte of a job that stores none.
   *
   * @param sourceName the job, as the refusals name it
   */
  public static UnstructuredTableProperties embeddingTablePins(String sourceName, TypedProperties props) {
    String inlineMaxBytes = Long.toString(EmbeddingInputConfig.inlineMaxBytes(props));
    TypedProperties job = TypedProperties.copy(props);
    UnstructuredTableProperties.Builder pins = UnstructuredTableVersion.tablePropertiesBuilder(sourceName, UnstructuredMode.EMBEDDING.configValue())
        .requireTableProperty(EmbeddingColumnInfoStore.LAYOUT_VERSION, EmbeddingColumnInfoStore.CURRENT_LAYOUT_VERSION)
        .requireTableProperties(Collections.singletonMap(EmbeddingInputConfig.TABLE_INLINE_MAX_BYTES.key(), inlineMaxBytes),
            (name, basePath, table) -> {
              if (!inlineMaxBytes.equals(table.getProperty(EmbeddingInputConfig.TABLE_INLINE_MAX_BYTES.key()))
                  && storesInputs(UnstructuredMode.EMBEDDING, job)) {
                EmbeddingInputConfig.checkTable(name, basePath, table, inlineMaxBytes);
              }
            });
    PrepLogicVersions.from(props).requireTableProperties(pins);
    return pins.build();
  }

  /** The job this contract is of. */
  public UnstructuredMode mode() {
    return mode;
  }

  /** Merges this mode's settings once, before any job consumer reads them. */
  @Override
  public TypedProperties prepareJob(TypedProperties jobProps) {
    if (isPreparedJob(jobProps)) {
      return jobProps;
    }
    TypedProperties merged = TypedProperties.copy(jobProps);
    require(merged, KeyGeneratorOptions.RECORDKEY_FIELD_NAME, String.join(",", mode.keyColumns()),
        UnstructuredSourceContract::fields, " (these fields, in this order)");
    require(merged, HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS, mode.injectorClass(), String::trim,
        " (a job's own injector goes under " + mode.injectorDelegateKey() + ", and runs first)");
    if (mode == UnstructuredMode.EMBEDDING && ConfigUtils.getBooleanWithAltKeys(merged, HoodieWriteConfig.BLOB_MARKERS_ENABLE)
        && storesInputs(mode, merged)) {
      require(merged, HoodieWriteConfig.MARKERS_TYPE, MarkerType.TIMELINE_SERVER_BASED.name(),
          value -> value.trim().toLowerCase(java.util.Locale.ROOT), "");
      require(merged, HoodieWriteConfig.ROLLBACK_USING_MARKERS_ENABLE, "true",
          value -> value.trim().toLowerCase(java.util.Locale.ROOT), "");
    }
    if (mode == UnstructuredMode.BLOB) {
      setDefault(merged, HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME, "uncompressed");
      setDefault(merged, HoodieStorageConfig.PARQUET_WITH_BLOOM_FILTER_ENABLED, "false");
    }
    merged = tablePins.apply(merged);
    checkKeyGenerator(UnstructuredSchemaContract.sourceName(mode), merged);
    validateJob(merged);
    preparedProperties = TypedProperties.copy(merged);
    return merged;
  }

  /**
   * Whether the job writes files beside its data files (out-of-line inputs, which need timeline-server markers so a failed
   * round's rollback removes them): only an embedding job whose allowed preprocessors store inputs (images, video frames). A blob job
   * never does: a file above its inline threshold is referenced where it is.
   */
  private static boolean storesInputs(UnstructuredMode mode, TypedProperties props) {
    if (mode == UnstructuredMode.BLOB) {
      return false;
    }
    PreprocessorRegistry registry = new PreprocessorRegistry(props);
    try {
      return registry.storesInputs(EmbeddingTableConfig.from(props));
    } finally {
      registry.close();
    }
  }

  @Override
  public Properties tableProperties() {
    return tablePins.tableProperties();
  }

  /** The stream's exact persisted pins, independent of Hudi's generic source preparation. */
  public UnstructuredTableProperties requiredTableProperties() {
    return tablePins;
  }

  private void require(TypedProperties props, ConfigProperty<?> key, String value, UnaryOperator<String> normalize, String hint) {
    String configuredKey = configuredKey(props, key);
    String configured = configuredKey == null ? null : props.get(configuredKey).toString();
    if (configured != null && !normalize.apply(value).equals(normalize.apply(configured))) {
      throw new IllegalArgumentException(UnstructuredSchemaContract.sourceName(mode) + " requires " + key.key() + "=" + value + hint
          + ", configured: " + (configuredKey.equals(key.key()) ? "" : configuredKey + "=") + configured);
    }
    props.setProperty(key.key(), value);
  }

  private static void setDefault(TypedProperties props, ConfigProperty<?> key, String value) {
    if (configuredKey(props, key) == null) {
      props.setProperty(key.key(), value);
    }
  }

  private static String configuredKey(TypedProperties props, ConfigProperty<?> key) {
    List<String> keys = new ArrayList<>();
    keys.add(key.key());
    keys.addAll(key.getAlternatives());
    for (String candidate : keys) {
      Object value = props.get(candidate);
      if (value != null && !StringUtils.isNullOrEmpty(value.toString().trim())) {
        return candidate;
      }
    }
    return null;
  }

  /** A record key is a field list with significant order, independent of surrounding whitespace. */
  public static String fields(String value) {
    return Arrays.stream(value.split(",")).map(String::trim).filter(field -> !field.isEmpty()).collect(Collectors.joining(","));
  }

  @Override
  public List<String> transformerChain(List<String> configured) {
    return UnstructuredTransformerChain.compose(mode, configured);
  }

  /**
   * Reads the job and the cloud, refuses a key of the other cloud, and validates the job's other properties
   * ({@link BlobSourceSetup#validate}), the table-version floor among them; returns validated properties. The streamer runs it
   * with the merge, before it builds anything or creates the table, and the source reuses the result while the properties are unchanged.
   *
   * @throws IllegalArgumentException          naming the setting and {@code UnstructuredDataSource (mode ...)}
   */
  public static TypedProperties validateJob(TypedProperties props) {
    UnstructuredMode mode = UnstructuredMode.from(props);
    UnstructuredCloud cloud = UnstructuredCloud.from(props);
    String name = UnstructuredSchemaContract.sourceName(mode);
    cloud.validate(props, name);
    return BlobSourceSetup.validate(props, mode, name);
  }

  @Override
  public boolean isPreparedJob(TypedProperties props) {
    return preparedProperties != null && preparedProperties.equals(props);
  }

  /**
   * Refuses a key generator, named or inferred from the record key and partition fields, other than {@link #KEY_GENERATORS}; a name
   * with spaces is also refused, since the writer loads it as written.
   */
  private static void checkKeyGenerator(String sourceName, TypedProperties props) {
    String keyGenerator = HoodieSparkKeyGeneratorFactory.getKeyGeneratorClassName(props);
    if (!KEY_GENERATORS.contains(keyGenerator)) {
      throw new IllegalArgumentException(sourceName + " supports only " + String.join(" or ", KEY_GENERATORS) + ", configured: " + keyGenerator);
    }
  }

  /** Checks the chain and the schemas against the mode's contract, before the source is built or any input is read. */
  @Override
  public void checkJob(JavaSparkContext jsc, SparkSession spark, List<String> chain, TypedProperties props,
                       Option<HoodieSchema> targetSchema, Option<StructType> tableSchema) {
    UnstructuredDataSource.validate(mode, jsc, spark, chain, props, targetSchema, tableSchema);
  }

  /** Checks the table's pins next to the record-key validation, before source construction. */
  @Override
  public void checkTable(String basePath, HoodieTableConfig table) {
    if (basePath.equals(checkedTablePath) && table.getProps().equals(checkedTableProperties)) {
      return;
    }
    UnstructuredDataSource.validate(mode, basePath, table);
    tablePins.checkTable(basePath, table);
    checkedTablePath = basePath;
    checkedTableProperties = new Properties();
    checkedTableProperties.putAll(table.getProps());
  }

}
