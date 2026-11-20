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

package org.apache.hudi.utilities.unstructured.embedding.streamer;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** Stream-owned settings and schema inspection for the auxiliary column history. */
final class EmbeddingColumnInfoConfig {
  /**
   * Physical VECTOR names to retire after this round commits; physical columns and their history remain. A retirement is
   * recorded only by a round that commits with the name set, so keep it set until the column history shows the retirement.
   */
  static final ConfigProperty<String> RETIRE_COLUMNS = ConfigProperty
      .key(EmbeddingTableConfig.PREFIX + "retire.columns")
      .defaultValue("")
      .sinceVersion("1.3.0")
      .withDocumentation("Comma-separated physical VECTOR column names to retire after a successful embedding round. "
          + "A retired name cannot be configured again. Existing nullable columns and all history remain. A retirement is "
          + "recorded only by a round that commits with the name set: keep it set until the column history shows the "
          + "retirement, as a request removed before then is lost.");

  /** An explicit provider revision, only when the provider exposes one; absence is part of the identity. */
  static final String MODEL_VERSION_SUFFIX = ".version";

  /** Metadata for the per-model provider revision setting. */
  static final ConfigProperty<String> MODEL_VERSION = ConfigProperty
      .key(EmbeddingTableConfig.MODEL_PREFIX + "<alias>" + MODEL_VERSION_SUFFIX)
      .noDefaultValue()
      .sinceVersion("1.3.0")
      .withDocumentation("Provider-exposed model revision, when available. Replace <alias> with the declared model alias. "
          + "Do not infer a revision from the model name or put connection settings here.");

  private EmbeddingColumnInfoConfig() {
  }

  static EmbeddingColumnInfoStore.Identity identity(EmbeddingModelSpec model, TypedProperties props) {
    String key = EmbeddingTableConfig.MODEL_PREFIX + model.getName() + MODEL_VERSION_SUFFIX;
    String version = props.getString(key, null);
    if (version != null) {
      version = version.trim();
      if (version.isEmpty()) {
        throw new IllegalArgumentException(key + " must be a nonempty provider revision, or absent");
      }
    }
    String inputTransform = model.declares(EmbeddingTableSchema.MODALITY_IMAGE)
        ? EmbeddingTableConfig.from(props).imagePreprocessing().inputTransformVersion()
        : EmbeddingColumnInfoStore.Identity.NO_INPUT_TRANSFORM;
    return new EmbeddingColumnInfoStore.Identity(model.getProvider(), model.getModelName(), model.getDimension(), version,
        inputTransform);
  }

  static Set<String> retiredColumns(TypedProperties props) {
    return new LinkedHashSet<>(EmbeddingTableConfig.splitList(props.getString(RETIRE_COLUMNS.key(), RETIRE_COLUMNS.defaultValue())));
  }

  static Map<String, Integer> vectorColumns(Option<StructType> schema) {
    Map<String, Integer> vectors = new LinkedHashMap<>();
    if (schema.isPresent()) {
      for (StructField field : schema.get().fields()) {
        if (field.metadata().contains(HoodieSchema.TYPE_METADATA_FIELD)) {
          String type = field.metadata().getString(HoodieSchema.TYPE_METADATA_FIELD);
          if (type.startsWith("VECTOR(")) {
            vectors.put(field.name(), ((HoodieSchema.Vector) HoodieSchema.parseTypeDescriptor(type)).getDimension());
          }
        }
      }
    }
    return vectors;
  }

  static Map<String, Integer> vectorColumns(HoodieSchema schema) {
    Map<String, Integer> vectors = new LinkedHashMap<>();
    for (HoodieSchemaField field : schema.getFields()) {
      HoodieSchema type = field.schema().getNonNullType();
      if (type.getType() == HoodieSchemaType.VECTOR) {
        vectors.put(field.name(), ((HoodieSchema.Vector) type).getDimension());
      }
    }
    return vectors;
  }
}
