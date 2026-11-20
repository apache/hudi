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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.sources.blob.BlobParquetConfigInjector;
import org.apache.hudi.utilities.sources.blob.BlobQuarantineTransformer;
import org.apache.hudi.utilities.sources.blob.BlobTableSchema;
import org.apache.hudi.utilities.sources.helpers.CloudObjectsSelectorCommon;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingParquetConfigInjector;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.streamer.EmbeddingTableTransformer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The unstructured data source's job and cloud keys, and what each value binds. */
public class TestUnstructuredModeAndCloud {

  private static final String SOURCE = "UnstructuredDataSource (mode blob)";

  private static TypedProperties props(String key, String value) {
    TypedProperties props = new TypedProperties();
    if (value != null) {
      props.setProperty(key, value);
    }
    return props;
  }

  @Test
  void modeFromBlobAndEmbedding() {
    assertEquals(UnstructuredMode.BLOB, UnstructuredMode.from(props(UnstructuredSourceConfig.MODE.key(), "blob")));
    assertEquals(UnstructuredMode.EMBEDDING, UnstructuredMode.from(props(UnstructuredSourceConfig.MODE.key(), " embedding ")));
    assertEquals("blob", UnstructuredMode.BLOB.configValue());
    assertEquals("embedding", UnstructuredMode.EMBEDDING.configValue());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " ", "BLOB", "blobs", "embedding_prep"})
  void modeUnsetOrUnknownRefusedNamingKeyAndValues(String value) {
    IllegalArgumentException unknown = assertThrows(IllegalArgumentException.class,
        () -> UnstructuredMode.from(props(UnstructuredSourceConfig.MODE.key(), value)));
    assertTrue(unknown.getMessage().contains("hoodie.unstructured.source.mode"), unknown.getMessage());
    assertTrue(unknown.getMessage().contains("blob, embedding"), unknown.getMessage());
    assertTrue(unknown.getMessage().contains("UnstructuredDataSource"), unknown.getMessage());
    IllegalArgumentException unset = assertThrows(IllegalArgumentException.class, () -> UnstructuredMode.from(new TypedProperties()));
    assertTrue(unset.getMessage().contains("hoodie.unstructured.source.mode, one of: blob, embedding; configured: none"), unset.getMessage());
  }

  @Test
  void cloudFromS3AndGcs() {
    assertEquals(UnstructuredCloud.S3, UnstructuredCloud.from(props(UnstructuredSourceConfig.CLOUD.key(), "s3")));
    assertEquals(UnstructuredCloud.GCS, UnstructuredCloud.from(props(UnstructuredSourceConfig.CLOUD.key(), "gcs")));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "S3", "gs", "azure"})
  void cloudUnsetOrUnknownRefused(String value) {
    IllegalArgumentException unknown = assertThrows(IllegalArgumentException.class,
        () -> UnstructuredCloud.from(props(UnstructuredSourceConfig.CLOUD.key(), value)));
    assertTrue(unknown.getMessage().contains("hoodie.unstructured.source.cloud, one of: s3, gcs"), unknown.getMessage());
    assertThrows(IllegalArgumentException.class, () -> UnstructuredCloud.from(new TypedProperties()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"hoodie.streamer.source.s3incr.fs.prefix", "hoodie.deltastreamer.source.s3incr.fs.prefix"})
  void gcsRefusesS3FsPrefix(String key) {
    TypedProperties props = props(key, "s3a");
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> UnstructuredCloud.GCS.validate(props, SOURCE));
    assertTrue(refused.getMessage().contains(SOURCE + " refuses hoodie.streamer.source.s3incr.fs.prefix"), refused.getMessage());
    assertTrue(refused.getMessage().contains("hoodie.unstructured.source.cloud=gcs"), refused.getMessage());
    assertDoesNotThrow(() -> UnstructuredCloud.S3.validate(props, SOURCE));
    assertDoesNotThrow(() -> UnstructuredCloud.GCS.validate(new TypedProperties(), SOURCE));
  }

  @Test
  void blobRefusesEmbeddingMode() {
    TypedProperties props = props("hoodie.unstructured.embedding.mode", "prep_only");
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> UnstructuredMode.BLOB.validate(props, SOURCE));
    assertTrue(refused.getMessage().contains(SOURCE + " refuses hoodie.unstructured.embedding.mode=prep_only"), refused.getMessage());
    assertDoesNotThrow(() -> UnstructuredMode.BLOB.validate(new TypedProperties(), SOURCE));
  }

  @Test
  void embeddingRequiresEmbeddingMode() {
    String source = "UnstructuredDataSource (mode embedding)";
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> UnstructuredMode.EMBEDDING.validate(new TypedProperties(), source));
    assertTrue(refused.getMessage().contains(source + " requires hoodie.unstructured.embedding.mode"), refused.getMessage());
    assertDoesNotThrow(() -> UnstructuredMode.EMBEDDING.validate(props("hoodie.unstructured.embedding.mode", "prep_only"), source));
    assertThrows(IllegalArgumentException.class,
        () -> UnstructuredMode.EMBEDDING.validate(props("hoodie.unstructured.embedding.mode", "prep_and_compute"), source));
  }

  @Test
  void embeddingChecksPropertyOnlySettingsBeforeDataArrives() {
    String source = "UnstructuredDataSource (mode embedding)";
    TypedProperties invalidMode = props(EmbeddingTableConfig.MODE.key(), "compute_only");
    assertThrows(IllegalArgumentException.class, () -> UnstructuredMode.EMBEDDING.validate(invalidMode, source));
    TypedProperties invalidExtension = props(EmbeddingTableConfig.MODE.key(), "prep_only");
    invalidExtension.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "unmapped_extension");
    assertThrows(IllegalArgumentException.class, () -> UnstructuredMode.EMBEDDING.validate(invalidExtension, source));
    TypedProperties invalidModel = props(EmbeddingTableConfig.MODE.key(), "prep_only");
    invalidModel.setProperty(EmbeddingTableConfig.MODELS.key(), "m1");
    assertThrows(IllegalArgumentException.class, () -> UnstructuredMode.EMBEDDING.validate(invalidModel, source));
  }

  @Test
  void perModeBindings() {
    assertEquals(BlobTableSchema.KEY_COLUMNS, UnstructuredMode.BLOB.keyColumns());
    assertEquals(BlobQuarantineTransformer.class, UnstructuredMode.BLOB.requiredTransformer());
    assertEquals(Collections.singleton(EmbeddingTableTransformer.class), UnstructuredMode.BLOB.excludedTransformers());
    assertEquals(BlobParquetConfigInjector.class.getName(), UnstructuredMode.BLOB.injectorClass());
    assertEquals(BlobParquetConfigInjector.DELEGATE_CLASS, UnstructuredMode.BLOB.injectorDelegateKey());

    assertEquals(EmbeddingTableSchema.KEY_COLUMNS, UnstructuredMode.EMBEDDING.keyColumns());
    assertEquals(EmbeddingTableTransformer.class, UnstructuredMode.EMBEDDING.requiredTransformer());
    assertEquals(Collections.singleton(BlobQuarantineTransformer.class), UnstructuredMode.EMBEDDING.excludedTransformers());
    assertEquals(EmbeddingParquetConfigInjector.class.getName(), UnstructuredMode.EMBEDDING.injectorClass());
    assertEquals(EmbeddingParquetConfigInjector.DELEGATE_CLASS, UnstructuredMode.EMBEDDING.injectorDelegateKey());
  }

  @Test
  void perCloudColumns() {
    assertEquals(CloudObjectsSelectorCommon.S3_OBJECT_KEY, UnstructuredCloud.S3.keyColumn());
    assertEquals(CloudObjectsSelectorCommon.S3_OBJECT_SIZE, UnstructuredCloud.S3.sizeColumn());
    assertEquals(CloudObjectsSelectorCommon.Type.S3, UnstructuredCloud.S3.selectorType());
    assertEquals(CloudObjectsSelectorCommon.GCS_OBJECT_KEY, UnstructuredCloud.GCS.keyColumn());
    assertEquals(CloudObjectsSelectorCommon.GCS_OBJECT_SIZE, UnstructuredCloud.GCS.sizeColumn());
    assertEquals(CloudObjectsSelectorCommon.Type.GCS, UnstructuredCloud.GCS.selectorType());
  }
}
