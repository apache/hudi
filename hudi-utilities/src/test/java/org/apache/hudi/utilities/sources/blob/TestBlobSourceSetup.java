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

package org.apache.hudi.utilities.sources.blob;

import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.utilities.sources.UnstructuredDataSource;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredMode;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The construction-time validation of each job's properties by the unstructured data source, which never changes them. The settings
 * the source declares for each job are merged and refused by the streamer ({@code TestUnstructuredSourceContract}).
 */
public class TestBlobSourceSetup {

  /**
   * Properties of {@code job} as the streamer hands them to its source: one of the settings it merges in, the round's instant
   * handed to the transformer. Validation does not check it; it is here so each case reads as a real job.
   */
  private static TypedProperties job(UnstructuredMode job) {
    TypedProperties props = new TypedProperties();
    return props;
  }

  /** Validation changes no property: the writer settings in a full blob job's properties are exactly as the job set them. */
  @ParameterizedTest
  @EnumSource(UnstructuredMode.class)
  public void validationChangesNoProperty(UnstructuredMode job) {
    TypedProperties props = job(job);
    props.setProperty(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME.key(), "zstd");
    props.setProperty(HoodieStorageConfig.PARQUET_WITH_BLOOM_FILTER_ENABLED.key(), "true");
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), " source_path_sha ");
    TypedProperties before = TypedProperties.copy(props);
    BlobSourceSetup.validate(props, job, UnstructuredDataSource.name(job));
    assertEquals(before, props);
    TypedProperties minimal = job(job);
    BlobSourceSetup.validate(minimal, job, UnstructuredDataSource.name(job));
    assertEquals(job(job), minimal, "validation adds nothing");
  }

  /** Existence filtering runs before the source can refuse a key that storage would normalize. */
  @ParameterizedTest
  @ValueSource(strings = {"hoodie.streamer.source.cloud.data.check.file.exists", "hoodie.deltastreamer.source.cloud.data.check.file.exists"})
  public void existenceFilteringIsRefusedBeforeItCanSkipAnUnsupportedPath(String key) {
    for (UnstructuredMode mode : UnstructuredMode.values()) {
      TypedProperties props = job(mode);
      props.setProperty(key, "true");
      IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
          () -> BlobSourceSetup.validate(props, mode, UnstructuredDataSource.name(mode)));
      assertTrue(refused.getMessage().contains("check.file.exists"), refused.getMessage());
      props.setProperty(key, "false");
      BlobSourceSetup.validate(props, mode, UnstructuredDataSource.name(mode));
    }
  }

  /** Every refusal left at source construction names the source with the job's mode. */
  @ParameterizedTest(name = "{0} {1}={2}")
  @CsvSource(delimiter = '|', value = {
      "BLOB|hoodie.streamer.write.row.writer.enable|true",
      "BLOB|hoodie.streamer.source.cloud.data.datafile.format|parquet",
      "BLOB|hoodie.blob.table.file.extensions|parquet",
      "BLOB|hoodie.write.record.merge.custom.implementation.classes|org.apache.hudi.OverwriteWithLatestSparkRecordMerger"})
  public void refusalNamesTheJobsSourceClass(UnstructuredMode job, String key, String value) {
    TypedProperties props = job(job);
    props.setProperty(key, value);
    RuntimeException refused = assertThrows(RuntimeException.class, () -> BlobSourceSetup.validate(props, job, UnstructuredDataSource.name(job)));
    assertTrue(refused.getMessage().contains(UnstructuredDataSource.name(job)), refused.getMessage());
  }
}
