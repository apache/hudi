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

import org.apache.hudi.OverwriteWithLatestSparkRecordMerger;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.OverwriteWithLatestMerger;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.utilities.sources.UnstructuredDataSource;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredMode;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingParquetConfigInjector;

import org.junit.jupiter.api.Test;
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

  private static final String INJECTOR_KEY = HoodieStorageConfig.HOODIE_PARQUET_CONFIG_INJECTOR_CLASS.key();

  /** The injector each job's properties name. */
  private static String injectorOf(UnstructuredMode job) {
    return job == UnstructuredMode.BLOB ? BlobParquetConfigInjector.class.getName() : EmbeddingParquetConfigInjector.class.getName();
  }

  /**
   * Properties of {@code job} as the streamer hands them to its source: two of the settings it merges in, the round's instant
   * handed to the transformer and the job's injector. Validation checks neither; they are here so each case reads as a real job.
   */
  private static TypedProperties job(UnstructuredMode job) {
    TypedProperties props = new TypedProperties();
    props.setProperty(INJECTOR_KEY, injectorOf(job));
    if (job == UnstructuredMode.EMBEDDING) {
      props.setProperty("hoodie.unstructured.embedding.mode", "prep_only");
    }
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

  /** Both jobs' tables are at table version 9 or above: a lower or non-numeric job version is refused before any read. */
  @ParameterizedTest
  @CsvSource({"BLOB,6", "BLOB,8", "BLOB,invalid", "EMBEDDING,6", "EMBEDDING,8", "EMBEDDING,invalid"})
  public void unsupportedJobTableVersionIsRefused(UnstructuredMode job, String version) {
    TypedProperties props = job(job);
    props.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), version);
    HoodieValidationException failure = assertThrows(HoodieValidationException.class, () -> BlobSourceSetup.validate(props, job, UnstructuredDataSource.name(job)));
    assertTrue(failure.getMessage().contains(HoodieWriteConfig.WRITE_TABLE_VERSION.key() + "=" + version), failure.getMessage());
    assertTrue(failure.getMessage().contains("9 or above"), failure.getMessage());
  }

  /** Version 9 and above, or no version (Hudi's current one), pass for both jobs. */
  @ParameterizedTest
  @EnumSource(UnstructuredMode.class)
  public void supportedJobTableVersionIsAccepted(UnstructuredMode job) {
    BlobSourceSetup.validate(job(job), job, UnstructuredDataSource.name(job));
    TypedProperties nine = job(job);
    nine.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "9");
    BlobSourceSetup.validate(nine, job, UnstructuredDataSource.name(job));
  }

  /** The blob job copies nothing, so the former copy buffer and copy retry keys are read by neither job. */
  @ParameterizedTest
  @ValueSource(strings = {"hoodie.blob.table.copy.buffer.bytes", "hoodie.blob.table.copy.retry.max.attempts",
      "hoodie.blob.table.copy.retry.backoff.ms"})
  public void formerCopySettingsAreNotRead(String key) {
    for (UnstructuredMode job : UnstructuredMode.values()) {
      TypedProperties props = job(job);
      props.setProperty(key, "-1");
      BlobSourceSetup.validate(props, job, UnstructuredDataSource.name(job));
    }
  }

  /** The blob table is written, and its blob column checked, only as Avro records encoded with the table schema. */
  @ParameterizedTest
  @EnumSource(UnstructuredMode.class)
  public void rowWriterIsRefused(UnstructuredMode job) {
    TypedProperties props = job(job);
    props.setProperty("hoodie.streamer.write.row.writer.enable", "true");
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> BlobSourceSetup.validate(props, job, UnstructuredDataSource.name(job)));
    assertTrue(e.getMessage().startsWith("hoodie.streamer.write.row.writer.enable is not supported by " + UnstructuredDataSource.name(job)),
        e.getMessage());
  }

  /** Spark records are not encoded with the table schema either. */
  @Test
  public void sparkRecordMergerIsRefused() {
    TypedProperties props = job(UnstructuredMode.BLOB);
    props.setProperty(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), OverwriteWithLatestSparkRecordMerger.class.getName());
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> BlobSourceSetup.validate(props, UnstructuredMode.BLOB, UnstructuredDataSource.name(UnstructuredMode.BLOB)));
    assertTrue(e.getMessage().startsWith("Spark records are not supported by UnstructuredDataSource (mode blob)"), e.getMessage());
  }

  @Test
  public void avroRecordMergerAndDisabledRowWriterAreAccepted() {
    TypedProperties props = job(UnstructuredMode.BLOB);
    props.setProperty("hoodie.streamer.write.row.writer.enable", "false");
    props.setProperty(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key(), OverwriteWithLatestMerger.class.getName());
    BlobSourceSetup.validate(props, UnstructuredMode.BLOB, UnstructuredDataSource.name(UnstructuredMode.BLOB));
  }

  /** Every refusal left at source construction names the source with the job's mode. */
  @ParameterizedTest(name = "{0} {1}={2}")
  @CsvSource(delimiter = '|', value = {
      "BLOB|hoodie.streamer.write.row.writer.enable|true",
      "BLOB|hoodie.streamer.source.cloud.data.datafile.format|parquet",
      "BLOB|hoodie.blob.table.file.extensions|parquet",
      "BLOB|hoodie.write.table.version|8",
      "BLOB|hoodie.write.record.merge.custom.implementation.classes|org.apache.hudi.OverwriteWithLatestSparkRecordMerger",
      "EMBEDDING|hoodie.streamer.write.row.writer.enable|true",
      "EMBEDDING|hoodie.streamer.source.cloud.data.datafile.format|parquet",
      "EMBEDDING|hoodie.blob.table.file.extensions|parquet",
      "EMBEDDING|hoodie.write.table.version|8",
      "EMBEDDING|hoodie.write.record.merge.custom.implementation.classes|org.apache.hudi.OverwriteWithLatestSparkRecordMerger"})
  public void refusalNamesTheJobsSourceClass(UnstructuredMode job, String key, String value) {
    TypedProperties props = job(job);
    props.setProperty(key, value);
    RuntimeException refused = assertThrows(RuntimeException.class, () -> BlobSourceSetup.validate(props, job, UnstructuredDataSource.name(job)));
    assertTrue(refused.getMessage().contains(UnstructuredDataSource.name(job)), refused.getMessage());
  }
}
