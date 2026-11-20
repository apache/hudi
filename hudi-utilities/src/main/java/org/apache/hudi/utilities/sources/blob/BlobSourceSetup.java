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

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.io.hadoop.HoodieBaseParquetWriter;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.sources.UnstructuredDataSource;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredMode;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSourceContract;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSourcePolicy;

import org.apache.spark.SparkConf;

/**
 * Construction-time validation of the job of {@link UnstructuredDataSource}, the blob job or the embedding job
 * ({@link UnstructuredMode}). It only checks the job's properties and changes none of them. The settings each job runs with
 * (the record key, the Parquet config injector, and the embedding job's markers) are the source's contract's
 * ({@link UnstructuredSourceContract#prepareJob}), merged in by the streamer before the source is built; this checks the
 * others, and refuses a value the job cannot run with, naming the setting and the source with its mode.
 */
public final class BlobSourceSetup {

  /** The streamer's row-writer switch for BULK_INSERT. */
  static final String ROW_WRITER_ENABLE = "hoodie.streamer.write.row.writer.enable";

  private BlobSourceSetup() {
  }

  /**
   * Validates the job's properties for {@code mode} while its source is built, and returns them unchanged. Refusals name
   * {@code source}, the source with its mode.
   *
   * @throws IllegalArgumentException    when a setting has a value the job cannot run with
   * @throws HoodieValidationException   when the job's table version is below the tables' floor (9)
   */
  public static TypedProperties validate(TypedProperties props, UnstructuredMode mode, String source) {
    UnstructuredSourcePolicy.fileExtensions(props, source);
    if (ConfigUtils.getBooleanWithAltKeys(props, CloudSourceConfig.ENABLE_EXISTS_CHECK)) {
      throw new IllegalArgumentException(CloudSourceConfig.ENABLE_EXISTS_CHECK.key() + " is not supported by " + source
          + "; source paths must be checked before storage can normalize or filter them");
    }


    BlobInlineCeiling.inlineMaxBytes(props);
    if (mode == UnstructuredMode.BLOB) {
      BlobParquetConfigInjector.checkLegacyStatisticsBudget(props,
          new HoodieConfig(props).getIntOrDefault(HoodieStorageConfig.PARQUET_BLOCK_SIZE),
          HoodieBaseParquetWriter.supportsColumnStatisticsSwitch());
    }

    // the blob table is written, and its blob column checked, only as Avro records encoded with the table schema
    if (props.getBoolean(ROW_WRITER_ENABLE, false)) {
      throw new IllegalArgumentException(ROW_WRITER_ENABLE + " is not supported by " + source + "; blob rows are "
          + "written as Avro records");
    }
    if (UtilHelpers.createRecordMerger(props).getRecordType() == HoodieRecordType.SPARK) {
      throw new IllegalArgumentException("Spark records are not supported by " + source + "; blob rows are written "
          + "as Avro records: remove the Spark merger from " + HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key());
    }
    // both tables are at table version 9 or above; the embedding transformer also checks an existing table's version, which
    // needs the table's path, at its first round
    checkTableVersion(props, source);
    mode.validate(props, source);
    return props;
  }

  /** Checks the driver inline ceiling at fetch, before any source task or instant. */
  public static void check(TypedProperties props, SparkConf sparkConf) {
    BlobInlineCeiling.checkOnDriver(props, sparkConf);
  }

  /** Refuses a job's {@code hoodie.write.table.version} below the version-9 floor of both jobs' tables before any source bytes are read. */
  static void checkTableVersion(TypedProperties props, String source) {
    int minimum = HoodieTableVersion.NINE.versionCode();
    int version;
    try {
      version = ConfigUtils.getIntWithAltKeys(props, HoodieWriteConfig.WRITE_TABLE_VERSION);
    } catch (NumberFormatException failure) {
      throw new HoodieValidationException(source + " refuses " + HoodieWriteConfig.WRITE_TABLE_VERSION.key() + "="
          + props.getProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key()) + ": not a table version; unset it or set " + minimum + " or above");
    }
    if (version < minimum) {
      throw new HoodieValidationException(source + " refuses " + HoodieWriteConfig.WRITE_TABLE_VERSION.key() + "="
          + version + ": blob and embedding tables are at table version " + minimum + " or above; unset it or set " + minimum + " or above");
    }
  }
}
