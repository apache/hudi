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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.sources.UnstructuredDataSource;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredMode;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSourceContract;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSourcePolicy;

/**
 * Construction-time validation of the job of {@link UnstructuredDataSource}, the blob job or the embedding job
 * ({@link UnstructuredMode}). It only checks the job's properties and changes none of them. The settings each job runs with
 * (the record key and the embedding job's markers) are the source's contract's
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
   */
  public static TypedProperties validate(TypedProperties props, UnstructuredMode mode, String source) {
    UnstructuredSourcePolicy.fileExtensions(props, source);
    if (ConfigUtils.getBooleanWithAltKeys(props, CloudSourceConfig.ENABLE_EXISTS_CHECK)) {
      throw new IllegalArgumentException(CloudSourceConfig.ENABLE_EXISTS_CHECK.key() + " is not supported by " + source
          + "; source paths must be checked before storage can normalize or filter them");
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
    mode.validate(props, source);
    return props;
  }
}
