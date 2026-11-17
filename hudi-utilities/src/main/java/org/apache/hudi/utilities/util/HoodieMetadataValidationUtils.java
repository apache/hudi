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

package org.apache.hudi.utilities.util;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;

import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Helpers for the metadata table validator v2: the metadata table availability check, its
 * output-directory namespace, and the file-slice comparison rules it shares with v1.
 *
 * <p>{@link #assertBaseFilesEquality} and {@link FileSliceComparator} are the same rules
 * {@link org.apache.hudi.utilities.HoodieMetadataTableValidator} applies to latest file slices, so
 * the two validators agree on what makes two slices' base files equal and how slices are ordered.
 */
public class HoodieMetadataValidationUtils {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieMetadataValidationUtils.class);

  /**
   * The namespace this validator writes its own per-table output under.
   *
   * <p><b>It is a naming convention and not a guard.</b> Nothing skips a listing on the strength of
   * it, and nothing needs to: a directory is a partition <b>iff</b> it carries
   * {@code .hoodie_partition_metadata} ({@code HoodiePartitionMetadata.hasPartitionMetadata}, via
   * {@code FSUtils.getAllPartitionPaths}), and inside a partition only entries
   * {@code FSUtils.isBaseFile}/{@code isLogFile} accept are considered. A report directory is
   * neither, whatever it is called, and no prefix is consulted anywhere in the walk.
   *
   * <p>Two consumers, both of them naming rather than filtering:
   * {@code Config.DEFAULT_VALIDATION_OUTPUT_DIR} is built from it, so the default output directory
   * and this constant cannot drift apart; and {@code HoodieMetadataTableValidatorV2.validateConfig}
   * warns when reports are pointed under the table but outside it, on the grounds that an
   * unrecognised report directory under a table is usually a typo.
   */
  public static final String TOOLING_DIR_PREFIX = ".hoodie_validation";

  private HoodieMetadataValidationUtils() {
  }

  /**
   * Whether the metadata table can be validated: it exists and has a completed instant. Returns
   * false (skip) when neither table has a completed commit, or when the metadata table cannot be
   * read yet; throws when the data table has completed commits but the metadata table has none.
   * The same check {@code HoodieMetadataTableValidator} runs before validating.
   */
  public static boolean checkMetadataTableIsAvailable(JavaSparkContext jsc, String basePath,
                                               HoodieTableMetaClient dataMetaClient, String taskLabels) {
    try {
      HoodieTableMetaClient mdtMetaClient = HoodieTableMetaClient.builder()
          .setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()))
          .setBasePath(new Path(basePath, HoodieTableMetaClient.METADATA_TABLE_FOLDER_PATH).toString())
          .setLoadActiveTimelineOnLoad(true)
          .build();
      int finishedInstants = mdtMetaClient.getCommitsTimeline().filterCompletedInstants().countInstants();
      if (finishedInstants == 0) {
        if (dataMetaClient.getCommitsTimeline().filterCompletedInstants().countInstants() == 0) {
          LOG.info("There is no completed commit in both metadata table and corresponding data table: {}", taskLabels);
          return false;
        } else {
          throw new HoodieValidationException("There is no completed instant for metadata table: " + basePath);
        }
      }
      return true;
    } catch (TableNotFoundException tbe) {
      // Suppress the TableNotFound exception if Metadata table is not available to read for now
      LOG.warn("Metadata table is not found for table: {}. Skip current validation.", basePath);
      return false;
    } catch (Exception ex) {
      LOG.warn("Metadata table is not available to read for now for table: {}, ", basePath, ex);
      return false;
    }
  }
  /**
   * Whether two file slices carry the same base file (name, file id and size), or both carry none.
   */
  static boolean assertBaseFilesEquality(FileSlice fileSlice1, FileSlice fileSlice2) {
    if (fileSlice1.getBaseFile().isPresent() && fileSlice2.getBaseFile().isPresent()) {
      HoodieBaseFile baseFile1 = fileSlice1.getBaseFile().get();
      HoodieBaseFile baseFile2 = fileSlice2.getBaseFile().get();
      return baseFile1.getFileName().equals(baseFile2.getFileName()) && baseFile1.getFileId().equals(baseFile2.getFileId())
          && baseFile1.getFileSize() == baseFile2.getFileSize();
    } else {
      return fileSlice1.getBaseFile().isPresent() == fileSlice2.getBaseFile().isPresent();
    }
  }

  /** Orders file slices by partition path, file id and base instant. */
  public static class FileSliceComparator implements Comparator<FileSlice>, Serializable {

    @Override
    public int compare(FileSlice o1, FileSlice o2) {
      return (o1.getPartitionPath() + o1.getFileId() + o1.getBaseInstantTime())
          .compareTo(o2.getPartitionPath() + o2.getFileId() + o2.getBaseInstantTime());
    }
  }
}
