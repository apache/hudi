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

package org.apache.hudi.client;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.Path;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieRollingStatMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.TableFileSystemView;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Stream;

/**
 * Operates on marker files for a given write action (commit, delta commit, compaction).
 */
public class ReplaceArchivalHelper implements Serializable {

  private static final Logger LOG = LogManager.getLogger(ReplaceArchivalHelper.class);

  /**
   * Convert json metadata to avro format.
   */
  public static org.apache.hudi.avro.model.HoodieReplaceCommitMetadata convertReplaceCommitMetadata(
      HoodieReplaceCommitMetadata hoodieReplaceCommitMetadata) {
    ObjectMapper mapper = new ObjectMapper();
    // Need this to ignore other public get() methods
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    org.apache.hudi.avro.model.HoodieReplaceCommitMetadata avroMetaData =
        mapper.convertValue(hoodieReplaceCommitMetadata, org.apache.hudi.avro.model.HoodieReplaceCommitMetadata.class);

    // Do not archive Rolling Stats, cannot set to null since AVRO will throw null pointer
    avroMetaData.getExtraMetadata().put(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY, "");
    return avroMetaData;
  }

  /**
   * Delete all files represented by FileSlices in parallel. Return true if all files are deleted successfully.
   */
  public static boolean deleteReplacedFileGroups(HoodieEngineContext context, HoodieTableMetaClient metaClient,
                                                 TableFileSystemView fileSystemView,
                                                 HoodieInstant instant, List<String> replacedPartitions) {
    context.setJobStatus(ReplaceArchivalHelper.class.getSimpleName(), "Delete replaced file groups");
    List<Boolean> f = context.map(replacedPartitions, partition -> {
      Stream<FileSlice> fileSlices =  fileSystemView.getReplacedFileGroupsBeforeOrOn(instant.getTimestamp(), partition)
          .flatMap(HoodieFileGroup::getAllRawFileSlices);
      return fileSlices.allMatch(slice -> deleteFileSlice(slice, metaClient, instant));
    }, replacedPartitions.size());

    return f.stream().reduce((x, y) -> x & y).orElse(true);
  }

  private static boolean deleteFileSlice(FileSlice fileSlice, HoodieTableMetaClient metaClient, HoodieInstant instant) {
    boolean baseFileDeleteSuccess = fileSlice.getBaseFile().map(baseFile ->
        deletePath(new Path(baseFile.getPath()), metaClient, instant)).orElse(true);

    boolean logFileSuccess = fileSlice.getLogFiles().map(logFile ->
        deletePath(logFile.getPath(), metaClient, instant)).allMatch(x -> x);
    return baseFileDeleteSuccess & logFileSuccess;
  }

  private static boolean deletePath(Path path, HoodieTableMetaClient metaClient, HoodieInstant instant) {
    try {
      LOG.info("Deleting " + path + " before archiving " + instant);
      metaClient.getFs().delete(path);
      return true;
    } catch (IOException e) {
      LOG.error("unable to delete file groups that are replaced", e);
      return false;
    }
  }
}
