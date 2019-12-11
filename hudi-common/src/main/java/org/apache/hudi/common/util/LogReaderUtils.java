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

package org.apache.hudi.common.util;

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.HoodieLogFormat.Reader;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utils class for performing various log file reading operations.
 */
public class LogReaderUtils {

  private static Schema readSchemaFromLogFileInReverse(FileSystem fs, HoodieActiveTimeline activeTimeline, Path path)
      throws IOException {
    Reader reader = HoodieLogFormat.newReader(fs, new HoodieLogFile(path), null, true, true);
    Schema writerSchema = null;
    HoodieTimeline completedTimeline = activeTimeline.getCommitsTimeline().filterCompletedInstants();
    while (reader.hasPrev()) {
      HoodieLogBlock block = reader.prev();
      if (block instanceof HoodieAvroDataBlock && block != null) {
        HoodieAvroDataBlock lastBlock = (HoodieAvroDataBlock) block;
        if (completedTimeline
            .containsOrBeforeTimelineStarts(lastBlock.getLogBlockHeader().get(HeaderMetadataType.INSTANT_TIME))) {
          writerSchema = new Schema.Parser().parse(lastBlock.getLogBlockHeader().get(HeaderMetadataType.SCHEMA));
          break;
        }
      }
    }
    reader.close();
    return writerSchema;
  }

  public static Schema readLatestSchemaFromLogFiles(String basePath, List<String> deltaFilePaths, JobConf jobConf)
      throws IOException {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jobConf, basePath);
    List<String> deltaPaths = deltaFilePaths.stream().map(s -> new HoodieLogFile(new Path(s)))
        .sorted(HoodieLogFile.getReverseLogFileComparator()).map(s -> s.getPath().toString())
        .collect(Collectors.toList());
    if (deltaPaths.size() > 0) {
      for (String logPath : deltaPaths) {
        FileSystem fs = FSUtils.getFs(logPath, jobConf);
        Schema schemaFromLogFile =
            readSchemaFromLogFileInReverse(fs, metaClient.getActiveTimeline(), new Path(logPath));
        if (schemaFromLogFile != null) {
          return schemaFromLogFile;
        }
      }
    }
    return null;
  }

}
