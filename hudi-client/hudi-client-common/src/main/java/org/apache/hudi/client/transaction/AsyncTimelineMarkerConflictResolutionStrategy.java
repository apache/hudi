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

package org.apache.hudi.client.transaction;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.table.marker.TimelineServerBasedWriteMarkers;
import org.apache.hudi.table.marker.WriteMarkers;

import java.io.IOException;
import java.util.ConcurrentModificationException;

/**
 * This strategy is used for timeline server based marker writers, trying to do early conflict detection.
 * It will call timeline server /v1/hoodie/marker/check-marker-conflict API to check if there is any marker conflict.
 */
public class AsyncTimelineMarkerConflictResolutionStrategy extends SimpleConcurrentFileWritesConflictResolutionStrategy {

  @Override
  public boolean hasMarkerConflict(WriteMarkers writeMarkers, HoodieWriteConfig config, FileSystem fs , String partitionPath, String fileId) {
    try {
      assert writeMarkers instanceof TimelineServerBasedWriteMarkers;
      return writeMarkers.hasMarkerConflict(config, partitionPath, fileId);
    } catch (IOException e) {
      throw new HoodieIOException("IOException occurs during checking markers conflict");
    }
  }

  @Override
  public Option<HoodieCommitMetadata> resolveMarkerConflict(WriteMarkers writeMarkers, String partitionPath, String dataFileName) throws HoodieWriteConflictException {
    throw new HoodieWriteConflictException(new ConcurrentModificationException("Cannot resolve conflicts for overlapping writes"));
  }
}
