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
import org.apache.hudi.exception.HoodieEarlyConflictException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ConcurrentModificationException;

/**
 * This strategy is used for direct marker writers, trying to do early conflict detection.
 * It will use fileSystem api like list and exist directly to check if there is any marker file conflict.
 */
public class SimpleDirectMarkerConflictResolutionStrategy extends SimpleConcurrentFileWritesConflictResolutionStrategy {
  private static final Logger LOG = LogManager.getLogger(SimpleDirectMarkerConflictResolutionStrategy.class);

  @Override
  public boolean hasMarkerConflict(WriteMarkers writeMarkers, HoodieWriteConfig config, FileSystem fs, String partitionPath, String fileId) {
    try {
      return writeMarkers.hasMarkerConflict(config, partitionPath, fileId);
    } catch (IOException e) {
      LOG.warn("Exception occurs during create marker file in eager conflict detection mode.");
      throw new HoodieIOException("Exception occurs during create marker file in eager conflict detection mode.", e);
    }
  }

  @Override
  public Option<HoodieCommitMetadata> resolveMarkerConflict(WriteMarkers writeMarkers, String partitionPath, String dataFileName) {
    throw new HoodieEarlyConflictException(new ConcurrentModificationException("Early conflict detected but cannot resolve conflicts for overlapping writes"));
  }
}
