/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.timeline.service.handlers;

import org.apache.hudi.common.table.timeline.dto.CkpMetadataDTO;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.timeline.service.TimelineService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * REST Handler servicing flink checkpoint metadata requests.
 */
public class FlinkCkpMetadataHandler extends Handler {

  private static final Logger LOG = LoggerFactory.getLogger(MarkerHandler.class);

  private static final String BASE_URL = "/v1/hoodie/flinkckpmetadata";

  public static final String CKP_METADATA_DIR_PATH_PARAM = "ckpmetdatadirpath";

  // GET requests
  public static final String ALL_CKP_METADATA_URL = String.format("%s/%s", BASE_URL, "all");

  // POST requests
  public static final String REFRESH_CKP_METADATA = String.format("%s/%s", BASE_URL, "refresh/");

  private ConcurrentHashMap<String, List<CkpMetadataDTO>> cachedCkpMetadata;

  private AtomicLong requestCount;

  public FlinkCkpMetadataHandler(Configuration conf, TimelineService.Config timelineServiceConfig, FileSystem fileSystem,
                                 FileSystemViewManager viewManager) throws IOException {
    super(conf, timelineServiceConfig, fileSystem, viewManager);

    this.cachedCkpMetadata = new ConcurrentHashMap<>();
    this.requestCount = new AtomicLong();
  }

  public List<CkpMetadataDTO> getAllCkpMessage(String ckpMetaPath) {
    if (requestCount.incrementAndGet() % 3000L == 0) {
      refresh(ckpMetaPath);
    }
    return cachedCkpMetadata.computeIfAbsent(ckpMetaPath, k -> scanCkpMetadata(new Path(k)));
  }

  public boolean refresh(String ckpMetaPath) {
    try {
      cachedCkpMetadata.put(ckpMetaPath, scanCkpMetadata(new Path(ckpMetaPath)));
    } catch (Exception e) {
      LOG.error("Failed to load ckp metadata, path: " + ckpMetaPath, e);
      return false;
    }
    return true;
  }

  public List<CkpMetadataDTO> scanCkpMetadata(Path ckpMetaPath) {
    try {
      // Check ckpMetaPath exists before list status, see HUDI-5915
      if (this.fileSystem.exists(ckpMetaPath)) {
        return Arrays.stream(this.fileSystem.listStatus(ckpMetaPath)).map(CkpMetadataDTO::fromFileStatus).collect(Collectors.toList());
      } else {
        return Collections.emptyList();
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to load ckp metadata, path: " + ckpMetaPath, e);
    }
  }

}
