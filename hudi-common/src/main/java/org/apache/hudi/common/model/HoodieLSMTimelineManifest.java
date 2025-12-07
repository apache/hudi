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

package org.apache.hudi.common.model;

import org.apache.hudi.common.util.JsonUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Manifest entry for a version snapshot of the archived timeline.
 */
@Getter
@JsonIgnoreProperties(ignoreUnknown = true)
public class HoodieLSMTimelineManifest implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieLSMTimelineManifest.class);

  public static final HoodieLSMTimelineManifest EMPTY = new HoodieLSMTimelineManifest();

  private final List<LSMFileEntry> files;

  // for ser/deser
  public HoodieLSMTimelineManifest() {
    this.files = new ArrayList<>();
  }

  public HoodieLSMTimelineManifest(List<LSMFileEntry> files) {
    this.files = files;
  }

  public void addFile(String fileName, long fileLen) {
    this.files.add(LSMFileEntry.getInstance(fileName, fileLen));
  }

  public void addFile(LSMFileEntry fileEntry) {
    this.files.add(fileEntry);
  }

  public List<String> getFileNames() {
    return files.stream().map(LSMFileEntry::getFileName).collect(Collectors.toList());
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  public String toJsonString() throws IOException {
    return JsonUtils.getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }

  public static <T> T fromJsonString(String jsonStr, Class<T> clazz) throws Exception {
    if (jsonStr == null || jsonStr.isEmpty()) {
      // For empty commit file (no data or something bad happen).
      return clazz.newInstance();
    }
    return JsonUtils.getObjectMapper().readValue(jsonStr, clazz);
  }

  public HoodieLSMTimelineManifest copy(List<String> filesToRemove) {
    if (filesToRemove.isEmpty()) {
      return new HoodieLSMTimelineManifest(new ArrayList<>(this.files));
    }
    List<LSMFileEntry> newFiles = this.files.stream().filter(fileEntry -> !filesToRemove.contains(fileEntry.getFileName())).collect(Collectors.toList());
    return new HoodieLSMTimelineManifest(newFiles);
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------

  /**
   * A file entry.
   */
  @Getter
  public static class LSMFileEntry implements Serializable, Comparable<LSMFileEntry> {
    private String fileName;
    private long fileLen;

    // for ser/deser
    public LSMFileEntry() {
    }

    private LSMFileEntry(String fileName, long fileLen) {
      this.fileName = fileName;
      this.fileLen = fileLen;
    }

    public static LSMFileEntry getInstance(String fileName, long fileLen) {
      return new LSMFileEntry(fileName, fileLen);
    }

    @Override
    public int compareTo(LSMFileEntry other) {
      // sorts the files by order of min instant time in file name.
      return this.fileName.compareTo(other.fileName);
    }
  }
}
