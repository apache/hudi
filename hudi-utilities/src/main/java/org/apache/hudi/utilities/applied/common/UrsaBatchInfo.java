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

package org.apache.hudi.utilities.applied.common;

import org.apache.hudi.common.util.JsonUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class UrsaBatchInfo implements Serializable {
  @JsonProperty("created_timestamp_s")
  private long createdTimestampSec;
  @JsonProperty("origin_created_timestamp_s")
  private long originCreatedTimestampSec;
  @JsonProperty("origin_updated_timestamp_s")
  private long originUpdatedTimestampSec;
  @JsonProperty("source_type")
  private String sourceType;
  @JsonProperty("uuid")
  private String uuid;
  @JsonProperty("schema_info")
  private UrsaBatchSchemaInfo schemaInfo;
  @JsonProperty("files_format")
  private String filesFormat;
  @JsonProperty("files")
  private List<String> files;

  private static final long serialVersionUID = 1L;

  public UrsaBatchInfo() {
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UrsaBatchInfo that = (UrsaBatchInfo) o;
    return Objects.equals(sourceType, that.sourceType) && Objects.equals(uuid, that.uuid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sourceType, uuid);
  }

  public long getCreatedTimestampSec() {
    return createdTimestampSec;
  }

  public void setCreatedTimestampSec(long createdTimestampSec) {
    this.createdTimestampSec = createdTimestampSec;
  }

  public String getSourceType() {
    return sourceType;
  }

  public void setSourceType(String sourceType) {
    this.sourceType = sourceType;
  }

  public String getUuid() {
    return uuid;
  }

  public void setUuid(String uuid) {
    this.uuid = uuid;
  }

  public UrsaBatchSchemaInfo getSchemaInfo() {
    return schemaInfo;
  }

  public void setSchemaInfo(UrsaBatchSchemaInfo schemaInfo) {
    this.schemaInfo = schemaInfo;
  }

  public String getFilesFormat() {
    return filesFormat;
  }

  public void setFilesFormat(String filesFormat) {
    this.filesFormat = filesFormat;
  }

  public List<String> getFiles() {
    return files;
  }

  public void setFiles(List<String> files) {
    this.files = files;
  }

  public long getOriginCreatedTimestampSec() {
    return originCreatedTimestampSec;
  }

  public void setOriginCreatedTimestampSec(long originCreatedTimestampSec) {
    this.originCreatedTimestampSec = originCreatedTimestampSec;
  }

  public long getOriginUpdatedTimestampSec() {
    return originUpdatedTimestampSec;
  }

  public void setOriginUpdatedTimestampSec(long originUpdatedTimestampSec) {
    this.originUpdatedTimestampSec = originUpdatedTimestampSec;
  }

  public static UrsaBatchInfo fromJsonString(String jsonStr) throws Exception {
    if (jsonStr == null || jsonStr.isEmpty()) {
      return null;
    }
    return JsonUtils.getObjectMapper().readValue(jsonStr, UrsaBatchInfo.class);
  }
}