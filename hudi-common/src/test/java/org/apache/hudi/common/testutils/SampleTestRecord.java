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

package org.apache.hudi.common.testutils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A sample record entity for tests.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings({"unused", "FieldCanBeLocal", "MismatchedQueryAndUpdateOfCollection"})
public class SampleTestRecord implements Serializable {

  class TestMapItemRecord implements Serializable {

    private String item1;
    private String item2;

    TestMapItemRecord(String item1, String item2) {
      this.item1 = item1;
      this.item2 = item2;
    }
  }

  class TestNestedRecord implements Serializable {

    private boolean isAdmin;
    private String userId;

    TestNestedRecord(boolean isAdmin, String userId) {
      this.isAdmin = isAdmin;
      this.userId = userId;
    }
  }

  private String _hoodie_commit_time;
  private String _hoodie_record_key;
  private String _hoodie_partition_path;
  private String _hoodie_file_name;
  private String _hoodie_commit_seqno;

  private String field1;
  private String field2;
  private String name;
  private Integer favoriteIntNumber;
  private Long favoriteNumber;
  private Float favoriteFloatNumber;
  private Double favoriteDoubleNumber;
  private Map<String, TestMapItemRecord> tags;
  private TestNestedRecord testNestedRecord;
  private String[] stringArray;

  public SampleTestRecord(String instantTime, int recordNumber, String fileId) {
    this(instantTime, recordNumber, fileId, true);
  }

  public SampleTestRecord(String instantTime, int recordNumber, String fileId, boolean populateMetaFields) {
    if (populateMetaFields) {
      this._hoodie_commit_time = instantTime;
      this._hoodie_record_key = "key" + recordNumber;
      this._hoodie_partition_path = instantTime;
      this._hoodie_file_name = fileId;
      this._hoodie_commit_seqno = instantTime + recordNumber;
    }

    String commitTimeSuffix = "@" + instantTime;
    int commitHashCode = instantTime.hashCode();

    this.field1 = "field" + recordNumber;
    this.field2 = "field" + recordNumber + commitTimeSuffix;
    this.name = "name" + recordNumber;
    this.favoriteIntNumber = recordNumber + commitHashCode;
    this.favoriteNumber = (long) (recordNumber + commitHashCode);
    this.favoriteFloatNumber = (float) ((recordNumber + commitHashCode) / 1024.0);
    this.favoriteDoubleNumber = (recordNumber + commitHashCode) / 1024.0;
    this.tags = new HashMap<>();
    this.tags.put("mapItem1", new TestMapItemRecord("item" + recordNumber, "item" + recordNumber + commitTimeSuffix));
    this.tags.put("mapItem2", new TestMapItemRecord("item2" + recordNumber, "item2" + recordNumber + commitTimeSuffix));
    this.testNestedRecord = new TestNestedRecord(false, "UserId" + recordNumber + commitTimeSuffix);
    this.stringArray = new String[] {"stringArray0" + commitTimeSuffix, "stringArray1" + commitTimeSuffix};
  }

  public String toJsonString() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
  }
}
