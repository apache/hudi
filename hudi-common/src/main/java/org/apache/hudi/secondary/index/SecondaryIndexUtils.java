/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.secondary.index;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.exception.HoodieSecondaryIndexException;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.util.SerializationUtil;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.util.List;

public class SecondaryIndexUtils {
  /**
   * key to configure the specific row id set to read
   */
  public static final String SPECIFIC_ROW_ID_SET = "parquet.read.specific.row.id.set";

  public static void setSpecificRowIdSet(Configuration configuration, RoaringBitmap rowIdSet) {
    try {
      SerializationUtil.writeObjectToConfAsBase64(SPECIFIC_ROW_ID_SET, rowIdSet, configuration);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static RoaringBitmap getSpecificRowIdSet(Configuration configuration) {
    try {
      return SerializationUtil.readObjectFromConfAsBase64(SPECIFIC_ROW_ID_SET, configuration);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get secondary index metadata for this table
   *
   * @param metaClient HoodieTableMetaClient
   * @return HoodieSecondaryIndex List
   */
  public static Option<List<HoodieSecondaryIndex>> getSecondaryIndexes(HoodieTableMetaClient metaClient) {
    Option<String> indexesMetadata = metaClient.getTableConfig().getSecondaryIndexesMetadata();
    return indexesMetadata.map(SecondaryIndexUtils::fromJsonString);
  }

  /**
   * Parse secondary index str to List<HoodieSecondaryIndex>
   *
   * @param jsonStr Secondary indexes with json format
   * @return List<HoodieSecondaryIndex>
   */
  public static List<HoodieSecondaryIndex> fromJsonString(String jsonStr) {
    return SecondaryIndexUtils.fromJsonString(jsonStr,
        new TypeReference<List<HoodieSecondaryIndex>>() {
        });
  }

  public static String toJsonString(Object value) {
    try {
      return getObjectMapper().writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new HoodieIndexException("Fail to convert object to json string", e);
    }
  }

  public static <T> T fromJsonString(String jsonStr, TypeReference<T> type) {
    if (jsonStr == null || jsonStr.isEmpty()) {
      return null;
    }

    try {
      return getObjectMapper().readValue(jsonStr, type);
    } catch (IOException e) {
      throw new HoodieSecondaryIndexException("Fail to parse json string");
    }
  }

  public static ObjectMapper getObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    return mapper;
  }
}
