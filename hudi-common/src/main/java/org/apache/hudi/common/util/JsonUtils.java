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

package org.apache.hudi.common.util;

import org.apache.hudi.exception.HoodieIOException;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Utils for JSON serialization and deserialization.
 */
public class JsonUtils {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    // We need to exclude custom getters, setters and creators which can use member fields
    // to derive new fields, so that they are not included in the serialization
    MAPPER.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    MAPPER.setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.NONE);
    MAPPER.setVisibility(PropertyAccessor.IS_GETTER, JsonAutoDetect.Visibility.NONE);
    MAPPER.setVisibility(PropertyAccessor.SETTER, JsonAutoDetect.Visibility.NONE);
    MAPPER.setVisibility(PropertyAccessor.CREATOR, JsonAutoDetect.Visibility.NONE);
    // NOTE: Registering [[JavaTimeModule]] is required for Jackson >= 2.11 (Spark >= 3.3)
    MAPPER.registerModule(new JavaTimeModule());
  }

  public static ObjectMapper getObjectMapper() {
    return MAPPER;
  }

  public static String toString(Object value) {
    try {
      return MAPPER.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new HoodieIOException(
          "Fail to convert the class: " + value.getClass().getName() + " to Json String", e);
    }
  }

  public static void registerModules() {
    // NOTE: Registering [[JavaTimeModule]] is required for Jackson >= 2.11 (Spark >= 3.3)
    MAPPER.registerModules(new JavaTimeModule());
  }
}
