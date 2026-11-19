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

package org.apache.hudi.utilities.sources.unstructured;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.StringUtils;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Reads a required, enumerated key of {@link UnstructuredSourceConfig}. */
final class UnstructuredSourceKeys {

  /** The source class every refusal of an unset or unknown value names. */
  static final String SOURCE = "UnstructuredDataSource";

  private UnstructuredSourceKeys() {
  }

  /**
   * The constant of {@code values} whose configured value equals the key's value after trim, compared exactly (lower case).
   *
   * @throws IllegalArgumentException naming the key and its values when the key is unset, blank or another value
   */
  static <E extends Enum<E>> E read(TypedProperties props, ConfigProperty<String> key, E[] values,
                                    Function<E, String> configValue) {
    Object raw = props.get(key.key());
    String configured = raw == null ? null : raw.toString().trim();
    if (!StringUtils.isNullOrEmpty(configured)) {
      for (E value : values) {
        if (configValue.apply(value).equals(configured)) {
          return value;
        }
      }
    }
    String allowed = Arrays.stream(values).map(configValue).collect(Collectors.joining(", "));
    throw new IllegalArgumentException(SOURCE + " requires " + key.key() + ", one of: " + allowed + "; configured: "
        + (StringUtils.isNullOrEmpty(configured) ? "none" : configured));
  }
}
