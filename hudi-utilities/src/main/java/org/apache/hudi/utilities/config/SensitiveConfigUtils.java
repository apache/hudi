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

package org.apache.hudi.utilities.config;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Masks the values of sensitive configs (credentials, SASL / SSL settings, auth info) so that a
 * config map can be logged without leaking secrets.
 *
 * <p>A config key is sensitive when it contains any of the filters configured in
 * {@link HoodieWriteConfig#SENSITIVE_CONFIG_KEYS_FILTER}, e.g. {@code sasl.jaas.config} or
 * {@code basic.auth.user.info} with the default filters. Independently of the key, credentials
 * embedded in URLs (e.g. a Schema Registry URL of the form {@code https://key:secret@host/...})
 * are removed from every logged value.
 */
public final class SensitiveConfigUtils {

  /**
   * Placeholder logged in place of a sensitive config value.
   */
  public static final String SENSITIVE_VALUES_MASKED = "SENSITIVE_INFO_MASKED";

  private static final List<String> DEFAULT_SENSITIVE_CONFIG_KEYS = Collections.unmodifiableList(Arrays.asList(
      HoodieWriteConfig.SENSITIVE_CONFIG_KEYS_FILTER.defaultValue().split(",")));

  /**
   * Matches the userinfo of a URL, e.g. {@code key:secret@} in {@code https://key:secret@host/path}.
   * Greedy up to the last '@' before the next ',' or whitespace, because credentials written into
   * URLs are not always URL-encoded and may themselves contain '/' or '@'.
   */
  private static final Pattern URL_USER_INFO = Pattern.compile("(?<=://)[^,\\s]*@");

  private SensitiveConfigUtils() {
  }

  /**
   * Removes the credentials (userinfo) from every URL in {@code value}, keeping scheme, host and
   * path for debugging, e.g. {@code https://key:secret@host/subjects/s} becomes
   * {@code https://host/subjects/s}. Each URL of a comma-separated list is scrubbed.
   */
  public static String maskUrlCredentials(String value) {
    if (value == null || value.indexOf('@') < 0) {
      return value;
    }
    return URL_USER_INFO.matcher(value).replaceAll("");
  }

  /**
   * Returns the loggable form of one config value: {@link #SENSITIVE_VALUES_MASKED} when the key is
   * sensitive, otherwise the value with any URL credentials removed.
   */
  public static Object maskSensitiveValue(String key, Object value, List<String> sensitiveConfigFilters) {
    if (isSensitiveKey(key, sensitiveConfigFilters)) {
      return SENSITIVE_VALUES_MASKED;
    }
    return value instanceof String ? maskUrlCredentials((String) value) : value;
  }

  /**
   * Returns the filters that mark a config key as sensitive, read from
   * {@link HoodieWriteConfig#SENSITIVE_CONFIG_KEYS_FILTER} in {@code props}, falling back to its default.
   */
  public static List<String> getSensitiveConfigFilters(TypedProperties props) {
    return props.getStringList(HoodieWriteConfig.SENSITIVE_CONFIG_KEYS_FILTER.key(), ",", DEFAULT_SENSITIVE_CONFIG_KEYS);
  }

  /**
   * Returns true if {@code key} contains any of the given sensitive config filters.
   */
  public static boolean isSensitiveKey(String key, List<String> sensitiveConfigFilters) {
    return sensitiveConfigFilters.stream().anyMatch(key::contains);
  }

  /**
   * Returns a key-sorted copy of {@code configs} that is safe to log: values of sensitive keys are
   * replaced with {@link #SENSITIVE_VALUES_MASKED}, and credentials are removed from URLs in all
   * other values. The input map is not modified.
   *
   * @param configs configs to mask, e.g. the Kafka consumer params
   * @param props   properties holding {@link HoodieWriteConfig#SENSITIVE_CONFIG_KEYS_FILTER}
   */
  public static Map<String, Object> maskSensitiveValues(Map<?, ?> configs, TypedProperties props) {
    List<String> sensitiveConfigFilters = getSensitiveConfigFilters(props);
    Map<String, Object> maskedConfigs = new TreeMap<>();
    configs.forEach((key, value) -> {
      String configKey = String.valueOf(key);
      maskedConfigs.put(configKey, maskSensitiveValue(configKey, value, sensitiveConfigFilters));
    });
    return maskedConfigs;
  }
}
