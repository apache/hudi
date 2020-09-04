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

package org.apache.hudi.common.config;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Type-aware extension of {@link java.util.Properties}.
 */
public class TypedProperties extends Properties implements Serializable {

  public TypedProperties() {
    super(null);
  }

  public TypedProperties(Properties defaults) {
    super(defaults);
  }

  private void checkKey(String property) {
    if (!keyExists(property)) {
      throw new IllegalArgumentException("Property " + property + " not found");
    }
  }

  private boolean keyExists(String property) {
    Set<String> keys = super.stringPropertyNames();
    if (keys.contains(property)) {
      return true;
    }
    return false;
  }

  public String getString(String property) {
    checkKey(property);
    return getProperty(property);
  }

  public String getString(String property, String defaultValue) {
    return keyExists(property) ? getProperty(property) : defaultValue;
  }

  public List<String> getStringList(String property, String delimiter, List<String> defaultVal) {
    if (!keyExists(property)) {
      return defaultVal;
    }
    return Arrays.stream(getProperty(property).split(delimiter)).map(String::trim).collect(Collectors.toList());
  }

  public int getInteger(String property) {
    checkKey(property);
    return Integer.parseInt(getProperty(property));
  }

  public int getInteger(String property, int defaultValue) {
    return keyExists(property) ? Integer.parseInt(getProperty(property)) : defaultValue;
  }

  public long getLong(String property) {
    checkKey(property);
    return Long.parseLong(getProperty(property));
  }

  public long getLong(String property, long defaultValue) {
    return keyExists(property) ? Long.parseLong(getProperty(property)) : defaultValue;
  }

  public boolean getBoolean(String property) {
    checkKey(property);
    return Boolean.parseBoolean(getProperty(property));
  }

  public boolean getBoolean(String property, boolean defaultValue) {
    return keyExists(property) ? Boolean.parseBoolean(getProperty(property)) : defaultValue;
  }

  public double getDouble(String property) {
    checkKey(property);
    return Double.parseDouble(getProperty(property));
  }

  public double getDouble(String property, double defaultValue) {
    return keyExists(property) ? Double.parseDouble(getProperty(property)) : defaultValue;
  }
}
