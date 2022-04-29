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

package org.apache.hudi.common.config;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 * An extension of {@link java.util.Properties} that maintains the order.
 * The implementation is not thread-safe.
 */
public class OrderedProperties extends Properties {

  private final HashSet<Object> keys = new LinkedHashSet<>();

  public OrderedProperties() {
    super(null);
  }

  public OrderedProperties(Properties defaults) {
    if (Objects.nonNull(defaults)) {
      for (String key : defaults.stringPropertyNames()) {
        put(key, defaults.getProperty(key));
      }
    }
  }

  @Override
  public Enumeration propertyNames() {
    return Collections.enumeration(keys);
  }

  @Override
  public synchronized Enumeration<Object> keys() {
    return Collections.enumeration(keys);
  }

  @Override
  public Set<String> stringPropertyNames() {
    Set<String> set = new LinkedHashSet<>();
    for (Object key : this.keys) {
      if (key instanceof String) {
        set.add((String) key);
      }
    }
    return set;
  }

  public synchronized void putAll(Properties t) {
    for (Map.Entry<?, ?> e : t.entrySet()) {
      if (!containsKey(String.valueOf(e.getKey()))) {
        keys.add(e.getKey());
      }
      super.put(e.getKey(), e.getValue());
    }
  }

  @Override
  public synchronized Object put(Object key, Object value) {
    keys.remove(key);
    keys.add(key);
    return super.put(key, value);
  }

  public synchronized Object putIfAbsent(Object key, Object value) {
    if (!containsKey(String.valueOf(key))) {
      keys.add(key);
    }
    return super.putIfAbsent(key, value);
  }

  @Override
  public Object remove(Object key) {
    keys.remove(key);
    return super.remove(key);
  }
}
