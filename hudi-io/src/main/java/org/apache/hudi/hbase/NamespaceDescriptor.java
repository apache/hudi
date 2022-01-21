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

package org.apache.hudi.hbase;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.hudi.hbase.util.Bytes;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Namespace POJO class. Used to represent and define namespaces.
 *
 * Descriptors will be persisted in an hbase table.
 * This works since namespaces are essentially metadata of a group of tables
 * as opposed to a more tangible container.
 */
@InterfaceAudience.Public
public class NamespaceDescriptor {

  /** System namespace name. */
  public static final byte [] SYSTEM_NAMESPACE_NAME = Bytes.toBytes("hbase");
  public static final String SYSTEM_NAMESPACE_NAME_STR =
      Bytes.toString(SYSTEM_NAMESPACE_NAME);
  /** Default namespace name. */
  public static final byte [] DEFAULT_NAMESPACE_NAME = Bytes.toBytes("default");
  public static final String DEFAULT_NAMESPACE_NAME_STR =
      Bytes.toString(DEFAULT_NAMESPACE_NAME);

  public static final NamespaceDescriptor DEFAULT_NAMESPACE = NamespaceDescriptor.create(
      DEFAULT_NAMESPACE_NAME_STR).build();
  public static final NamespaceDescriptor SYSTEM_NAMESPACE = NamespaceDescriptor.create(
      SYSTEM_NAMESPACE_NAME_STR).build();

  public final static Set<String> RESERVED_NAMESPACES;
  static {
    Set<String> set = new HashSet<>();
    set.add(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR);
    set.add(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR);
    RESERVED_NAMESPACES = Collections.unmodifiableSet(set);
  }
  public final static Set<byte[]> RESERVED_NAMESPACES_BYTES;
  static {
    Set<byte[]> set = new TreeSet<>(Bytes.BYTES_RAWCOMPARATOR);
    for(String name: RESERVED_NAMESPACES) {
      set.add(Bytes.toBytes(name));
    }
    RESERVED_NAMESPACES_BYTES = Collections.unmodifiableSet(set);
  }

  private String name;
  private Map<String, String> configuration;

  public static final Comparator<NamespaceDescriptor> NAMESPACE_DESCRIPTOR_COMPARATOR =
      new Comparator<NamespaceDescriptor>() {
        @Override
        public int compare(NamespaceDescriptor namespaceDescriptor,
                           NamespaceDescriptor namespaceDescriptor2) {
          return namespaceDescriptor.getName().compareTo(namespaceDescriptor2.getName());
        }
      };

  private NamespaceDescriptor() {
  }

  private NamespaceDescriptor(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  /**
   * Getter for accessing the configuration value by key
   */
  public String getConfigurationValue(String key) {
    return configuration.get(key);
  }

  /**
   * Getter for fetching an unmodifiable {@link #configuration} map.
   */
  public Map<String, String> getConfiguration() {
    // shallow pointer copy
    return Collections.unmodifiableMap(configuration);
  }

  /**
   * Setter for storing a configuration setting in {@link #configuration} map.
   * @param key Config key. Same as XML config key e.g. hbase.something.or.other.
   * @param value String value. If null, removes the setting.
   */
  public void setConfiguration(String key, String value) {
    if (value == null) {
      removeConfiguration(key);
    } else {
      configuration.put(key, value);
    }
  }

  /**
   * Remove a config setting represented by the key from the {@link #configuration} map
   */
  public void removeConfiguration(final String key) {
    configuration.remove(key);
  }

  @Override
  public String toString() {
    StringBuilder s = new StringBuilder();
    s.append('{');
    s.append(HConstants.NAME);
    s.append(" => '");
    s.append(name);
    s.append("'");
    for (Map.Entry<String, String> e : configuration.entrySet()) {
      String key = e.getKey();
      String value = e.getValue();
      if (key == null) {
        continue;
      }
      s.append(", ");
      s.append(key);
      s.append(" => '");
      s.append(value);
      s.append("'");
    }
    s.append('}');
    return s.toString();
  }

  public static Builder create(String name) {
    return new Builder(name);
  }

  public static Builder create(NamespaceDescriptor ns) {
    return new Builder(ns);
  }

  @InterfaceAudience.Public
  public static class Builder {
    private String bName;
    private Map<String, String> bConfiguration = new TreeMap<>();

    private Builder(NamespaceDescriptor ns) {
      this.bName = ns.name;
      this.bConfiguration = ns.configuration;
    }

    private Builder(String name) {
      this.bName = name;
    }

    public Builder addConfiguration(Map<String, String> configuration) {
      this.bConfiguration.putAll(configuration);
      return this;
    }

    public Builder addConfiguration(String key, String value) {
      this.bConfiguration.put(key, value);
      return this;
    }

    public Builder removeConfiguration(String key) {
      this.bConfiguration.remove(key);
      return this;
    }

    public NamespaceDescriptor build() {
      if (this.bName == null){
        throw new IllegalArgumentException("A name has to be specified in a namespace.");
      }

      NamespaceDescriptor desc = new NamespaceDescriptor(this.bName);
      desc.configuration = this.bConfiguration;
      return desc;
    }
  }
}
