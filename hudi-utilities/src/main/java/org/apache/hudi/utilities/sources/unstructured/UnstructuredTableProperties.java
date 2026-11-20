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
import org.apache.hudi.common.table.HoodieTableConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Table pins owned by unstructured streams, written at table creation and checked before preparing or reading input: each
 * exactly, or a group by its own {@link TableCheck}.
 */
public final class UnstructuredTableProperties {
  private final String sourceName;
  /** Every pin, written at table creation. */
  private final Map<String, String> pins;
  /** The pins an existing table must store exactly. */
  private final Map<String, String> exact;
  private final List<TableCheck> checks;

  private UnstructuredTableProperties(String sourceName, Map<String, String> pins, Map<String, String> exact, List<TableCheck> checks) {
    this.sourceName = sourceName;
    this.pins = new LinkedHashMap<>(pins);
    this.exact = new LinkedHashMap<>(exact);
    this.checks = Collections.unmodifiableList(new ArrayList<>(checks));
  }

  /** Checks a group of pins on an existing table its own way, e.g. comparing a key the table does not store as its default. */
  @FunctionalInterface
  public interface TableCheck {
    /**
     * @throws IllegalArgumentException naming {@code sourceName}, the table at {@code basePath} and the pins it differs on
     */
    void check(String sourceName, String basePath, Properties table);
  }

  public static Builder builder(String sourceName) {
    return new Builder(sourceName);
  }

  /** Adds the creation pins to a copy of the job, refusing an explicitly different encoding or layout. */
  public TypedProperties apply(TypedProperties props) {
    TypedProperties merged = TypedProperties.copy(props);
    pins.forEach((key, value) -> {
      String configured = props.getProperty(key);
      if (configured != null && !value.equals(configured)) {
        throw new IllegalArgumentException(sourceName + " requires " + key + "=" + value + ", configured: " + configured);
      }
      merged.setProperty(key, value);
    });
    return merged;
  }

  /**
   * Refuses a missing or different exact pin, then runs each group's {@link TableCheck}; table properties are never inferred
   * from the current job.
   */
  public void checkTable(String basePath, HoodieTableConfig table) {
    exact.forEach((key, value) -> {
      String stored = table.getProps().getProperty(key);
      if (!value.equals(stored)) {
        throw new IllegalArgumentException(sourceName + " requires the table's " + key + "=" + value
            + ": the table at " + basePath + " stores " + (stored == null ? "none" : stored) + "; write this job to a new table");
      }
    });
    checks.forEach(check -> check.check(sourceName, basePath, table.getProps()));
  }

  /** Opaque properties passed to Hudi's generic table creation mechanism. */
  public Properties tableProperties() {
    Properties properties = new Properties();
    properties.putAll(pins);
    return properties;
  }

  public static final class Builder {
    private final String sourceName;
    private final Map<String, String> pins = new LinkedHashMap<>();
    private final Map<String, String> exact = new LinkedHashMap<>();
    private final List<TableCheck> checks = new ArrayList<>();

    private Builder(String sourceName) {
      this.sourceName = sourceName;
    }

    /** A pin an existing table must store exactly. */
    public Builder requireTableProperty(ConfigProperty<String> key, String value) {
      pins.put(key.key(), value);
      exact.put(key.key(), value);
      return this;
    }

    /** Pins written at table creation and checked on an existing table by {@code check} instead of exactly. */
    public Builder requireTableProperties(Map<String, String> properties, TableCheck check) {
      pins.putAll(properties);
      properties.keySet().forEach(exact::remove);
      checks.add(check);
      return this;
    }

    public UnstructuredTableProperties build() {
      return new UnstructuredTableProperties(sourceName, pins, exact, checks);
    }
  }
}
