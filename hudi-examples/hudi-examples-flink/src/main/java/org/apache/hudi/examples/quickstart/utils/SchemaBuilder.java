/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.examples.quickstart.utils;

import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.WatermarkSpec;
import org.apache.flink.table.types.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Builder for {@link ResolvedSchema}.
 */
public class SchemaBuilder {
  private final List<Column> columns;
  private final List<WatermarkSpec> watermarkSpecs;
  private UniqueConstraint constraint;

  public static SchemaBuilder instance() {
    return new SchemaBuilder();
  }

  private SchemaBuilder() {
    this.columns = new ArrayList<>();
    this.watermarkSpecs = new ArrayList<>();
  }

  public SchemaBuilder field(String name, DataType type) {
    this.columns.add(Column.physical(name, type));
    return this;
  }

  public SchemaBuilder fields(List<String> names, List<DataType> types) {
    List<Column> columns = IntStream.range(0, names.size())
        .mapToObj(idx -> Column.physical(names.get(idx), types.get(idx)))
        .collect(Collectors.toList());
    this.columns.addAll(columns);
    return this;
  }

  public SchemaBuilder primaryKey(String... columns) {
    this.constraint = UniqueConstraint.primaryKey("pk", Arrays.asList(columns));
    return this;
  }

  public ResolvedSchema build() {
    return new ResolvedSchema(columns, watermarkSpecs, constraint);
  }
}
