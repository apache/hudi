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

package org.apache.hudi.utilities.unstructured.embedding;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredTableProperties;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredTableVersion;
import org.apache.hudi.utilities.unstructured.embedding.prep.PrepLogicVersions;

import java.util.Properties;

/** Declares the contracts of tables created directly by test fixtures, before their first write. */
public final class UnstructuredTableTestProperties {
  private UnstructuredTableTestProperties() {
  }

  public static Properties layoutOne() {
    Properties properties = new Properties();
    properties.setProperty(UnstructuredTableVersion.BLOB_LAYOUT_VERSION.key(), "1");
    return properties;
  }

  public static Properties embedding(TypedProperties jobProps) {
    UnstructuredTableProperties.Builder settings = UnstructuredTableProperties.builder("Embedding test fixture")
        .requireTableProperty(UnstructuredTableVersion.BLOB_LAYOUT_VERSION, "1")
        .requireTableProperty(UnstructuredTableVersion.KIND, "embedding")
        .requireTableProperty(UnstructuredTableVersion.SCHEMA_VERSION, "1")
        .requireTableProperty(UnstructuredTableVersion.KEY_ENCODING_VERSION, "1");
    settings.requireTableProperty(org.apache.hudi.common.config.ConfigProperty
        .key("hoodie.table.embedding.column.info.layout.version").noDefaultValue(), "1");
    settings.requireTableProperty(org.apache.hudi.common.config.ConfigProperty
        .key("hoodie.table.embedding.input.inline.max.bytes").noDefaultValue(),
        Long.toString(org.apache.hudi.utilities.unstructured.embedding.input.EmbeddingInputConfig.inlineMaxBytes(jobProps)));
    PrepLogicVersions.from(jobProps).requireTableProperties(settings);
    return settings.build().tableProperties();
  }
}
