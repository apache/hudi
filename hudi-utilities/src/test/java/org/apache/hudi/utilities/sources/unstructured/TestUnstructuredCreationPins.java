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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.utilities.sources.blob.BlobTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.input.EmbeddingInputConfig;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestUnstructuredCreationPins {
  @Test
  void blobCreationStoresPhysicalSettings() {
    TypedProperties props = mode("blob");
    props.setProperty(BlobTableConfig.INLINE_MAX_BYTES.key(), "4096");
    Properties pins = new UnstructuredSourceContract(props).tableProperties();
    assertEquals("4096", pins.getProperty("hoodie.table.blob.inline.max.bytes"));
    assertEquals("uncompressed", pins.getProperty("hoodie.table.blob.parquet.compression.codec"));
    assertEquals("false", pins.getProperty("hoodie.table.blob.parquet.bloom.filter.enabled"));
  }

  @Test
  void changedBlobPhysicalSettingsAreRefused() {
    TypedProperties props = mode("blob");
    UnstructuredTableProperties required = new UnstructuredSourceContract(props).requiredTableProperties();
    HoodieTableConfig table = table(required.tableProperties());
    table.getProps().setProperty("hoodie.table.blob.inline.max.bytes", "4096");
    assertThrows(IllegalArgumentException.class, () -> required.checkTable("file:/blob", table));
  }

  @Test
  void embeddingCreationStoresInputAndColumnInfoLayout() {
    TypedProperties props = mode("embedding");
    props.setProperty(EmbeddingInputConfig.INLINE_MAX_BYTES.key(), "4096");
    Properties pins = new UnstructuredSourceContract(props).tableProperties();
    assertEquals("1", pins.getProperty("hoodie.table.embedding.column.info.layout.version"));
    assertEquals("4096", pins.getProperty("hoodie.table.embedding.input.inline.max.bytes"));
  }

  @Test
  void matchingBasePinsRemainAccepted() {
    for (String mode : new String[] {"blob", "embedding"}) {
      UnstructuredTableProperties required = new UnstructuredSourceContract(mode(mode)).requiredTableProperties();
      assertDoesNotThrow(() -> required.checkTable("file:/table", table(required.tableProperties())));
    }
  }

  private static TypedProperties mode(String value) {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.unstructured.source.mode", value);
    return props;
  }

  private static HoodieTableConfig table(Properties pins) {
    HoodieTableConfig table = new HoodieTableConfig();
    table.getProps().putAll(pins);
    return table;
  }
}
