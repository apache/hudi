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
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StorageConfiguration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The start-up checks of the embedding table: whether it exists and its table version. */
class TestEmbeddingTableChecks {

  @TempDir
  Path temp;

  private final StorageConfiguration<?> conf = HoodieTestUtils.getDefaultStorageConf();

  private static EmbeddingTableConfig config(String writeTableVersion) {
    TypedProperties props = new TypedProperties();
    if (writeTableVersion != null) {
      props.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), writeTableVersion);
    }
    return EmbeddingTableConfig.from(props);
  }

  @Test
  void writeTableVersionBelowNineIsRefused() {
    String table = temp.resolve("none").toString();
    EmbeddingTableConfig eight = config("8");
    assertThrows(EmbeddingTableVersionException.class, () -> EmbeddingTableChecks.checkTableVersion(table, conf, eight));
    EmbeddingTableConfig unparsable = config("x");
    assertThrows(EmbeddingTableVersionException.class, () -> EmbeddingTableChecks.checkTableVersion(table, conf, unparsable));
    EmbeddingTableChecks.checkTableVersion(table, conf, config(null));
  }

  @Test
  void existingTableBelowNineIsRefused() throws Exception {
    String table = temp.resolve("v8").toString();
    HoodieTestUtils.init(table, HoodieTableType.COPY_ON_WRITE, HoodieTableVersion.EIGHT);
    EmbeddingTableConfig unpinned = config(null);
    EmbeddingTableVersionException error = assertThrows(EmbeddingTableVersionException.class,
        () -> EmbeddingTableChecks.checkTableVersion(table, conf, unpinned));
    assertTrue(error.getMessage().contains(HoodieTableConfig.VERSION.key() + "=8"), error.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {"missing", "2"})
  void standaloneEmbeddingRefusesUnknownKeyEncoding(String version) throws Exception {
    String key = "hoodie.table.unstructured.key.encoding.version";
    java.util.Properties pins = UnstructuredTableTestProperties.embedding(new TypedProperties());
    pins.setProperty(key, "1");
    String table = temp.resolve("encoding").toUri().toString();
    HoodieTableMetaClient meta = HoodieTableMetaClient.newTableBuilder().setTableVersion(9).setTableName("embedding")
        .setTableType(HoodieTableType.COPY_ON_WRITE).setAdditionalProperties(pins).initTable(conf, table);
    EmbeddingTableChecks.checkTableVersion(table, conf, config(null));
    if ("missing".equals(version)) {
      HoodieTableConfig.delete(meta.getStorage(), meta.getMetaPath(), java.util.Collections.singleton(key));
    } else {
      pins.setProperty(key, version);
      HoodieTableConfig.update(meta.getStorage(), meta.getMetaPath(), pins);
    }
    EmbeddingTableConfig unpinned = config(null);
    IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingTableChecks.checkTableVersion(table, conf, unpinned));
    assertTrue(error.getMessage().contains(key), error.getMessage());
  }

  private static final String TABLE_INLINE_MAX_BYTES = "hoodie.table.embedding.input.inline.max.bytes";
  private static final String COLUMN_INFO_LAYOUT_VERSION = "hoodie.table.embedding.column.info.layout.version";

  /** Creates an embedding table at {@code name} that stores {@code pins}. */
  private String table(String name, java.util.Properties pins) throws Exception {
    String table = temp.resolve(name).toUri().toString();
    HoodieTableMetaClient.newTableBuilder().setTableVersion(9).setTableName("embedding")
        .setTableType(HoodieTableType.COPY_ON_WRITE).setAdditionalProperties(pins).initTable(conf, table);
    return table;
  }

  private static TypedProperties pdfOnly() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "pdf");
    return props;
  }

  /**
   * The input store's threshold decides no byte of a job whose preprocessors store no inputs: a PDF-only job runs on a table
   * that stores another threshold, or none.
   */
  @Test
  void jobThatStoresNoInputsIsNotCheckedAgainstTheInputStoreThreshold() throws Exception {
    java.util.Properties pins = UnstructuredTableTestProperties.embedding(pdfOnly());
    pins.setProperty(TABLE_INLINE_MAX_BYTES, "4096");
    EmbeddingTableChecks.checkTableVersion(table("other-threshold", pins), conf, EmbeddingTableConfig.from(pdfOnly()));
    pins.remove(TABLE_INLINE_MAX_BYTES);
    EmbeddingTableChecks.checkTableVersion(table("no-threshold", pins), conf, EmbeddingTableConfig.from(pdfOnly()));
  }

  /**
   * A job that stores inputs (the default job preps images and video) is refused on another threshold, told the setting that
   * matches the table, and on none, told the table predates the pin.
   */
  @Test
  void jobThatStoresInputsIsRefusedOnAnotherInputStoreThreshold() throws Exception {
    java.util.Properties pins = UnstructuredTableTestProperties.embedding(new TypedProperties());
    String stored = pins.getProperty(TABLE_INLINE_MAX_BYTES);
    String table = table("threshold", pins);
    EmbeddingTableChecks.checkTableVersion(table, conf, config(null));
    TypedProperties other = new TypedProperties();
    other.setProperty("hoodie.unstructured.embedding.input.inline.max.bytes", "4096");
    EmbeddingTableConfig otherThreshold = EmbeddingTableConfig.from(other);
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingTableChecks.checkTableVersion(table, conf, otherThreshold));
    assertTrue(refused.getMessage().startsWith("Embedding prep requires the table's " + TABLE_INLINE_MAX_BYTES + "=4096"),
        refused.getMessage());
    assertTrue(refused.getMessage().contains("(the table stores " + stored + "; set hoodie.unstructured.embedding.input.inline.max.bytes="
        + stored + " to match it)"), refused.getMessage());

    pins.remove(TABLE_INLINE_MAX_BYTES);
    String unpinned = table("no-threshold", pins);
    EmbeddingTableConfig defaults = config(null);
    IllegalArgumentException missing = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingTableChecks.checkTableVersion(unpinned, conf, defaults));
    assertTrue(missing.getMessage().contains(TABLE_INLINE_MAX_BYTES + "=" + stored + " (the table stores none; the table was "
        + "created by an earlier build that does not store it: write this job to a new table)"), missing.getMessage());
  }

  /** A table without the column-info layout pin (one created before the pin) is refused on the direct-transformer path. */
  @Test
  void tableWithoutTheColumnInfoLayoutPinIsRefused() throws Exception {
    java.util.Properties pins = UnstructuredTableTestProperties.embedding(new TypedProperties());
    pins.remove(COLUMN_INFO_LAYOUT_VERSION);
    String table = table("no-layout", pins);
    EmbeddingTableConfig defaults = config(null);
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingTableChecks.checkTableVersion(table, conf, defaults));
    assertTrue(refused.getMessage().contains(COLUMN_INFO_LAYOUT_VERSION + "=1: the table at " + table + " stores none"),
        refused.getMessage());
  }

  /** A table left with only the hoodie.properties backup, as a crash during a table config update leaves it, exists. */
  @Test
  void tableWithOnlyThePropertiesBackupExists() throws Exception {
    String table = temp.resolve("backup").toString();
    HoodieTestUtils.init(conf, table);
    Path metaFolder = temp.resolve("backup").resolve(HoodieTableMetaClient.METAFOLDER_NAME);
    Files.move(metaFolder.resolve(HoodieTableConfig.HOODIE_PROPERTIES_FILE),
        metaFolder.resolve(HoodieTableConfig.HOODIE_PROPERTIES_FILE_BACKUP));
    assertTrue(EmbeddingTableChecks.tableExists(table, conf));
  }
}
