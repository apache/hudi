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

package org.apache.hudi.common.table;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end tests for meta-field-population state-transition enforcement at the
 * {@link HoodieTableConfig#update} boundary - the CLI-equivalent path operators hit
 * via {@code hudi-cli table update-configs}. Complements the pure-function lattice
 * tests in {@code TestHoodieMetaFieldFlagsTransition} by exercising the end-to-end
 * flow including atomicity (no on-disk mutation on rejection) and the
 * populate-true-to-false side effect of clearing the now-meaningless exclude list.
 */
class TestHoodieTableConfigMetaFieldsTransition extends HoodieCommonTestHarness {

  private static final String COMMIT_TIME = HoodieRecord.COMMIT_TIME_METADATA_FIELD;
  private static final String RECORD_KEY = HoodieRecord.RECORD_KEY_METADATA_FIELD;
  private static final String PARTITION_PATH = HoodieRecord.PARTITION_PATH_METADATA_FIELD;
  private static final String FILE_NAME = HoodieRecord.FILENAME_METADATA_FIELD;

  private HoodieStorage storage;
  private StoragePath metaPath;
  private StoragePath cfgPath;

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    storage = HoodieStorageUtils.getStorage(basePath, HoodieTestUtils.getDefaultStorageConfWithDefaults());
    metaPath = new StoragePath(basePath, HoodieTableMetaClient.METAFOLDER_NAME);
    cfgPath = new StoragePath(metaPath, HoodieTableConfig.HOODIE_PROPERTIES_FILE);

    Properties props = new Properties();
    props.setProperty(HoodieTableConfig.NAME.key(), "test_meta_fields_transition");
    HoodieTableConfig.create(storage, metaPath, props);
  }

  @AfterEach
  public void tearDown() throws Exception {
    storage.close();
  }

  @Test
  void emptyToSubsetPersisted() throws IOException {
    setExcludeList(joined(RECORD_KEY, FILE_NAME));
    assertExcludeListEquals(joined(RECORD_KEY, FILE_NAME));
  }

  @Test
  void subsetToWiderSubsetPersisted() throws IOException {
    setExcludeList(joined(RECORD_KEY));
    setExcludeList(joined(RECORD_KEY, PARTITION_PATH, FILE_NAME));
    assertExcludeListEquals(joined(RECORD_KEY, PARTITION_PATH, FILE_NAME));
  }

  @Test
  void narrowingExcludeListRejectedAndFileUnchanged() throws IOException {
    // First widen to {RECORD_KEY, PARTITION_PATH, FILE_NAME} - allowed.
    setExcludeList(joined(RECORD_KEY, PARTITION_PATH, FILE_NAME));
    byte[] before = readProperties();

    // Attempt to narrow back to {RECORD_KEY} - must be rejected, file must be untouched.
    Properties narrow = new Properties();
    narrow.setProperty(HoodieTableConfig.META_FIELDS_EXCLUDE_LIST.key(), joined(RECORD_KEY));
    HoodieException ex = assertThrows(HoodieException.class,
        () -> HoodieTableConfig.update(storage, metaPath, narrow));
    assertTrue(ex.getMessage().contains(PARTITION_PATH)
            || ex.getMessage().contains(FILE_NAME),
        "Rejection message should mention the field being removed, was: " + ex.getMessage());

    byte[] after = readProperties();
    assertArrayEquals(before, after,
        "hoodie.properties must be byte-identical after a rejected transition");
    assertFalse(storage.exists(new StoragePath(metaPath, HoodieTableConfig.HOODIE_PROPERTIES_FILE_BACKUP)),
        "No backup file should be left behind after a rejected transition");
  }

  @Test
  void populateFalseToTrueRejectedAndFileUnchanged() throws IOException {
    setPopulateMetaFields(false);
    byte[] before = readProperties();

    Properties flip = new Properties();
    flip.setProperty(HoodieTableConfig.POPULATE_META_FIELDS.key(), "true");
    HoodieException ex = assertThrows(HoodieException.class,
        () -> HoodieTableConfig.update(storage, metaPath, flip));
    assertTrue(ex.getMessage().contains("false to true"), ex.getMessage());

    byte[] after = readProperties();
    assertArrayEquals(before, after,
        "hoodie.properties must be byte-identical after a rejected populate=false->true");
  }

  @Test
  void populateTrueToFalseStripsExcludeList() throws IOException {
    setExcludeList(joined(RECORD_KEY, FILE_NAME));
    HoodieTableConfig configBefore = new HoodieTableConfig(storage, metaPath);
    assertEquals(joined(RECORD_KEY, FILE_NAME),
        configBefore.getProps().getProperty(HoodieTableConfig.META_FIELDS_EXCLUDE_LIST.key()));

    // Flip populate=false. The validator allows this and strips the (now meaningless) exclude list.
    setPopulateMetaFields(false);

    HoodieTableConfig configAfter = new HoodieTableConfig(storage, metaPath);
    assertEquals("false",
        configAfter.getProps().getProperty(HoodieTableConfig.POPULATE_META_FIELDS.key()));
    assertNull(configAfter.getProps().getProperty(HoodieTableConfig.META_FIELDS_EXCLUDE_LIST.key()),
        "META_FIELDS_EXCLUDE_LIST should be removed from hoodie.properties after populate=true->false");
  }

  @Test
  void populateTrueToFalseIsAllowedEvenWithNoPriorExcludeList() throws IOException {
    setPopulateMetaFields(false);
    HoodieTableConfig configAfter = new HoodieTableConfig(storage, metaPath);
    assertEquals("false",
        configAfter.getProps().getProperty(HoodieTableConfig.POPULATE_META_FIELDS.key()));
  }

  @Test
  void unknownFieldNameRejectedAndFileUnchanged() throws IOException {
    byte[] before = readProperties();

    Properties bad = new Properties();
    bad.setProperty(HoodieTableConfig.META_FIELDS_EXCLUDE_LIST.key(),
        joined(RECORD_KEY, "_hoodie_typo"));
    assertThrows(IllegalArgumentException.class,
        () -> HoodieTableConfig.update(storage, metaPath, bad));

    byte[] after = readProperties();
    assertArrayEquals(before, after,
        "hoodie.properties must be byte-identical after a rejected unknown-field transition");
  }

  @Test
  void idempotentExcludeListUpdateAllowed() throws IOException {
    setExcludeList(joined(RECORD_KEY, FILE_NAME));
    // Re-apply the same value.
    setExcludeList(joined(RECORD_KEY, FILE_NAME));
    assertExcludeListEquals(joined(RECORD_KEY, FILE_NAME));
  }

  @Test
  void emptyToAllFivePersisted() throws IOException {
    String all = joined(COMMIT_TIME, HoodieRecord.COMMIT_SEQNO_METADATA_FIELD,
        RECORD_KEY, PARTITION_PATH, FILE_NAME);
    setExcludeList(all);
    HoodieTableConfig config = new HoodieTableConfig(storage, metaPath);
    // The persisted value is the same string the caller set (order-preserving).
    assertEquals(all,
        config.getProps().getProperty(HoodieTableConfig.META_FIELDS_EXCLUDE_LIST.key()));
  }

  // ------- helpers -------

  private void setExcludeList(String value) {
    Properties props = new Properties();
    props.setProperty(HoodieTableConfig.META_FIELDS_EXCLUDE_LIST.key(), value);
    HoodieTableConfig.update(storage, metaPath, props);
  }

  private void setPopulateMetaFields(boolean value) {
    Properties props = new Properties();
    props.setProperty(HoodieTableConfig.POPULATE_META_FIELDS.key(), Boolean.toString(value));
    HoodieTableConfig.update(storage, metaPath, props);
  }

  private void assertExcludeListEquals(String expected) {
    HoodieTableConfig config = new HoodieTableConfig(storage, metaPath);
    assertEquals(expected,
        config.getProps().getProperty(HoodieTableConfig.META_FIELDS_EXCLUDE_LIST.key()));
  }

  private byte[] readProperties() throws IOException {
    try (InputStream in = storage.open(cfgPath)) {
      java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
      byte[] buf = new byte[1024];
      int n;
      while ((n = in.read(buf)) > 0) {
        out.write(buf, 0, n);
      }
      return out.toByteArray();
    }
  }

  private static String joined(String... fields) {
    return String.join(",", fields);
  }
}
