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

package org.apache.hudi.common.fs;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link StorageSchemes}.
 */
public class TestStorageSchemes {

  @Test
  public void testStorageSchemes() {
    assertTrue(StorageSchemes.isSchemeSupported("hdfs"));
    assertTrue(StorageSchemes.isSchemeSupported("afs"));
    assertFalse(StorageSchemes.isSchemeSupported("s2"));
    assertFalse(StorageSchemes.isAppendSupported("s3a"));
    assertFalse(StorageSchemes.isAppendSupported("gs"));
    assertFalse(StorageSchemes.isAppendSupported("wasb"));
    assertFalse(StorageSchemes.isAppendSupported("adl"));
    assertFalse(StorageSchemes.isAppendSupported("abfs"));
    assertFalse(StorageSchemes.isAppendSupported("oss"));
    assertTrue(StorageSchemes.isAppendSupported("viewfs"));
    assertFalse(StorageSchemes.isAppendSupported("alluxio"));
    assertFalse(StorageSchemes.isAppendSupported("cosn"));
    assertFalse(StorageSchemes.isAppendSupported("dbfs"));
    assertFalse(StorageSchemes.isAppendSupported("cos"));
    assertTrue(StorageSchemes.isAppendSupported("jfs"));
    assertFalse(StorageSchemes.isAppendSupported("bos"));
    assertFalse(StorageSchemes.isAppendSupported("ks3"));
    assertTrue(StorageSchemes.isAppendSupported("ofs"));
    assertFalse(StorageSchemes.isAppendSupported("oci"));
    assertThrows(IllegalArgumentException.class, () -> {
      StorageSchemes.isAppendSupported("s2");
    }, "Should throw exception for unsupported schemes");
  }
}
