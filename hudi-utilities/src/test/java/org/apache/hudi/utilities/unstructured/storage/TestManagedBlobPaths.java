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

package org.apache.hudi.utilities.unstructured.storage;

import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestManagedBlobPaths {

  @Test
  void managedBlobPathsAreFilesUnderTheBlobFolder() {
    assertEquals(".hoodie/.blob", ManagedBlobPaths.FOLDER);
    assertTrue(ManagedBlobPaths.isManagedBlobPath(".hoodie/.blob/blob-1_0-0-0_001.bin"));
    assertTrue(ManagedBlobPaths.isManagedBlobPath(".hoodie/.blob/2016/03/15/blob-1_0-1-1_001.bin"));
    assertFalse(ManagedBlobPaths.isManagedBlobPath(".hoodie/.blob"), "the folder itself");
    assertFalse(ManagedBlobPaths.isManagedBlobPath(".hoodie/.blobs/x.bin"), "a sibling folder");
    assertFalse(ManagedBlobPaths.isManagedBlobPath(".hoodie/timeline/001.commit"), "another meta-folder file");
    assertFalse(ManagedBlobPaths.isManagedBlobPath("2016/03/15/file-1_0-1-1_001.parquet"), "a data file");
    assertFalse(ManagedBlobPaths.isManagedBlobPath(null));
  }

  @Test
  void theWriterNamesItsFilesUnderTheBlobFolder() {
    OutOfLineWriter writer = new OutOfLineWriter(null, "/table", "001", Option.empty());
    String unpartitioned = writer.newPath("");
    String partitioned = writer.newPath("2016/03/15");
    assertTrue(ManagedBlobPaths.isManagedBlobPath(unpartitioned), unpartitioned);
    assertTrue(ManagedBlobPaths.isManagedBlobPath(partitioned), partitioned);
  }
}
