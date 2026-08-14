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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieNativeLogFileReader {

  @Test
  void testValidateDataBlockHeaderRequiresSchema() {
    HoodieLogFile logFile = new HoodieLogFile(new StoragePath(
        "/tmp/file-id_1-0-1_001_1.log.parquet"));
    Map<HeaderMetadataType, String> header = new HashMap<>();
    header.put(HeaderMetadataType.INSTANT_TIME, "001");

    HoodieIOException exception = assertThrows(HoodieIOException.class,
        () -> HoodieNativeLogFileReader.validateDataBlockHeader(logFile, header));

    assertTrue(exception.getMessage().contains(HeaderMetadataType.SCHEMA.name()));
    assertTrue(exception.getMessage().contains(NativeLogFooterMetadata.FOOTER_METADATA_KEY));
    assertTrue(exception.getMessage().contains(logFile.toString()));
  }
}
