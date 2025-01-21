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

package org.apache.hudi.common.serialization;

import org.apache.hudi.common.model.FileSlice;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

class TestHoodieFileSliceSerializer {

  @Test
  void testSerDe() throws IOException {
    HoodieFileSliceSerializer hoodieFileSliceSerializer = new HoodieFileSliceSerializer();
    List<FileSlice> fileSliceList = Arrays.asList(
        new FileSlice("partition1", "001", "fileId-1"),
        new FileSlice("partition2", "002", "fileId-2")
    );

    byte[] serializedBytes = hoodieFileSliceSerializer.serialize(fileSliceList);
    List<FileSlice> deserialized = hoodieFileSliceSerializer.deserialize(serializedBytes);
    Assertions.assertEquals(fileSliceList, deserialized);
  }
}