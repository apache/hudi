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

package org.apache.hudi.common.util;

import org.apache.hudi.common.model.HoodieColumnRangeMetadata;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.chrono.ChronoLocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestJsonUtils {

  @Test
  void deserLocalDate() {
    HoodieColumnRangeMetadata<ChronoLocalDate> colMetadata = HoodieColumnRangeMetadata.create(
        "/dummpy/file",
        "dt",
        LocalDate.of(2000, 1, 1),
        LocalDate.of(2022, 1, 1),
        0L,
        2L,
        100L,
        100L);

    assertEquals(
        "{\"filePath\":\"/dummpy/file\",\"columnName\":\"dt\","
            + "\"minValue\":{\"year\":2000,\"month\":1,\"day\":1},"
            + "\"maxValue\":{\"year\":2022,\"month\":1,\"day\":1},"
            + "\"nullCount\":0,\"valueCount\":2,\"totalSize\":100,\"totalUncompressedSize\":100}",
        JsonUtils.toString(colMetadata));
  }
}
