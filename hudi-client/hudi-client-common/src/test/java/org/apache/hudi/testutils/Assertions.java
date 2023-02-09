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

package org.apache.hudi.testutils;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.testutils.CheckedFunction;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Commonly used assertion functions.
 */
public class Assertions {

  /**
   * Assert no failures in writing hoodie files.
   */
  public static void assertNoWriteErrors(List<WriteStatus> statuses) {
    assertAll(statuses.stream().map(status -> () ->
        assertFalse(status.hasErrors(), "Errors found in write of " + status.getFileId())));
  }

  /**
   * Assert each file size equal to its source of truth.
   *
   * @param fileSizeGetter to retrieve the source of truth of file size.
   */
  public static void assertFileSizesEqual(List<WriteStatus> statuses, CheckedFunction<WriteStatus, Long> fileSizeGetter) {
    assertAll(statuses.stream().map(status -> () ->
        assertEquals(fileSizeGetter.apply(status), status.getStat().getFileSizeInBytes())));
  }

}
