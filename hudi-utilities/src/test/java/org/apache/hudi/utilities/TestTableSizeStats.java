/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hudi.utilities;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestTableSizeStats {

  @Test
  void testGetFileSizeUnit() {
    assertEquals("100.00 B", TableSizeStats.getFileSizeUnit(100));
    assertEquals("9.77 KB", TableSizeStats.getFileSizeUnit(10002));
    assertEquals("95.49 MB", TableSizeStats.getFileSizeUnit(100124384.0));
    assertEquals("9.32 GB", TableSizeStats.getFileSizeUnit(10012438464.0));
    assertEquals("9.10 TB", TableSizeStats.getFileSizeUnit(10002342345354.0));
    assertEquals("90980.87 TB", TableSizeStats.getFileSizeUnit(100034523535043354.0));
  }
}
