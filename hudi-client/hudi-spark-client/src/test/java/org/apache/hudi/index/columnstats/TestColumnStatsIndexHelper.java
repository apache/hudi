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

package org.apache.hudi.index.columnstats;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestColumnStatsIndexHelper {

  @Test
  public void testMergeSql() {
    String q = ColumnStatsIndexHelper.createIndexMergeSql("old", "new", Arrays.asList("file", "a", "b"));
    assertEquals(
        "SELECT "
            + "if (new.file is null, old.file, new.file) AS file, "
            + "if (new.a is null, old.a, new.a) AS a, "
            + "if (new.b is null, old.b, new.b) AS b "
            + "FROM old FULL JOIN new ON old.file = new.file", q);
  }

}
