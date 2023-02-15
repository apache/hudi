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

package org.apache.hudi.keygen;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class TestKeyGenUtils {

  @Test
  public void testExtractRecordKeys() {
    // test complex key form: field1:val1,field2:val2,...
    String[] s1 = KeyGenUtils.extractRecordKeys("id:1");
    Assertions.assertArrayEquals(new String[] {"1"}, s1);

    String[] s2 = KeyGenUtils.extractRecordKeys("id:1,id:2");
    Assertions.assertArrayEquals(new String[] {"1", "2"}, s2);

    String[] s3 = KeyGenUtils.extractRecordKeys("id:1,id2:__null__,id3:__empty__");
    Assertions.assertArrayEquals(new String[] {"1", null, ""}, s3);

    String[] s4 = KeyGenUtils.extractRecordKeys("id:ab:cd,id2:ef");
    Assertions.assertArrayEquals(new String[] {"ab:cd", "ef"}, s4);

    // test simple key form: val1
    String[] s5 = KeyGenUtils.extractRecordKeys("1");
    Assertions.assertArrayEquals(new String[] {"1"}, s5);

    String[] s6 = KeyGenUtils.extractRecordKeys("id:1,id2:2,2");
    Assertions.assertArrayEquals(new String[]{"1", "2", "2"}, s6);
  }

  @Test
  public void testExtractRecordKeysWithFields() {
    List<String> fields = new ArrayList<>(1);
    fields.add("id2");

    String[] s1 = KeyGenUtils.extractRecordKeysByFields("id1:1,id2:2,id3:3", fields);
    Assertions.assertArrayEquals(new String[] {"2"}, s1);

    String[] s2 = KeyGenUtils.extractRecordKeysByFields("id1:1,id2:2,2,id3:3", fields);
    Assertions.assertArrayEquals(new String[] {"2", "2"}, s2);
  }
}
