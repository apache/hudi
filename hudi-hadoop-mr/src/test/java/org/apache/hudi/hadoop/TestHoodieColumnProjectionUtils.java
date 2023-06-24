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

package org.apache.hudi.hadoop;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieColumnProjectionUtils {

  @Test
  void testTypeContainsTimestamp() {
    String col1 = "timestamp";
    TypeInfo typeInfo1 = TypeInfoUtils.getTypeInfosFromTypeString(col1).get(0);
    assertTrue(HoodieColumnProjectionUtils.typeContainsTimestamp(typeInfo1));

    String col2 = "string";
    TypeInfo typeInfo2 = TypeInfoUtils.getTypeInfosFromTypeString(col2).get(0);
    assertFalse(HoodieColumnProjectionUtils.typeContainsTimestamp(typeInfo2));

    String col3 = "array<timestamp>";
    TypeInfo typeInfo3 = TypeInfoUtils.getTypeInfosFromTypeString(col3).get(0);
    assertTrue(HoodieColumnProjectionUtils.typeContainsTimestamp(typeInfo3));

    String col4 = "array<string>";
    TypeInfo typeInfo4 = TypeInfoUtils.getTypeInfosFromTypeString(col4).get(0);
    assertFalse(HoodieColumnProjectionUtils.typeContainsTimestamp(typeInfo4));

    String col5 = "map<string,timestamp>";
    TypeInfo typeInfo5 = TypeInfoUtils.getTypeInfosFromTypeString(col5).get(0);
    assertTrue(HoodieColumnProjectionUtils.typeContainsTimestamp(typeInfo5));

    String col6 = "map<string,string>";
    TypeInfo typeInfo6 = TypeInfoUtils.getTypeInfosFromTypeString(col6).get(0);
    assertFalse(HoodieColumnProjectionUtils.typeContainsTimestamp(typeInfo6));

    String col7 = "struct<name1:string,name2:timestamp>";
    TypeInfo typeInfo7 = TypeInfoUtils.getTypeInfosFromTypeString(col7).get(0);
    assertTrue(HoodieColumnProjectionUtils.typeContainsTimestamp(typeInfo7));

    String col8 = "struct<name1:string,name2:string>";
    TypeInfo typeInfo8 = TypeInfoUtils.getTypeInfosFromTypeString(col8).get(0);
    assertFalse(HoodieColumnProjectionUtils.typeContainsTimestamp(typeInfo8));

    String col9 = "uniontype<string,timestamp>";
    TypeInfo typeInfo9 = TypeInfoUtils.getTypeInfosFromTypeString(col9).get(0);
    assertTrue(HoodieColumnProjectionUtils.typeContainsTimestamp(typeInfo9));

    String col10 = "uniontype<string,int>";
    TypeInfo typeInfo10 = TypeInfoUtils.getTypeInfosFromTypeString(col10).get(0);
    assertFalse(HoodieColumnProjectionUtils.typeContainsTimestamp(typeInfo10));

    String col11 = "uniontype<string,int,map<string,array<timestamp>>>";
    TypeInfo typeInfo11 = TypeInfoUtils.getTypeInfosFromTypeString(col11).get(0);
    assertTrue(HoodieColumnProjectionUtils.typeContainsTimestamp(typeInfo11));
  }
}
