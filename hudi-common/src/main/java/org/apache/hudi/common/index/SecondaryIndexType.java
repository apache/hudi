/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.index;

import org.apache.hudi.exception.HoodieSecondaryIndexException;

import java.util.Arrays;

public enum SecondaryIndexType {
  LUCENE((byte) 1);

  private final byte type;

  SecondaryIndexType(byte type) {
    this.type = type;
  }

  public byte getValue() {
    return type;
  }

  public static SecondaryIndexType of(byte indexType) {
    return Arrays.stream(SecondaryIndexType.values())
        .filter(t -> t.type == indexType)
        .findAny()
        .orElseThrow(() ->
            new HoodieSecondaryIndexException("Unknown secondary index type:" + indexType)
        );
  }

  public static SecondaryIndexType of(String indexType) {
    return Arrays.stream(SecondaryIndexType.values())
        .filter(t -> t.name().equals(indexType.toUpperCase()))
        .findAny()
        .orElseThrow(() ->
            new HoodieSecondaryIndexException("Unknown secondary index type:" + indexType)
        );
  }
}
