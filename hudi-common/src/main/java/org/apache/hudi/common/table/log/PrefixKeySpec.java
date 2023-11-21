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

import java.util.List;

/**
 * This class specifies a set of record keys, each of which is a prefix.
 * That is, the comparison between a record key and an element
 * of the set is {@link String#startsWith(String)}.
 */
public class PrefixKeySpec implements KeySpec {
  private final List<String> keysPrefixes;

  public PrefixKeySpec(List<String> keysPrefixes) {
    this.keysPrefixes = keysPrefixes;
  }

  @Override
  public List<String> getKeys() {
    return keysPrefixes;
  }

  @Override
  public boolean isFullKey() {
    return false;
  }
}
