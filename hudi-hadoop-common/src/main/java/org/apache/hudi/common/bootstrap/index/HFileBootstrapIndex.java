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

package org.apache.hudi.common.bootstrap.index;

import org.apache.hadoop.hbase.CellComparatorImpl;

/**
 * WARNING: DO NOT DO ANYTHING TO THIS CLASS INCLUDING CHANGING THE PACKAGE
 *          OR YOU COULD BREAK BACKWARDS COMPATIBILITY!!!
 * see https://github.com/apache/hudi/pull/5004
 */
public class HFileBootstrapIndex {
  /**
   * This class is explicitly used as Key Comparator to workaround hard coded
   * legacy format class names inside HBase. Otherwise we will face issues with shading.
   */
  public static class HoodieKVComparator extends CellComparatorImpl {}
}

